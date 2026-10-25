package einoforest_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

func TestNativeGraphStreamEarlyStopAndCallbackError(t *testing.T) {
	ctx := context.Background()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	owner, _ := ef.NewOwner(f, 8)
	var ended atomic.Int32
	d, err := ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "stream", Version: "1", Namespace: "s", Owner: owner}, Build: func(context.Context, *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{}, NewSession: func(ctx context.Context) (ef.GraphSession[string, string], error) {
			g := compose.NewGraph[string, string]()
			err := g.AddLambdaNode("stream", compose.StreamableLambda(func(ctx context.Context, in string) (*schema.StreamReader[string], error) {
				r, w := schema.Pipe[string](0)
				err := ef.Go(ctx, func(ctx context.Context) error {
					defer ended.Add(1)
					defer w.Close()
					for _, chunk := range []string{in, "b", "c"} {
						if w.Send(chunk, nil) {
							return nil
						}
					}
					return nil
				})
				if err != nil {
					r.Close()
					w.Close()
					return nil, err
				}
				return r, nil
			}))
			if err != nil {
				return ef.GraphSession[string, string]{}, err
			}
			_ = g.AddEdge(compose.START, "stream")
			_ = g.AddEdge("stream", compose.END)
			r, err := g.Compile(ctx)
			return ef.GraphSession[string, string]{Runnable: r, Lifetime: ef.ExecutionFrom(ctx)}, err
		}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("g", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("consumer stopped")
	for i := 0; i < 3; i++ {
		err = f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[ef.Graph[string, string]](c, f, "g", "graph", func(g ef.Graph[string, string]) error {
				return g.Stream(c, "a", ef.GraphCall{}, func(r *schema.StreamReader[string]) error {
					first, err := r.Recv()
					if err != nil || first != "a" {
						t.Fatalf("first=%s err=%v", first, err)
					}
					if i == 1 {
						return sentinel
					}
					if i == 2 {
						return nil
					}
					var rest string
					for {
						v, err := r.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							return err
						}
						rest += v
					}
					if rest != "bc" {
						t.Fatal(rest)
					}
					return nil
				})
			})
		})
		if i == 1 && !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		if i != 1 && err != nil {
			t.Fatal(err)
		}
	}
	if ended.Load() != 3 {
		t.Fatal("stream producer outlived callback")
	}
}
