package einoforest_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type streamValue interface{ Value(context.Context) string }
type valueProvider string

func (v valueProvider) Value(context.Context) string { return string(v) }

func TestNativeOutputStreamPinsProviderAndEngineAcrossReplacement(t *testing.T) {
	ctx := context.Background()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	owner, _ := ef.NewOwner(f, 8)
	first, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var stoppedProvider, stoppedGraph atomic.Int32
	provider := func(version string) forest.Definition {
		return forest.Definition{TypeID: "values", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[streamValue]("value", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{OnStop: func(context.Context) error {
				if version == "1" {
					stoppedProvider.Add(1)
				}
				return nil
			}}, forest.Provide(env, "value", valueProvider("p"+version))
		}}
	}
	graph := func(version string) forest.Definition {
		d, err := ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "stream", Version: version, Namespace: "s", Owner: owner, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[streamValue]("value", forest.Required())}}, Build: func(_ context.Context, env *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
			ref, err := forest.Reference[streamValue](env, "value")
			if err != nil {
				return nil, err
			}
			return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{OnStop: func(context.Context) error {
				if version == "1" {
					stoppedGraph.Add(1)
				}
				return nil
			}}, NewSession: func(ctx context.Context) (ef.GraphSession[string, string], error) {
				g := compose.NewGraph[string, string]()
				_ = g.AddLambdaNode("source", compose.StreamableLambda(func(ctx context.Context, _ string) (*schema.StreamReader[string], error) {
					r, w := schema.Pipe[string](0)
					err := ef.Go(ctx, func(ctx context.Context) error {
						defer w.Close()
						for n := 0; n < 2; n++ {
							if version == "1" && n == 1 {
								<-release
							}
							var out string
							if err := ref.Use(ctx, func(v streamValue) error { out = "g" + version + ":" + v.Value(ctx); return nil }); err != nil {
								return err
							}
							if w.Send(out, nil) {
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
				_ = g.AddEdge(compose.START, "source")
				_ = g.AddEdge("source", compose.END)
				r, err := g.Compile(ctx)
				return ef.GraphSession[string, string]{Runnable: r, Lifetime: ef.ExecutionFrom(ctx)}, err
			}}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	p1, p2, g1, g2 := provider("1"), provider("2"), graph("1"), graph("2")
	for _, d := range []forest.Definition{p1, p2, g1, g2} {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("p", p1.Ref()), forest.AddRoot("g", g1.Ref()), forest.Connect(forest.Link{ID: "g.p", Consumer: "g", Port: "value", Provider: "p", Capability: "value"})}}); err != nil {
		t.Fatal(err)
	}
	read := func(signal bool) (out string, err error) {
		err = f.Invoke(ctx, func(ctx context.Context) error {
			return forest.Use[ef.Graph[string, string]](ctx, f, "g", "graph", func(g ef.Graph[string, string]) error {
				return g.Stream(ctx, "", ef.GraphCall{}, func(r *schema.StreamReader[string]) error {
					var chunks []string
					for {
						v, err := r.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							return err
						}
						chunks = append(chunks, v)
						if signal && len(chunks) == 1 {
							close(first)
						}
					}
					out = strings.Join(chunks, "/")
					return nil
				})
			})
		})
		return
	}
	done := make(chan struct {
		out string
		err error
	}, 1)
	go func() {
		out, err := read(true)
		done <- struct {
			out string
			err error
		}{out, err}
	}()
	waitSignal(t, first)
	if _, err := f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("p", p2.Ref(), false), forest.Replace("g", g2.Ref(), false)}}); err != nil {
		t.Fatal(err)
	}
	if out, err := read(false); err != nil || out != "g2:p2/g2:p2" {
		t.Fatalf("new=%s %v", out, err)
	}
	if stoppedGraph.Load() != 0 || stoppedProvider.Load() != 0 {
		t.Fatal("resource closed while stream was paused")
	}
	releaseOnce.Do(func() { close(release) })
	old := <-done
	if old.err != nil || old.out != "g1:p1/g1:p1" {
		t.Fatalf("old=%s %v", old.out, old.err)
	}
	if err := f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if stoppedGraph.Load() != 1 || stoppedProvider.Load() != 1 {
		t.Fatal("retired stream resources not closed exactly once")
	}
}
