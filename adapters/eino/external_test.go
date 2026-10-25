package einoforest_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/compose"
)

type externalWork struct {
	once           sync.Once
	release, ended chan struct{}
}

func (w *externalWork) Cancel()     { w.once.Do(func() { close(w.release) }) }
func (w *externalWork) Join() error { <-w.ended; return nil }

func TestContextCancellationReachesExternalLifetimeBeforeNativeReturns(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 4)
	w := &externalWork{release: make(chan struct{}), ended: make(chan struct{})}
	entered := make(chan struct{})
	defer w.Cancel()
	d, err := ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "external", Version: "1", Namespace: "e", Owner: owner}, Build: func(context.Context, *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{}, NewSession: func(ctx context.Context) (ef.GraphSession[string, string], error) {
			g := compose.NewGraph[string, string]()
			_ = g.AddLambdaNode("external", compose.InvokableLambda(func(context.Context, string) (string, error) {
				close(entered)
				<-w.release
				close(w.ended)
				return "stopped", nil
			}))
			_ = g.AddEdge(compose.START, "external")
			_ = g.AddEdge("external", compose.END)
			r, err := g.Compile(ctx)
			return ef.GraphSession[string, string]{Runnable: r, Lifetime: w}, err
		}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[ef.Graph[string, string]](c, f, "agent", "graph", func(g ef.Graph[string, string]) error { _, err := g.Invoke(c, "input", ef.GraphCall{}); return err })
		})
	}()
	waitSignal(t, entered)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation became success: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("context cancellation did not reach external Cancel while native node waited")
	}
}
