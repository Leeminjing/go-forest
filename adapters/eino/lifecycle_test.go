package einoforest_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/compose"
)

type memoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (s *memoryStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return append([]byte(nil), v...), ok, nil
}
func (s *memoryStore) Set(_ context.Context, key string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[key] = append([]byte(nil), v...)
	return nil
}
func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("signal timeout")
	}
}

type nativeProbe struct {
	compose.Runnable[string, string]
	ended chan struct{}
}

func (p nativeProbe) Invoke(ctx context.Context, in string, opts ...compose.Option) (string, error) {
	out, err := p.Runnable.Invoke(ctx, in, opts...)
	close(p.ended)
	return out, err
}
func TestNativeCancelDoesNotReleaseRunningResource(t *testing.T) {
	ctx := context.Background()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	owner, _ := ef.NewOwner(f, 8)
	entered, release, nativeEnded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var stopped atomic.Int32
	o := ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "eino.graph", Version: "1", Namespace: "graph", Owner: owner, HotSwappable: true}, Build: func(context.Context, *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		return &ef.GraphHost[string, string]{Checkpoints: true, Lifecycle: forest.Hooks{OnStop: func(context.Context) error { stopped.Add(1); return nil }}, NewSession: func(ctx context.Context) (ef.GraphSession[string, string], error) {
			g := compose.NewGraph[string, string]()
			if err := g.AddLambdaNode("slow", compose.InvokableLambda(func(c context.Context, in string) (out string, err error) {
				err = ef.Guard(c, func() error { close(entered); <-release; out = in + "-old"; return nil })
				return
			})); err != nil {
				return ef.GraphSession[string, string]{}, err
			}
			_ = g.AddEdge(compose.START, "slow")
			_ = g.AddEdge("slow", compose.END)
			runnable, err := g.Compile(ctx, compose.WithCheckPointStore(&memoryStore{}))
			return ef.GraphSession[string, string]{Runnable: nativeProbe{runnable, nativeEnded}, Lifetime: ef.ExecutionFrom(ctx)}, err
		}}, nil
	}}
	def, err := ef.GraphDefinition(o)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(def); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("graph", def.Ref())}}); err != nil {
		t.Fatal(err)
	}
	callCtx, interrupt := compose.WithGraphInterrupt(ctx)
	done := make(chan error, 1)
	callEnded := make(chan struct{})
	go func() {
		done <- f.Invoke(callCtx, func(c context.Context) error {
			return forest.Use[ef.Graph[string, string]](c, f, "graph", "graph", func(g ef.Graph[string, string]) error {
				_, err := g.Invoke(c, "input", ef.GraphCall{CheckpointID: "one"})
				close(callEnded)
				return err
			})
		})
	}()
	waitSignal(t, entered)
	interrupt(compose.WithGraphInterruptTimeout(0))
	waitSignal(t, nativeEnded)
	// Remove is published while the protected call is still joining its node.
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Remove("graph")}}); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	err = f.Wait(short)
	cancel()
	if !errors.Is(err, forest.ErrDrainTimeout) {
		t.Fatalf("expected drain timeout: %v", err)
	}
	if stopped.Load() != 0 {
		t.Fatal("closed resource before node completed")
	}
	select {
	case <-callEnded:
		t.Fatal("adapter returned while native work was alive")
	default:
	}
	close(release)
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("native interrupt lost")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Join did not complete")
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if stopped.Load() != 1 {
		t.Fatal("resource not closed exactly once")
	}
}
