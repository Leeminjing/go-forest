package einoforest_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/compose"
)

type failingStore struct{ err error }

func (s failingStore) Get(context.Context, string) ([]byte, bool, error) { return nil, false, s.err }
func (s failingStore) Set(context.Context, string, []byte) error         { return s.err }
func TestNativeCheckpointSaveFailureIsNotSuccessfulPause(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 4)
	sentinel := errors.New("checkpoint unavailable")
	// Use an actual native interrupting Agent without a fake execution engine.
	d, err := ef.ADKDefinition(ef.ADKOptions{Config: ef.Config{TypeID: "native", Version: "1", Namespace: "failed-store", Owner: owner}, Build: func(context.Context, *forest.BuildEnv) (*ef.ADKHost, error) {
		return &ef.ADKHost{Lifecycle: forest.Hooks{}, NewSession: func(ctx context.Context) (ef.ADKSession, error) {
			return ef.ADKSession{Runner: adk.NewRunner(ctx, adk.RunnerConfig{Agent: interruptAgent{}, CheckPointStore: failingStore{sentinel}}), Lifetime: ef.ExecutionFrom(ctx)}, nil
		}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Query(ctx, "pause", ef.RunOptions{CheckpointID: "fail"}, func(*adk.AgentEvent) error { return nil })
		return err
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("checkpoint write failure hidden: %v", err)
	}
}

type interruptAgent struct{}

func (interruptAgent) Name(context.Context) string        { return "interrupt" }
func (interruptAgent) Description(context.Context) string { return "native interrupt fixture" }
func (interruptAgent) Run(ctx context.Context, _ *adk.AgentInput, _ ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
	it, g := adk.NewAsyncIteratorPair[*adk.AgentEvent]()
	g.Send(adk.Interrupt(ctx, "pause"))
	g.Close()
	return it
}
func TestInvalidNativeGraphRollsBackAndKeepsOldEngine(t *testing.T) {
	ctx := context.Background()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	owner, _ := ef.NewOwner(f, 4)
	old, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "native", Owner: owner}, t.TempDir(), nativeTool{label: "oak"})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, old)
	var cleaned atomic.Int32
	bad, err := ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "bad", Version: "1", Namespace: "bad", Owner: owner}, Build: func(c context.Context, _ *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		g := compose.NewGraph[string, string]()
		_, err := g.Compile(c)
		return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{OnStop: func(context.Context) error { cleaned.Add(1); return nil }}}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(bad); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("bad", bad.Ref())}}); !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatal(err)
	}
	if f.Snapshot().Generation != 1 {
		t.Fatal("failed graph changed version")
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if cleaned.Load() != 1 {
		t.Fatal("temporary resource not cleaned")
	}
	if err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Query(ctx, "still works", ef.RunOptions{CheckpointID: "old"}, func(e *adk.AgentEvent) error { _, err := ef.ReadMessage(e); return err })
		return err
	}); err != nil {
		t.Fatal(err)
	}
	unmanaged, err := ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "unmanaged", Version: "1", Namespace: "u", Owner: owner}, Build: func(context.Context, *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(unmanaged); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("unmanaged", unmanaged.Ref())}}); !errors.Is(err, ef.ErrUnmanaged) {
		t.Fatal(err)
	}
}
