package crossdemo_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/flow"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func TestQueuedWorkflowWaitsForCommittedFxAndWorkerAdmission(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	id, e := r.Start(ctx, o.TaskQueue, input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	w, e := tf.Inspect(ctx, r.Forest, "app/worker")
	if e != nil || w.Phase != "disabled" || w.Started != 0 {
		t.Fatal(w, e)
	}
	x, e := ff.Inspect(ctx, r.Forest, "app/fx")
	if e != nil || x.Accepting {
		t.Fatal(x, e)
	}
	if e = ff.Enable(ctx, r.Forest, "app/fx"); e != nil {
		t.Fatal(e)
	}
	w, e = tf.Inspect(ctx, r.Forest, "app/worker")
	if e != nil || w.Started != 0 || w.Phase != "disabled" {
		t.Fatal("Fx Enable started worker", w, e)
	}
	if e = tf.Enable(ctx, r.Forest, "app/worker"); e != nil {
		t.Fatal(e)
	}
	out, e := r.Result(ctx, id)
	if e != nil || out.Receipt.OperationID != input(o).OperationID {
		t.Fatal(out, e)
	}
	if r.Forest.Snapshot().Generation != 1 {
		t.Fatal("Enable changed generation")
	}
}

type ping interface{ Ping() string }
type pingValue struct{}

func (pingValue) Ping() string { return "original alive" }
func TestFailedCandidateLeavesOldForestAliveAndCleansThreeAdapters(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	d := forest.Definition{TypeID: "original", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[ping]("ping", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(env, "ping", pingValue{})
	}}
	if e := f.Register(d); e != nil {
		t.Fatal(e)
	}
	if _, e := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("original", d.Ref())}}); e != nil {
		t.Fatal(e)
	}
	// Independent control client owns its own native connection and test workflow.
	c, e := client.DialContext(ctx, client.Options{HostPort: o.Address})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	run, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: o.TaskQueue, TaskQueue: o.TaskQueue}, flow.Command, flow.Input{Request: input(o)})
	if e != nil {
		t.Fatal(e)
	}
	defer c.TerminateWorkflow(context.Background(), run.GetID(), run.GetRunID(), "owned test cleanup")
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var candidate *demo.Runtime
	var calls atomic.Int32
	o.Forest = f
	o.BetweenReads = func(context.Context, protocol.Request) error { calls.Add(1); return nil }
	boom := errors.New("later readiness failed")
	o.Prepared = func(_ context.Context, r *demo.Runtime) error { candidate = r; close(entered); <-release; return boom }
	result := make(chan error, 1)
	go func() { _, e := demo.New(ctx, o); result <- e }()
	wait(t, ctx, entered)
	if p := candidate.Resources(ctx); !p.FileOpen || !p.PoolResponding {
		t.Fatal("candidate not prepared", p)
	}
	if e = f.Invoke(ctx, func(call context.Context) error {
		return forest.Use[ping](call, f, "original", "ping", func(p ping) error {
			if p.Ping() != "original alive" {
				return errors.New("old capability changed")
			}
			return nil
		})
	}); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 0 {
		t.Fatal("candidate consumed queued workflow")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case e = <-result:
		if !errors.Is(e, boom) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e = f.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	if f.Snapshot().Generation != 1 || len(f.Snapshot().Trees) != 1 {
		t.Fatal("failed candidate changed old topology")
	}
	if p := candidate.Resources(ctx); p.FileOpen || p.FileCloses != 1 || p.PoolResponding {
		t.Fatal("candidate resource leaked", p)
	}
	if calls.Load() != 0 {
		t.Fatal("failed candidate admitted business")
	}
}

func TestWorkerEnableFailureDoesNotPretendToRollbackCommittedGraph(t *testing.T) {
	ctx, o := config(t)
	o.Namespace = o.TaskQueue + "-absent"
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e == nil {
		t.Fatal("nonexistent namespace worker enabled")
	}
	if r.Forest.Snapshot().Generation != 1 || len(r.Forest.Snapshot().Trees) != 6 {
		t.Fatal("Enable fabricated graph rollback")
	}
	x, e := ff.Inspect(ctx, r.Forest, "app/fx")
	if e != nil || !x.Accepting {
		t.Fatal("Fx was not first enabled", x, e)
	}
	if e = r.Close(ctx); e != nil {
		t.Fatal(e)
	}
}

func eventually(t *testing.T, ctx context.Context, fn func() bool) {
	t.Helper()
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestWrongEnableOrderReturnsDependencyUnavailableRatherThanHalfReadySuccess(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if _, e = r.Start(ctx, "", input(o), false); !errors.Is(e, protocol.ErrConflict) {
		t.Fatal("empty workflow identity silently randomized", e)
	}
	id, e := r.Start(ctx, o.TaskQueue, input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	if e = tf.Enable(ctx, r.Forest, "app/worker"); e != nil {
		t.Fatal(e)
	}
	_, e = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "DependencyUnavailable" {
		t.Fatal("downstream admission failure lost", e)
	}
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	obs, e := r.Inspect(ctx, input(o))
	if e != nil || obs.Found || obs.EffectCount != 0 {
		t.Fatal("half-ready app produced effect", obs, e)
	}
	id, e = r.Start(ctx, o.TaskQueue+"-correct-order", input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Result(ctx, id); e != nil {
		t.Fatal(e)
	}
}
