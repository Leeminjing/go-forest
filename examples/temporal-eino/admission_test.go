package einodemo_test

import (
	"context"
	"errors"
	"testing"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
)

type ping interface{ Ping() string }
type original struct{}

func (original) Ping() string { return "alive" }
func TestCandidateFailurePreservesBorrowedForestAndCleansOwnedResources(t *testing.T) {
	ctx, o := config(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	d := forest.Definition{TypeID: "original", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[ping]("ping", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "ping", original{})
	}}
	if e := f.Register(d); e != nil {
		t.Fatal(e)
	}
	if _, e := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("original", d.Ref())}}); e != nil {
		t.Fatal(e)
	}
	boom := errors.New("later readiness rejected")
	var candidate *demo.Runtime
	o.Forest = f
	o.Prepared = func(c context.Context, r *demo.Runtime) error {
		candidate = r
		if p := r.Resources(); !p.Claim || !p.NativeOpen || !p.BusinessOpen {
			t.Fatal("candidate not prepared", p)
		}
		return boom
	}
	if _, e := demo.New(ctx, o); !errors.Is(e, boom) {
		t.Fatal(e)
	}
	if e := f.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	if f.Snapshot().Generation != 1 || len(f.Snapshot().Trees) != 1 {
		t.Fatal("failed candidate published")
	}
	if p := candidate.Resources(); p.Claim || p.NativeOpen || p.BusinessOpen {
		t.Fatal("candidate leaked", p)
	}
	if candidate.Counts().Model != 0 || candidate.Counts().Apply != 0 {
		t.Fatal("preparation did business")
	}
	if e := f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[ping](c, f, "original", "ping", func(p ping) error {
			if p.Ping() != "alive" {
				return boom
			}
			return nil
		})
	}); e != nil {
		t.Fatal(e)
	}
}
func TestQueuedTasksWaitForPostCommitEnable(t *testing.T) {
	ctx, o := config(t)
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	s, e := tf.Inspect(ctx, r.Forest, "app/worker")
	if e != nil || s.Phase != "disabled" || s.Started != 0 || r.Counts().Query != 0 {
		t.Fatal(s, e)
	}
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	if r.Forest.Snapshot().Generation != 1 {
		t.Fatal("Enable committed topology")
	}
	continueRun(t, ctx, r, id, input(o))
}
func TestStateDirectoryCannotBeManagedTwiceAndBorrowedForestRemainsUsable(t *testing.T) {
	ctx, o := config(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	o.Forest = f
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	second := o
	second.Forest = nil
	if other, e := demo.New(ctx, second); !errors.Is(e, protocol.ErrOwner) {
		if other != nil {
			_ = other.Close(ctx)
		}
		t.Fatal("second manager admitted", e)
	}
	if p := r.Resources(); !p.Claim || !p.NativeOpen || !p.BusinessOpen {
		t.Fatal("failed second manager released first", p)
	}
	if e = r.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = f.Invoke(ctx, func(context.Context) error { return nil }); e != nil {
		t.Fatal("borrowed forest closed", e)
	}
}
