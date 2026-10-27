package crossdemo_test

import (
	"context"
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"sync"
	"sync/atomic"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
)

func wait(t *testing.T, ctx context.Context, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func checkGeneration(t *testing.T, out protocol.Result, g uint64) {
	t.Helper()
	if len(out.Trace) == 0 {
		t.Fatal("missing native trace")
	}
	for _, p := range out.Trace {
		if p.Generation != g {
			t.Fatalf("mixed native generation: %+v", out.Trace)
		}
	}
}

func TestOldActivityKeepsVersionWhileNewCallsUseReboundOrReplacedBusiness(t *testing.T) {
	for _, mode := range []string{"replace", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			ctx, o := config(t)
			capture := &pinCapture{}
			var r *demo.Runtime
			var e error
			barrier := tk.NewBarrier()
			defer barrier.Release()
			var stopped atomic.Int32
			o.BusinessStopped = func(v string) {
				if v == "v1" {
					stopped.Add(1)
				}
			}
			o.BetweenReads = func(call context.Context, in protocol.Request) error {
				if err := capture.record(call, r, "before-resolution"); err != nil {
					return err
				}
				if in.Payload == "hold" {
					return barrier.Hold(call)
				}
				return nil
			}
			o.AfterCommit = func(call context.Context, _ protocol.Receipt) error { return capture.record(call, r, "after-commit") }
			if e := demo.Setup(ctx, o.DSN); e != nil {
				t.Fatal(e)
			}
			r, e = demo.New(ctx, o)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { barrier.Release(); r.Close(ctx) }()
			if e = r.Enable(ctx); e != nil {
				t.Fatal(e)
			}
			before := r.Forest.Snapshot()
			oldInput := input(o)
			oldInput.OperationID += "-old"
			oldInput.Payload = "hold"
			oldID, e := r.Start(ctx, o.TaskQueue+"-old", oldInput, false)
			if e != nil {
				t.Fatal(e)
			}
			if err := tk.Await(ctx, barrier.Arrived()); err != nil {
				t.Fatal(err)
			}
			if e = r.Switch(ctx, mode); e != nil {
				t.Fatal(e)
			}
			newInput := input(o)
			newInput.OperationID += "-new"
			id, e := r.Start(ctx, o.TaskQueue+"-new", newInput, false)
			if e != nil {
				t.Fatal(e)
			}
			out, e := r.Result(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if out.VersionBefore != "v2" || out.VersionAfter != "v2" || out.Receipt.EffectVersion != "v2" {
				t.Fatalf("new=%+v", out)
			}
			checkGeneration(t, out, 2)
			if err := tk.CheckPinned(capture.proof(t, r.Process, id, id, 2)); err != nil {
				t.Fatal(err)
			}
			same := newInput
			same.OperationID += "-same-version"
			sameID, err := r.Start(ctx, o.TaskQueue+"-same-version", same, false)
			if err != nil {
				t.Fatal(err)
			}
			sameOut, err := r.Result(ctx, sameID)
			if err != nil || sameOut.VersionBefore != "v2" || sameOut.VersionAfter != "v2" {
				t.Fatal(sameOut, err)
			}
			if err = tk.CheckPinned(capture.proof(t, r.Process, sameID, sameID, 2)); err != nil {
				t.Fatal(err)
			}
			if err = tk.CheckPinned(capture.proof(t, r.Process, id, sameID, 2)); !errors.Is(err, tk.ErrViolation) {
				t.Fatal("other task at same generation accepted", err)
			}

			for _, id := range []forest.TreeID{"app/fx", "app/worker", "app/client", "database"} {
				if r.Forest.Snapshot().Trees[id].InstanceID != before.Trees[id].InstanceID {
					t.Fatal("framework resource reconstructed", id)
				}
			}
			if stopped.Load() != 0 {
				t.Fatal("old business released during Activity")
			}
			if p := r.Resources(ctx); !p.FileOpen || !p.PoolResponding {
				t.Fatal(p)
			}
			barrier.Release()
			old, e := r.Result(ctx, oldID)
			if e != nil {
				t.Fatal(e)
			}
			if old.VersionBefore != "v1" || old.VersionAfter != "v1" || old.Receipt.EffectVersion != "v1" {
				t.Fatalf("old=%+v", old)
			}
			checkGeneration(t, old, 1)
			if err := tk.CheckPinned(capture.proof(t, r.Process, oldID, oldID, 1)); err != nil {
				t.Fatal(err)
			}
			if e = r.Forest.Wait(ctx); e != nil {
				t.Fatal(e)
			}
			if stopped.Load() != 1 {
				t.Fatal("old business not eventually released")
			}
		})
	}
}

func TestNativeRetryUsesNewGenerationAfterAnExplicitTransientFailure(t *testing.T) {
	ctx, o := config(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	o.BetweenReads = func(c context.Context, _ protocol.Request) error {
		if activity.GetInfo(c).Attempt == 1 {
			close(entered)
			<-release
			return errors.New("transient before transaction")
		}
		return nil
	}
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { once.Do(func() { close(release) }); r.Close(ctx) }()
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.TaskQueue, input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	wait(t, ctx, entered)
	if e = r.Switch(ctx, "replace"); e != nil {
		t.Fatal(e)
	}
	once.Do(func() { close(release) })
	out, e := r.Result(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if out.Attempt != 2 || out.VersionBefore != "v2" || out.VersionAfter != "v2" {
		t.Fatalf("retry=%+v", out)
	}
	checkGeneration(t, out, 2)
	obs, e := r.Inspect(ctx, input(o))
	if e != nil || obs.EffectCount != 1 {
		t.Fatal(obs, e)
	}
}
