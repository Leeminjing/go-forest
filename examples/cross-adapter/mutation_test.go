package crossdemo_test

import (
	"context"
	"errors"
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
	for _, p := range out.Trace {
		if p.Generation != g {
			t.Fatalf("mixed generation: %+v", out.Trace)
		}
	}
}

func TestOldActivityKeepsVersionWhileNewCallsUseReboundOrReplacedBusiness(t *testing.T) {
	for _, mode := range []string{"replace", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			ctx, o := config(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var stopped atomic.Int32
			o.BusinessStopped = func(v string) {
				if v == "v1" {
					stopped.Add(1)
				}
			}
			o.BetweenReads = func(_ context.Context, in protocol.Request) error {
				if in.Payload == "hold" {
					close(entered)
					<-release
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
			before := r.Forest.Snapshot()
			oldInput := input(o)
			oldInput.OperationID += "-old"
			oldInput.Payload = "hold"
			oldID, e := r.Start(ctx, o.TaskQueue+"-old", oldInput, false)
			if e != nil {
				t.Fatal(e)
			}
			wait(t, ctx, entered)
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
			once.Do(func() { close(release) })
			old, e := r.Result(ctx, oldID)
			if e != nil {
				t.Fatal(e)
			}
			if old.VersionBefore != "v1" || old.VersionAfter != "v1" || old.Receipt.EffectVersion != "v1" {
				t.Fatalf("old=%+v", old)
			}
			checkGeneration(t, old, 1)
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
