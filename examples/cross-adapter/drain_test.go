package crossdemo_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/client"
)

func TestCoordinatedDrainKeepsActualActivityAndFxChildrenAliveUntilJoin(t *testing.T) {
	for _, stage := range []string{"activity", "child"} {
		t.Run(stage, func(t *testing.T) {
			ctx, o := config(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			if stage == "activity" {
				o.AfterCommit = func(context.Context, protocol.Receipt) error { close(entered); <-release; return nil }
			} else {
				o.Child = func(c context.Context) error { <-c.Done(); close(entered); <-release; return nil }
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
			short, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
			e = r.Close(short)
			cancel()
			if !errors.Is(e, tf.ErrDrainTimeout) {
				t.Fatal("drain returned before actual completion", e)
			}
			w, e := tf.Inspect(ctx, r.Forest, "app/worker")
			eventually(t, ctx, func() bool {
				var err error
				w, err = tf.Inspect(ctx, r.Forest, "app/worker")
				if err != nil {
					t.Fatal(err)
				}
				return w.NativeStopped
			})
			if e != nil || !w.NativeStopped || w.Inflight != 1 {
				t.Fatalf("worker=%+v %v", w, e)
			}
			x, e := ff.Inspect(ctx, r.Forest, "app/fx")
			if e != nil || x.Inflight != 1 {
				t.Fatalf("Fx=%+v %v", x, e)
			}
			if p := r.Resources(ctx); !p.FileOpen || !p.PoolResponding || p.FileCloses != 0 {
				t.Fatal("premature resource release", p)
			}
			if _, e = r.Start(ctx, o.TaskQueue+"late", input(o), false); !errors.Is(e, demo.ErrUnavailable) {
				t.Fatal("shutdown accepted new host request", e)
			}
			short, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
			e = ff.Quiesce(short, r.Forest, "app/fx")
			cancel()
			if !errors.Is(e, ff.ErrDrainTimeout) {
				t.Fatal(e)
			}
			once.Do(func() { close(release) })
			if e = r.Close(ctx); e != nil {
				t.Fatal(e)
			}
			if p := r.Resources(ctx); p.FileOpen || p.PoolResponding || p.FileCloses != 1 {
				t.Fatal("resources not finally released", p)
			}
			for _, i := range r.Forest.Snapshot().Instances {
				if i.State != forest.Stopped {
					t.Fatal("instance not stopped", i)
				}
			}
			// A durable workflow may still be queued after the local worker exits.
			c, e := client.DialContext(ctx, client.Options{HostPort: o.Address})
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			_ = c.TerminateWorkflow(ctx, id.WorkflowID, id.RunID, "owned drain test cleanup")
		})
	}
}

func TestCloseCancelsLocalResultWaitAndDoesNotConfuseItWithAdapterLeak(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.TaskQueue, input(o), true)
	if e != nil {
		t.Fatal(e)
	}
	eventually(t, ctx, func() bool {
		state, e := r.Query(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		return state.Stage == "waiting"
	})
	done := make(chan error, 1)
	go func() { _, e := r.Result(ctx, id); done <- e }()
	eventually(t, ctx, func() bool { return r.HostStatus().Active == 1 })
	if e = r.Close(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-done:
		if e == nil {
			t.Fatal("waiting workflow falsely completed")
		}
	case <-ctx.Done():
		t.Fatal("host Get retained graph after close", ctx.Err())
	}
	if e = r.Enable(ctx); !errors.Is(e, demo.ErrUnavailable) {
		t.Fatal(e)
	}
	c, e := client.DialContext(ctx, client.Options{HostPort: o.Address})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	_ = c.TerminateWorkflow(ctx, id.WorkflowID, id.RunID, "owned waiting test cleanup")
}
