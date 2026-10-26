package crossdemo_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestConcurrentWorkflowsShareOneDurableEffectAndRejectInputConflicts(t *testing.T) {
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
	const count = 6
	results := make(chan protocol.Result, count)
	failures := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, e := r.Start(ctx, o.TaskQueue+string(rune('a'+i)), input(o), false)
			if e == nil {
				var out protocol.Result
				out, e = r.Result(ctx, id)
				if e == nil {
					results <- out
				}
			}
			failures <- e
		}(i)
	}
	wg.Wait()
	close(results)
	close(failures)
	for e := range failures {
		if e != nil {
			t.Fatal(e)
		}
	}
	var saved protocol.Receipt
	fresh := 0
	for v := range results {
		if saved.OperationID == "" {
			saved = v.Receipt
		}
		if v.Receipt != saved {
			t.Fatalf("receipt changed: %+v %+v", saved, v)
		}
		if !v.Reused {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatal("fresh writes", fresh)
	}
	obs, e := r.Inspect(ctx, input(o))
	if e != nil || obs.EffectCount != 1 {
		t.Fatal(obs, e)
	}
	changed := input(o)
	changed.Payload = "different"
	id, e := r.Start(ctx, o.TaskQueue+"conflict", changed, false)
	if e != nil {
		t.Fatal(e)
	}
	_, e = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "InputConflict" || !app.NonRetryable() {
		t.Fatal("native conflict not terminal", e)
	}
	obs, e = r.Inspect(ctx, input(o))
	if e != nil || obs.EffectCount != 1 || obs.Receipt != saved {
		t.Fatal(obs, e)
	}
}

func TestOutcomeUnknownAcrossFxStaysNonRetryableAndReceiptCanBeReconciled(t *testing.T) {
	ctx, o := config(t)
	var effects, attempts atomic.Int32
	o.BetweenReads = func(context.Context, protocol.Request) error { attempts.Add(1); return nil }
	o.AfterCommit = func(context.Context, protocol.Receipt) error {
		effects.Add(1)
		return errors.Join(protocol.ErrUnknown, context.Canceled)
	}
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
	id, e := r.Start(ctx, o.TaskQueue, input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	_, e = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "OutcomeUnknown" || !app.NonRetryable() {
		t.Fatal("lost native unknown classification", e)
	}
	if effects.Load() != 1 || attempts.Load() != 1 {
		t.Fatalf("unknown blindly replayed: effects=%d attempts=%d", effects.Load(), attempts.Load())
	}
	obs, e := r.Inspect(ctx, input(o))
	if e != nil || !obs.Found || obs.EffectCount != 1 {
		t.Fatal(obs, e)
	}
}

func TestCancelBeforeCommitRollsBackButAfterCommitKeepsReceipt(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			ctx, o := config(t)
			entered := make(chan struct{})
			if stage == "before" {
				o.BeforeCommit = func(c context.Context, _ protocol.Request) error { close(entered); <-c.Done(); return c.Err() }
			} else {
				o.AfterCommit = func(c context.Context, _ protocol.Receipt) error { close(entered); <-c.Done(); return c.Err() }
			}
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
			id, e := r.Start(ctx, o.TaskQueue, input(o), false)
			if e != nil {
				t.Fatal(e)
			}
			wait(t, ctx, entered)
			if e = r.Cancel(ctx, id); e != nil {
				t.Fatal(e)
			}
			_, e = r.Result(ctx, id)
			if !temporal.IsCanceledError(e) {
				t.Fatal(e)
			}
			obs, e := r.Inspect(ctx, input(o))
			if e != nil {
				t.Fatal(e)
			}
			if stage == "before" {
				if obs.Found || obs.EffectCount != 0 {
					t.Fatal("cancelled transaction committed", obs)
				}
			} else {
				if !obs.Found || obs.EffectCount != 1 {
					t.Fatal("cancel claimed rollback of committed effect", obs)
				}
			}
		})
	}
}
