package crossdemo_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestPostCommitObserverFailureIsUnknownRatherThanARegularRetry(t *testing.T) {
	ctx, o := config(t)
	var attempts atomic.Int32
	o.BetweenReads = func(context.Context, protocol.Request) error { attempts.Add(1); return nil }
	o.AfterCommit = func(context.Context, protocol.Receipt) error { return errors.New("post-commit observation failed") }
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
	if !errors.As(e, &app) || app.Type() != "OutcomeUnknown" || attempts.Load() != 1 {
		t.Fatalf("post-commit failure retried: error=%v attempts=%d", e, attempts.Load())
	}
	obs, e := r.Inspect(ctx, input(o))
	if e != nil || !obs.Found || obs.EffectCount != 1 {
		t.Fatal(obs, e)
	}
}
