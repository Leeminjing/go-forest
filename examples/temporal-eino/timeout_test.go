package einodemo_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestTimedOutAttemptRetainsOwnershipUntilActualNativeExit(t *testing.T) {
	ctx, o := config(t)
	in := input(o)
	in.AttemptMillis = 1000
	entered, deadline, busy, release := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enterOnce, busyOnce, free sync.Once
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "tool.between" {
			enterOnce.Do(func() { close(entered) })
			<-c.Done()
			close(deadline)
			<-release
		}
		if p.Stage == "step.busy" {
			if p.Generation != 2 {
				return protocol.ErrConflict
			}
			busyOnce.Do(func() { close(busy) })
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	defer free.Do(func() { close(release) })
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, in)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e = r.Switch(ctx, "replace"); e != nil {
		t.Fatal(e)
	}
	select {
	case <-deadline:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-busy:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if r.Counts().Query != 1 || r.Resources().BusinessCloses != 0 || !r.Resources().NativeOpen {
		t.Fatal("timed out execution released/reentered", r.Counts(), r.Resources())
	}
	free.Do(func() { close(release) })
	_, e = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "OutcomeUnknown" {
		t.Fatal("timeout authorized replay", e)
	}
	if r.Counts().Query != 1 || r.Counts().Resume != 0 {
		t.Fatal(r.Counts())
	}
	ev, e := r.Inspect(ctx, in)
	if e != nil || ev.Artifacts != 0 || len(ev.Records) != 1 || ev.Records[0].State != "unknown" {
		t.Fatal(ev, e)
	}
}
