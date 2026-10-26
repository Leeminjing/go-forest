package crossdemo_test

import (
	"context"
	"errors"
	"testing"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
)

func TestActivityCannotSynchronouslyManageItsOwnPipeline(t *testing.T) {
	ctx, o := config(t)
	var r *demo.Runtime
	o.BetweenReads = func(c context.Context, _ protocol.Request) error {
		if e := r.Close(c); !errors.Is(e, tf.ErrNestedScope) {
			return errors.New("Activity can wait on its own shutdown")
		}
		if e := r.Switch(c, "replace"); !errors.Is(e, tf.ErrNestedScope) {
			return errors.New("Activity can synchronously mutate its pipeline")
		}
		return nil
	}
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	var e error
	r, e = demo.New(ctx, o)
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
	if _, e = r.Result(ctx, id); e != nil {
		t.Fatal(e)
	}
	if r.Forest.Snapshot().Generation != 1 || r.HostStatus().Closing {
		t.Fatal("rejected management changed application")
	}
}
