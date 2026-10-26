package einodemo_test

import (
	"context"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
)

func TestObservationCannotMutateSealedPauseOrDeliveredNativeResult(t *testing.T) {
	ctx, o := config(t)
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Result != nil {
			p.Result.Text = "observer changed result"
			p.Result.Pause.Token = "changed"
			if len(p.Result.Pause.Targets) > 0 {
				p.Result.Pause.Targets[0] = "changed"
			}
			if len(p.Result.Trace) > 0 {
				p.Result.Trace[0].Process = "changed"
			}
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	s := paused(t, ctx, r, id)
	ev, e := r.Inspect(ctx, input(o))
	if e != nil {
		t.Fatal(e)
	}
	if s.Pause.Token != ev.Records[0].Result.Pause.Token || s.Pause.Targets[0] != ev.Records[0].Result.Pause.Targets[0] || s.Last.Trace[0].Process != r.Process {
		t.Fatal("observer changed sealed/delivered identity", s, ev)
	}
	out := continueRun(t, ctx, r, id, input(o))
	if out.Text != "finished:v1:oak" {
		t.Fatal("observer transformed native result", out)
	}
}
