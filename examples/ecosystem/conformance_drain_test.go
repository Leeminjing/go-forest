package ecosystem

import (
	"context"
	"errors"
	"testing"
	"time"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	tk "github.com/Leeminjing/go-forest/testkit"
)

func TestConformanceQuiesceNotifiesAdmissionWithoutCancelingAcceptedWork(t *testing.T) {
	f := newConf(t)
	gate := tk.NewBarrier()
	defer gate.Release()
	j, _ := tk.NewJournal(64)
	var execution tk.Execution
	record := func(stage string, x tk.Execution, sample bool) error {
		e := tk.Event{Stage: stage, Execution: &x}
		if sample {
			var err error
			e.Resources, err = f.bundle.samples(f.ctx, "v1")
			if err != nil {
				return err
			}
		}
		_, err := j.Record(e)
		return err
	}
	entered := make(chan struct{})
	f.hook = func(c context.Context, stage string, call confCall, _ confReceipt) error {
		g, err := f.r.Forest.Generation(c)
		if err != nil {
			return err
		}
		x := call.Execution
		x.Scope.Generation = g
		if stage == "handler.enter" {
			execution = x
			if err = record("entered", x, true); err != nil {
				return err
			}
			close(entered)
		}
		if stage == "handler.between" {
			return gate.Hold(c)
		}
		if stage == "handler.return" {
			if c.Err() != nil {
				return errors.New("Quiesce canceled accepted handler")
			}
		}
		return nil
	}
	f.submit(t, "rpc", "drain")
	seq := f.relay(t, "drain")
	f.enable(t)
	if err := tk.Await(f.ctx, entered); err != nil {
		t.Fatal(err)
	}
	if err := tk.Await(f.ctx, gate.Arrived()); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
	err := f.quiesce(short)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("quiesce did not wait", err)
	}
	if err = f.control(f.ctx, func(w nt.Control) error {
		s := w.Status()
		if !s.Quiescing || s.Enabled || s.Inflight != 1 {
			return errors.New("quiesce/inflight evidence missing")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = record("notified", execution, false); err != nil {
		t.Fatal(err)
	}
	if err = record("held", execution, true); err != nil {
		t.Fatal(err)
	}
	before, err := f.native.Info(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.submit(t, "http", "late")
	f.relay(t, "late")
	after, err := f.native.Info(f.ctx)
	if err != nil || after.Delivered.Consumer != before.Delivered.Consumer {
		t.Fatal("quiescing consumer admitted new work", after, err)
	}
	if err = record("released", execution, false); err != nil {
		t.Fatal(err)
	}
	gate.Release()
	if err = f.quiesce(f.ctx); err != nil {
		t.Fatal(err)
	}
	finished := f.await(t, seq)
	if err = record("exited", finished.Execution, false); err != nil {
		t.Fatal(err)
	}
	f.acked(t, seq)
	stillOpen, err := f.bundle.samples(f.ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range stillOpen {
		if !p.Usable || p.CloseCount != 0 {
			t.Fatal("Quiesce closed resources", p)
		}
	}
	cleanup, c := context.WithTimeout(context.Background(), 20*time.Second)
	defer c()
	if err = f.r.Close(cleanup); err != nil {
		t.Fatal(err)
	}
	if err = record("joined", execution, true); err != nil {
		t.Fatal(err)
	}
	trace, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.CheckDrain(tk.DrainEvidence{Label: "NATS actual quiesce/handler/Ack/owner close", Execution: execution, Trace: trace}); err != nil {
		t.Fatal(err)
	}
}
