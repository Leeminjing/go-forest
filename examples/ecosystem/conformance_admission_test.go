package ecosystem

import (
	"context"
	"errors"
	"testing"
	"time"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	tk "github.com/Leeminjing/go-forest/testkit"
)

func TestConformanceCandidateNeverFetchesAndFailedResourcesReallyClose(t *testing.T) {
	ctx := confContext(t)
	o := confOptions(t)
	if err := confSetup(ctx, o); err != nil {
		t.Fatal(err)
	}
	f, err := confOpen(ctx, o)
	if f != nil {
		defer f.close(t)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.submit(t, "http", "candidate")
	seq := f.relay(t, "candidate")
	before := f.topology()
	brokerBefore, err := f.native.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gate := tk.NewBarrier()
	defer gate.Release()
	j, _ := tk.NewJournal(64)
	record := func(stage string, sample bool) error {
		point := tk.Event{Stage: stage}
		if sample {
			var err error
			point.Resources, err = f.bundle.samples(ctx, "v1")
			if err != nil {
				return err
			}
		}
		_, err := j.Record(point)
		return err
	}
	boom := errors.New("fixture readiness rejected")
	f.bundle.ready = func(c context.Context, _ *confBundle) error {
		if err := record("candidate.prepared", true); err != nil {
			return err
		}
		if err := gate.Hold(c); err != nil {
			return err
		}
		return boom
	}
	if err = record("candidate.begin", false); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var buildErr error
	go func() { _, buildErr = f.r.Forest.Apply(ctx, f.bundle.plan()); close(done) }()
	defer func() {
		gate.Release()
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := tk.Await(cleanup, done); err != nil {
			t.Error(err)
		}
	}()
	if err = tk.Await(ctx, gate.Arrived()); err != nil {
		t.Fatal(err)
	}
	during, err := f.native.Info(ctx)
	if err != nil || during.Delivered != brokerBefore.Delivered || during.AckFloor != brokerBefore.AckFloor || during.NumPending != brokerBefore.NumPending {
		t.Fatal("candidate fetched queued work", during, err)
	}
	task := f.readHTTP(t, "candidate")
	if task.State != "pending" {
		t.Fatal(task)
	}
	if err = record("original.served", false); err != nil {
		t.Fatal(err)
	}
	gate.Release()
	if err = tk.Await(ctx, done); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(buildErr, boom) {
		t.Fatal(buildErr)
	}
	if err = f.r.Forest.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err = record("candidate.end", true); err != nil {
		t.Fatal(err)
	}
	after := f.topology()
	brokerAfter, err := f.native.Info(ctx)
	if err != nil || brokerAfter.Delivered != brokerBefore.Delivered || brokerAfter.AckFloor != brokerBefore.AckFloor || brokerAfter.NumPending != brokerBefore.NumPending {
		t.Fatal("failed candidate changed delivery", brokerAfter, err)
	}
	// Registered definitions are not rolled back. A fresh native Forest provides
	// the positive control on the very same queued broker message.
	positive, err := confOpen(ctx, o)
	if positive != nil {
		defer positive.close(t)
	}
	if err != nil {
		t.Fatal(err)
	}
	positive.hook = func(_ context.Context, stage string, _ confCall, _ confReceipt) error {
		if stage == "handler.enter" {
			_, err := j.Record(tk.Event{Stage: "business.enter"})
			return err
		}
		return nil
	}
	if _, err = positive.r.Forest.Apply(ctx, positive.bundle.plan()); err != nil {
		t.Fatal(err)
	}
	if err = positive.bundle.anchor("v1", "app/proof/handler"); err != nil {
		t.Fatal(err)
	}
	if err = positive.control(ctx, func(c nt.Control) error {
		s := c.Status()
		if s.Enabled || s.Inflight != 0 || s.Handled != 0 {
			return errors.New("consumer enabled during commit")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = record("commit", false); err != nil {
		t.Fatal(err)
	}
	if err = record("enable", false); err != nil {
		t.Fatal(err)
	}
	positive.enable(t)
	positive.await(t, seq)
	positive.acked(t, seq)
	evidence, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.CheckAdmission(tk.AdmissionEvidence{Label: "native JetStream complete candidate interval", Before: before, After: after, Trace: evidence}); err != nil {
		t.Fatal(err)
	}
}
