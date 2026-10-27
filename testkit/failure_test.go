package testkit_test

import (
	"context"
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"testing"
)

func TestInvalidEvidenceCannotRecoverIntoAnApparentlyCompleteTrace(t *testing.T) {
	for _, n := range []int{0, -1, 65537} {
		if _, err := tk.NewJournal(n); !errors.Is(err, tk.ErrInvalidEvidence) {
			t.Fatal(n, err)
		}
	}
	j, _ := tk.NewJournal(2)
	if _, err := j.Record(tk.Event{}); !errors.Is(err, tk.ErrInvalidEvidence) {
		t.Fatal(err)
	}
	if _, err := j.Record(tk.Event{Stage: "later"}); !errors.Is(err, tk.ErrInvalidEvidence) {
		t.Fatal("invalid evidence was forgotten", err)
	}
	broken, err := j.Snapshot()
	if !errors.Is(err, tk.ErrInvalidEvidence) {
		t.Fatal(err)
	}
	var failure *tk.Failure
	err = tk.CheckPinned(tk.PinnedEvidence{Label: "invalid trace", Trace: broken})
	if !errors.As(err, &failure) || failure.Rule != "pinned" || failure.Label != "invalid trace" || !errors.Is(err, tk.ErrInvalidEvidence) {
		t.Fatal(failure, err)
	}
}
func TestCancellationUnblocksActualBarrierWaitWithoutInventingRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := tk.NewBarrier()
	defer b.Release()
	done := make(chan error, 1)
	go func() { done <- b.Hold(ctx) }()
	if err := tk.Await(context.Background(), b.Arrived()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := b.Hold(context.Background()); !errors.Is(err, tk.ErrBarrierUsed) {
		t.Fatal(err)
	}
}
