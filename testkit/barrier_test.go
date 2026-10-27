package testkit_test

import (
	"context"
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"testing"
	"time"
)

func TestBarrierCoordinatesActualWorkAndRejectsReentry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b := tk.NewBarrier()
	defer b.Release()
	done := make(chan error, 1)
	go func() { done <- b.Hold(ctx) }()
	if err := tk.Await(ctx, b.Arrived()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("work left before release")
	default:
	}
	if err := b.Hold(ctx); !errors.Is(err, tk.ErrBarrierUsed) {
		t.Fatal(err)
	}
	b.Release()
	b.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBarrierDoesNotInventArrivalAfterCancellationOrEarlyRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := tk.NewBarrier()
	if err := b.Hold(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-b.Arrived():
		t.Fatal("cancelled call arrived")
	default:
	}
	b.Release()
	if err := b.Hold(context.Background()); !errors.Is(err, tk.ErrBarrierReleased) {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	if err := tk.Await(ctx, ready); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
