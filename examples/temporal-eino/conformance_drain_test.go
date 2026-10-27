package einodemo

import (
	"context"
	"errors"
	"fmt"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	tk "github.com/Leeminjing/go-forest/testkit"
	"os"
	"testing"
	"time"
)

func TestConformanceRealDrainRetainsOriginalHandleAndBorrowedResource(t *testing.T) {
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("requires real services")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	o := Options{Address: os.Getenv("FOREST_TEMPORAL_ADDRESS"), Queue: fmt.Sprintf("kit-eino-%d", time.Now().UnixNano()), Directory: t.TempDir()}
	if o.Address == "" {
		t.Fatal("explicit integration requires real Temporal")
	}
	journal, err := tk.NewJournal(64)
	if err != nil {
		t.Fatal(err)
	}
	gate := tk.NewBarrier()
	defer gate.Release()
	borrowed, err := os.CreateTemp(t.TempDir(), "borrowed")
	if err != nil {
		t.Fatal(err)
	}
	defer borrowed.Close()
	var r *Runtime
	sample := func() ([]tk.Resource, error) {
		if _, e := borrowed.Stat(); e != nil {
			return nil, e
		}
		open, closes := r.native.Report()
		err := r.native.Check()
		if open && err != nil {
			return nil, err
		}
		if !open && !errors.Is(err, os.ErrClosed) {
			return nil, fmt.Errorf("expected closed original handle: %v", err)
		}
		return []tk.Resource{{ID: "original-native-handle", Ownership: tk.Owned, Usable: open, CloseCount: uint64(closes)}, {ID: "external-file", Ownership: tk.Borrowed, Usable: true}}, nil
	}
	record := func(stage string, withResource bool) error {
		e := tk.Event{Stage: stage}
		if withResource {
			var err error
			e.Resources, err = sample()
			if err != nil {
				return err
			}
		}
		_, err := journal.Record(e)
		return err
	}
	entered := make(chan struct{})
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage != "stream.producer" {
			return nil
		}
		if err := record("entered", true); err != nil {
			return err
		}
		close(entered)
		<-c.Done()
		if err := record("notified", false); err != nil {
			return err
		}
		if err := gate.Hold(context.WithoutCancel(c)); err != nil {
			return err
		}
		return record("exited", false)
	}
	r, err = New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		gate.Release()
		cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if err := r.Close(cleanup); err != nil {
			t.Error(err)
		}
	}()
	if err = r.Enable(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := r.Start(ctx, o.Queue, protocol.Input{ExecutionID: o.Queue, OperationID: o.Queue, Text: "oak"})
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.Await(ctx, entered); err != nil {
		t.Fatal(err)
	}
	if err = r.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = tk.Await(ctx, gate.Arrived()); err != nil {
		t.Fatal(err)
	}
	short, c := context.WithTimeout(ctx, 50*time.Millisecond)
	err = r.Close(short)
	c()
	if err == nil {
		t.Fatal("drain completed while real work is held")
	}
	deadline, c := context.WithTimeout(ctx, 5*time.Second)
	defer c()
	for {
		s, e := tf.Inspect(deadline, r.Forest, "app/worker")
		if e != nil {
			t.Fatal(e)
		}
		if s.NativeStopped && s.Inflight > 0 {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal(deadline.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err = record("held", true); err != nil {
		t.Fatal(err)
	}
	if err = record("released", false); err != nil {
		t.Fatal(err)
	}
	gate.Release()
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = record("joined", true); err != nil {
		t.Fatal(err)
	}
	evidence, err := journal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.CheckDrain(tk.DrainEvidence{Label: "Eino stream/Temporal actual drain", Trace: evidence}); err != nil {
		t.Fatal(err)
	}
}
