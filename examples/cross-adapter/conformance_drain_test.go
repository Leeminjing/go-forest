package crossdemo

import (
	"context"
	"errors"
	"fmt"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	tk "github.com/Leeminjing/go-forest/testkit"
	"go.temporal.io/sdk/activity"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestConformanceRealDrainRetainsOriginalHandleAndBorrowedResource(t *testing.T) {
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("requires real services")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	o := Options{Address: os.Getenv("FOREST_TEMPORAL_ADDRESS"), DSN: os.Getenv("FOREST_POSTGRES_DSN"), TaskQueue: fmt.Sprintf("kit-cross-%d", time.Now().UnixNano()), Directory: t.TempDir()}
	if o.Address == "" || o.DSN == "" {
		t.Fatal("explicit integration requires real services")
	}
	if err := Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
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
		closes := r.native.Closes()
		err := r.native.Check()
		open := err == nil
		if err != nil && closes != 1 {
			return nil, err
		}
		if closes == 1 && open {
			return nil, errors.New("closed original native file is still usable")
		}
		return []tk.Resource{{ID: "original-native-handle", Ownership: tk.Owned, Usable: open, CloseCount: uint64(closes)}, {ID: "external-file", Ownership: tk.Borrowed, Usable: true}}, nil
	}
	var work tk.Execution
	identity := func(c context.Context) (tk.Execution, error) {
		info := activity.GetInfo(c)
		g, err := r.Forest.Generation(c)
		if err != nil {
			return tk.Execution{}, err
		}
		task := info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID
		attempt := strconv.Itoa(int(info.Attempt))
		return tk.Execution{Scope: tk.Scope{Forest: r.Process, Call: task + "/" + info.ActivityID + "/" + attempt, Generation: g}, Task: task, Step: info.ActivityID, Attempt: attempt, Process: r.Process, Instance: r.Forest.Snapshot().Trees["app/worker"].InstanceID}, nil
	}
	record := func(stage string, withResource bool, current tk.Execution) error {
		e := tk.Event{Stage: stage, Execution: &current}
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
	o.Child = func(c context.Context) error {
		current, err := identity(c)
		if err != nil {
			return err
		}
		work = current
		if err := record("entered", true, current); err != nil {
			return err
		}
		close(entered)
		<-c.Done()
		if err := record("notified", false, current); err != nil {
			return err
		}
		if err := gate.Hold(context.WithoutCancel(c)); err != nil {
			return err
		}
		exit, err := identity(c)
		if err != nil {
			return err
		}
		return record("exited", false, exit)
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
	if _, err = r.Start(ctx, o.TaskQueue, protocol.Request{OperationID: o.TaskQueue, Target: o.TaskQueue, Payload: "oak"}, false); err != nil {
		t.Fatal(err)
	}
	if err = tk.Await(ctx, entered); err != nil {
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
	if err = record("held", true, work); err != nil {
		t.Fatal(err)
	}
	if err = record("released", false, work); err != nil {
		t.Fatal(err)
	}
	gate.Release()
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = record("joined", true, work); err != nil {
		t.Fatal(err)
	}
	evidence, err := journal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.CheckDrain(tk.DrainEvidence{Label: "Fx child/Temporal actual drain", Execution: work, Trace: evidence}); err != nil {
		t.Fatal(err)
	}
}
