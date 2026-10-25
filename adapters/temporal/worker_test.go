package temporalforest_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func connect(t *testing.T) client.Client {
	t.Helper()
	address := os.Getenv("FOREST_TEMPORAL_ADDRESS")
	if address == "" {
		if os.Getenv("FOREST_INTEGRATION") == "1" {
			t.Fatal("FOREST_TEMPORAL_ADDRESS required")
		}
		t.Skip("real Temporal integration: set FOREST_INTEGRATION=1 and FOREST_TEMPORAL_ADDRESS")
	}
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func onceWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteActivity(ctx, name).Get(ctx, &out)
	return out, err
}

func TestCandidateNeverPollsAndNativeStopIsNotPhysicalJoin(t *testing.T) {
	c := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	queue := "forest-" + time.Now().Format("150405.000000000")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, onceWorkflow, "hold")
	if err != nil {
		t.Fatal(err)
	}
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var calls, stopped atomic.Int32
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "native-worker", Version: "1", TreeID: "worker", TaskQueue: queue, Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS")}, BuildRegistration: func(*forest.BuildEnv) (tf.Register, error) {
		return func(r tf.Registry) error {
			r.RegisterWorkflow(onceWorkflow)
			r.RegisterActivityWithOptions(func(context.Context) (string, error) { calls.Add(1); close(entered); <-release; return "done", nil }, activity.RegisterOptions{Name: "hold"})
			return nil
		}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	// A later candidate's readiness barrier proves preparation completed without polling.
	ready, finish := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	}()
	bad := forest.Definition{TypeID: "barrier", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnReady: func(context.Context) error { close(ready); <-finish; return errors.New("rollback") }, OnStop: func(context.Context) error { stopped.Add(1); return nil }}, nil
	}}
	if err = f.Register(bad); err != nil {
		t.Fatal(err)
	}
	failed := make(chan error, 1)
	go func() {
		_, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("worker", d.Ref()), forest.Nest("worker", "barrier", bad.Ref())}})
		failed <- err
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if calls.Load() != 0 {
		t.Fatal("candidate consumed a task")
	}
	close(finish)
	if err = <-failed; !errors.Is(err, forest.ErrPrepareFailed) || f.Snapshot().Generation != 0 {
		t.Fatalf("rollback: %v", err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("worker", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("Apply implicitly enabled worker")
	}
	if err = tf.Enable(ctx, f, "worker"); err != nil {
		t.Fatal(err)
	}
	if err = tf.Enable(ctx, f, "worker"); err != nil {
		t.Fatalf("repeated Enable failed: %v", err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	short, end := context.WithTimeout(ctx, 100*time.Millisecond)
	err = tf.Quiesce(short, f, "worker")
	end()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("false completion: %v", err)
	}
	for {
		status, err := tf.Inspect(ctx, f, "worker")
		if err != nil || status.Inflight != 1 {
			t.Fatalf("status=%+v %v", status, err)
		}
		if status.NativeStopped {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Remove("worker")}}); err != nil {
		t.Fatal(err)
	}
	short, end = context.WithTimeout(ctx, 30*time.Millisecond)
	err = f.Wait(short)
	end()
	if !errors.Is(err, forest.ErrDrainTimeout) {
		t.Fatalf("scope released early: %v", err)
	}
	close(release)
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if stopped.Load() != 1 {
		t.Fatalf("rollback resource cleanup=%d", stopped.Load())
	}
	if calls.Load() != 1 {
		t.Fatalf("repeated Enable duplicated activity: %d", calls.Load())
	}
	// A shutdown attempt can be retried by the real service; its original result is not invented here.
	_ = run
}
