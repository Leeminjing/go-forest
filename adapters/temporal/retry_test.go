package temporalforest_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type attemptResult struct {
	Number     int32
	Generation uint64
	Value      string
}

func retryWorkflow(ctx workflow.Context, mode string) (out attemptResult, err error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second, ScheduleToCloseTimeout: 10 * time.Second, HeartbeatTimeout: 500 * time.Millisecond, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 2}})
	err = workflow.ExecuteActivity(ctx, "attempt", mode).Get(ctx, &out)
	return
}
func TestRealRetryCancellationAndOverlappingAttempts(t *testing.T) {
	c := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	queue := "retry-" + time.Now().Format("150405.000000000")
	entered, release, oldEnded, cancelEntered, cancelEnded := make(chan struct{}), make(chan struct{}), make(chan attemptResult, 1), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var stopped atomic.Int32
	provider := func(v string) forest.Definition {
		return forest.Definition{TypeID: "retry-reader", Version: v, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[reader]("reader", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{OnStop: func(context.Context) error {
				if v == "1" {
					stopped.Add(1)
				}
				return nil
			}}, forest.Provide(e, "reader", readFunc(func(context.Context) string { return v }))
		}}
	}
	p1, p2 := provider("1"), provider("2")
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "retry-worker", Version: "1", TreeID: "worker", TaskQueue: queue, Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS")}, Ports: []forest.PortDescriptor{forest.Port[reader]("reader", forest.Required())}, BuildRegistration: func(e *forest.BuildEnv) (tf.Register, error) {
		ref, err := forest.Reference[reader](e, "reader")
		if err != nil {
			return nil, err
		}
		return func(r tf.Registry) error {
			r.RegisterWorkflow(retryWorkflow)
			r.RegisterActivityWithOptions(func(c context.Context, mode string) (out attemptResult, err error) {
				out.Number = activity.GetInfo(c).Attempt
				out.Generation, err = f.Generation(c)
				if err != nil {
					return
				}
				err = ref.Use(c, func(v reader) error { out.Value = v.Read(c); return nil })
				if err != nil {
					return
				}
				switch mode {
				case "retry":
					if out.Number == 1 {
						return out, errors.New("retryable fixture failure")
					}
				case "cancel":
					close(cancelEntered)
					defer close(cancelEnded)
					ticker := time.NewTicker(20 * time.Millisecond)
					defer ticker.Stop()
					for {
						activity.RecordHeartbeat(c)
						select {
						case <-c.Done():
							return out, c.Err()
						case <-ticker.C:
						}
					}
				case "overlap":
					if out.Number == 1 {
						close(entered)
						<-release
						err = ref.Use(c, func(v reader) error { out.Value += "/" + v.Read(c); return nil })
						oldEnded <- out
					}
				}
				return
			}, activity.RegisterOptions{Name: "attempt"})
			return nil
		}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range []forest.Definition{d, p1, p2} {
		if err = f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("worker", d.Ref()), forest.AddRoot("reader", p1.Ref()), forest.Connect(forest.Link{ID: "read", Consumer: "worker", Port: "reader", Provider: "reader", Capability: "reader"})}}); err != nil {
		t.Fatal(err)
	}
	defer tf.Quiesce(ctx, f, "worker")
	if err = tf.Enable(ctx, f, "worker"); err != nil {
		t.Fatal(err)
	}
	start := func(mode string) client.WorkflowRun {
		r, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + mode, TaskQueue: queue}, retryWorkflow, mode)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	run := start("retry")
	var out attemptResult
	if err = run.Get(ctx, &out); err != nil || out.Number != 2 {
		t.Fatalf("retry=%+v %v", out, err)
	}
	run = start("cancel")
	select {
	case <-cancelEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
		t.Fatal(err)
	}
	if err = run.Get(ctx, &out); !temporal.IsCanceledError(err) {
		t.Fatalf("cancellation lost: %v", err)
	}
	select {
	case <-cancelEnded:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	run = start("overlap")
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("reader", p2.Ref(), false)}}); err != nil {
		t.Fatal(err)
	}
	if err = run.Get(ctx, &out); err != nil || out.Number != 2 || out.Generation != 2 || out.Value != "2" {
		t.Fatalf("overlap new=%+v %v", out, err)
	}
	if stopped.Load() != 0 {
		t.Fatal("timeout released old resource before actual return")
	}
	once.Do(func() { close(release) })
	select {
	case old := <-oldEnded:
		if old.Generation != 1 || old.Value != "1/1" {
			t.Fatalf("old attempt mixed topology: %+v", old)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if stopped.Load() != 1 {
		t.Fatal("old resource not drained")
	}
}
