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
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

type pureInterceptor struct {
	interceptor.ClientInterceptorBase
}
type hybridInterceptor struct {
	interceptor.InterceptorBase
	calls *atomic.Int32
}

func (h *hybridInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	h.calls.Add(1)
	return h.InterceptorBase.InterceptActivity(ctx, next)
}
func TestRegistryCannotExposeActuatorOrMutableClientHooks(t *testing.T) {
	c := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	queue := "boundary-" + time.Now().Format("150405.000000000")
	var calls atomic.Int32
	hooks := []interceptor.ClientInterceptor{&pureInterceptor{}}
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "boundary", Version: "1", TreeID: "w", TaskQueue: queue, Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS"), Interceptors: hooks}, BuildRegistration: func(*forest.BuildEnv) (tf.Register, error) {
		return func(r tf.Registry) error {
			if _, ok := r.(worker.Worker); ok {
				return errors.New("registration leaked Start/Stop actuator")
			}
			if _, ok := r.(worker.NexusServiceRegistry); ok {
				return errors.New("registration leaked unsupported Nexus")
			}
			r.RegisterWorkflow(onceWorkflow)
			r.RegisterActivityWithOptions(func(context.Context) (string, error) { return "safe", nil }, activity.RegisterOptions{Name: "safe"})
			return nil
		}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	hooks[0] = &hybridInterceptor{calls: &calls}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("w", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	defer tf.Quiesce(ctx, f, "w")
	if err = tf.Enable(ctx, f, "w"); err != nil {
		t.Fatal(err)
	}
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, onceWorkflow, "safe")
	if err != nil {
		t.Fatal(err)
	}
	var out string
	if err = run.Get(ctx, &out); err != nil || out != "safe" {
		t.Fatalf("native result=%s %v", out, err)
	}
	if calls.Load() != 0 {
		t.Fatal("caller slice mutation injected an outer WorkerInterceptor")
	}
}
