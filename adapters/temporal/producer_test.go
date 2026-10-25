package temporalforest_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

func TestManagedProducerJoinsAfterFunctionReturn(t *testing.T) {
	c := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	queue := "producer-" + time.Now().Format("150405.000000000")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var saved context.Context
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "producer", Version: "1", TreeID: "w", TaskQueue: queue, Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS")}, BuildRegistration: func(*forest.BuildEnv) (tf.Register, error) {
		return func(r tf.Registry) error {
			r.RegisterWorkflow(onceWorkflow)
			r.RegisterActivityWithOptions(func(c context.Context) (string, error) {
				saved = c
				err := tf.Go(c, func(context.Context) error { close(entered); <-release; return nil })
				return "joined", err
			}, activity.RegisterOptions{Name: "produce"})
			return nil
		}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("w", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	if err = tf.Enable(ctx, f, "w"); err != nil {
		t.Fatal(err)
	}
	defer tf.Quiesce(ctx, f, "w")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, onceWorkflow, "produce")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	s, err := tf.Inspect(ctx, f, "w")
	if err != nil || s.Inflight != 1 {
		t.Fatalf("producer escaped attempt: %+v %v", s, err)
	}
	once.Do(func() { close(release) })
	var out string
	if err = run.Get(ctx, &out); err != nil || out != "joined" {
		t.Fatalf("result=%s %v", out, err)
	}
	if err = tf.Guard(saved, func() error { t.Error("late resource access admitted"); return nil }); !errors.Is(err, tf.ErrUnavailable) {
		t.Fatalf("late guard=%v", err)
	}
}
