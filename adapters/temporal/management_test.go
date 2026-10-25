package temporalforest_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func TestUnsupportedOptionsFailBeforeAnyConnection(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	base := tf.WorkerOptions{TypeID: "worker", Version: "1", TreeID: "w", TaskQueue: "queue", BuildRegistration: func(*forest.BuildEnv) (tf.Register, error) { return func(tf.Registry) error { return nil }, nil }}
	for _, options := range []worker.Options{{BackgroundActivityContext: context.Background()}, {OnFatalError: func(error) {}}, {EnableSessionWorker: true}, {LocalActivityWorkerOnly: true}} {
		base.Native = options
		if _, err := tf.WorkerDefinition(f, base); !errors.Is(err, tf.ErrUnsupported) {
			t.Fatalf("unsupported mode accepted: %v", err)
		}
	}
	base.Native = worker.Options{}
	for _, namespace := range []string{strings.Repeat("n", 257), "bad\x00name", string([]byte{255})} {
		base.Client.Namespace = namespace
		if _, err := tf.WorkerDefinition(f, base); !errors.Is(err, tf.ErrInvalid) {
			t.Fatalf("namespace accepted: %v", err)
		}
		if _, err := tf.ClientDefinition(f, tf.ClientOptions{TypeID: "client", Version: "1", Native: base.Client}); !errors.Is(err, tf.ErrInvalid) {
			t.Fatalf("client namespace accepted: %v", err)
		}
	}
	base.Client.Namespace = ""
	base.TaskQueue = "bad\x00queue"
	if _, err := tf.WorkerDefinition(f, base); !errors.Is(err, tf.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := tf.ClientDefinition(nil, tf.ClientOptions{}); !errors.Is(err, tf.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestInvalidNativeRegistrationRollsBack(t *testing.T) {
	connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "invalid", Version: "1", TreeID: "w", TaskQueue: "invalid-registrations", Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS")}, BuildRegistration: func(*forest.BuildEnv) (tf.Register, error) {
		return func(r tf.Registry) error {
			r.RegisterWorkflow(onceWorkflow)
			r.RegisterWorkflow(onceWorkflow)
			return nil
		}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("w", d.Ref())}}); !errors.Is(err, tf.ErrPanic) || !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatalf("native registration panic leaked: %v", err)
	}
	if f.Snapshot().Generation != 0 {
		t.Fatal("invalid registry committed")
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, i := range f.Snapshot().Instances {
		if i.State != forest.Stopped {
			t.Fatalf("partial native registry not cleaned: %+v", i)
		}
	}
}
func TestStartFailureAndRepeatedQuiesceKeepCommittedVersion(t *testing.T) {
	connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "worker", Version: "1", TreeID: "w", TaskQueue: "no-tasks", Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS"), Namespace: "missing-" + time.Now().Format("150405000000")}, BuildRegistration: func(*forest.BuildEnv) (tf.Register, error) {
		return func(r tf.Registry) error { r.RegisterWorkflow(onceWorkflow); return nil }, nil
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
	err = f.Invoke(ctx, func(c context.Context) error { return tf.Enable(c, f, "w") })
	if !errors.Is(err, tf.ErrNestedScope) {
		t.Fatalf("nested/old management scope accepted: %v", err)
	}
	if err = tf.Enable(ctx, f, "w"); err == nil {
		t.Fatal("missing namespace startup succeeded")
	}
	if f.Snapshot().Generation != 1 {
		t.Fatal("enable failure rolled back a committed topology")
	}
	for n := 0; n < 2; n++ {
		if err = tf.Quiesce(ctx, f, "w"); err != nil {
			t.Fatal(err)
		}
		s, err := tf.Inspect(ctx, f, "w")
		if err != nil || s.Phase != "stopped" || !s.NativeStopped {
			t.Fatalf("quiesce changed terminal state: %+v %v", s, err)
		}
	}
	if err = tf.Enable(ctx, f, "w"); !errors.Is(err, tf.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("w", d.Ref(), false)}}); !errors.Is(err, forest.ErrUnsupportedHotReplace) {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Remove("w")}}); err != nil {
		t.Fatal(err)
	}
	if err = tf.Enable(ctx, f, "w"); !errors.Is(err, forest.ErrNotFound) {
		t.Fatalf("removed worker enabled: %v", err)
	}
}
