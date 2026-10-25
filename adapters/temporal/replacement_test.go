package temporalforest_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

type reader interface{ Read(context.Context) string }
type readFunc func(context.Context) string

func (r readFunc) Read(c context.Context) string { return r(c) }
func TestClientAndActivityKeepOldBindingAcrossReplacement(t *testing.T) {
	connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	queue := "replace-" + time.Now().Format("150405.000000000")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var stopped atomic.Int32
	provider := func(version string) forest.Definition {
		return forest.Definition{TypeID: "reader", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[reader]("reader", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{OnStop: func(context.Context) error {
				if version == "1" {
					stopped.Add(1)
				}
				return nil
			}}, forest.Provide(e, "reader", readFunc(func(context.Context) string { return version }))
		}}
	}
	p1, p2 := provider("1"), provider("2")
	d, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "worker", Version: "1", TreeID: "worker", TaskQueue: queue, Client: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS")}, Ports: []forest.PortDescriptor{forest.Port[reader]("reader", forest.Required())}, BuildRegistration: func(e *forest.BuildEnv) (tf.Register, error) {
		ref, err := forest.Reference[reader](e, "reader")
		if err != nil {
			return nil, err
		}
		return func(r tf.Registry) error {
			r.RegisterWorkflow(onceWorkflow)
			r.RegisterActivityWithOptions(func(c context.Context) (out string, err error) {
				err = ref.Use(c, func(v reader) error { out = v.Read(c); return nil })
				if err != nil {
					return
				}
				if out == "1" {
					close(entered)
					<-release
				}
				err = ref.Use(c, func(v reader) error { out += "/" + v.Read(c); return nil })
				return
			}, activity.RegisterOptions{Name: "read"})
			return nil
		}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	cd, err := tf.ClientDefinition(f, tf.ClientOptions{TypeID: "client", Version: "1", Native: client.Options{HostPort: os.Getenv("FOREST_TEMPORAL_ADDRESS")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range []forest.Definition{d, cd, p1, p2} {
		if err = f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("worker", d.Ref()), forest.AddRoot("client", cd.Ref()), forest.AddRoot("reader", p1.Ref()), forest.Connect(forest.Link{ID: "read", Consumer: "worker", Port: "reader", Provider: "reader", Capability: "reader"})}}); err != nil {
		t.Fatal(err)
	}
	defer tf.Quiesce(ctx, f, "worker")
	if err = tf.Enable(ctx, f, "worker"); err != nil {
		t.Fatal(err)
	}
	use := func(fn func(client.Client) error) error {
		return f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[tf.ClientAccess](c, f, "client", "client", func(access tf.ClientAccess) error { return access.Use(c, fn) })
		})
	}
	var oldID string
	err = use(func(c client.Client) error {
		r, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-old", TaskQueue: queue}, onceWorkflow, "read")
		if err == nil {
			oldID = r.GetRunID()
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("reader", p2.Ref(), false)}}); err != nil {
		t.Fatal(err)
	}
	var out string
	err = use(func(c client.Client) error {
		r, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-new", TaskQueue: queue}, onceWorkflow, "read")
		if err != nil {
			return err
		}
		return r.Get(ctx, &out)
	})
	if err != nil || out != "2/2" {
		t.Fatalf("new=%s %v", out, err)
	}
	if stopped.Load() != 0 {
		t.Fatal("old provider closed during Activity")
	}
	once.Do(func() { close(release) })
	err = use(func(c client.Client) error { return c.GetWorkflow(ctx, queue+"-old", oldID).Get(ctx, &out) })
	if err != nil || out != "1/1" {
		t.Fatalf("old=%s %v", out, err)
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if stopped.Load() != 1 {
		t.Fatal("old provider not released")
	}
}
