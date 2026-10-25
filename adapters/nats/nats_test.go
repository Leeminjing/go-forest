package natsforest_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func broker(t *testing.T) (*nats.Conn, jetstream.JetStream, string) {
	t.Helper()
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("set FOREST_INTEGRATION=1 and FOREST_NATS_URL for real JetStream")
	}
	url := os.Getenv("FOREST_NATS_URL")
	if url == "" {
		t.Fatal("FOREST_NATS_URL required")
	}
	nc, err := nats.Connect(url, nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("FOREST_%d", time.Now().UnixNano())
	ctx := context.Background()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name}, Storage: jetstream.FileStorage, Duplicates: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(ctx, name) })
	_, err = js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second, MaxDeliver: 3, MaxAckPending: 2, FilterSubject: name})
	if err != nil {
		t.Fatal(err)
	}
	return nc, js, name
}

type handler func(context.Context, nt.Message) error

func (h handler) Handle(c context.Context, m nt.Message) error { return h(c, m) }
func hdef(version string, h handler) forest.Definition {
	return forest.Definition{TypeID: "handler", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[nt.Handler]("handle", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "handle", h)
	}}
}
func apply(t *testing.T, f *forest.Forest, ops ...forest.Operation) {
	t.Helper()
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: ops})
	if err != nil {
		t.Fatal(err)
	}
}
func control(f *forest.Forest, ctx context.Context, fn func(nt.Control) error) error {
	return f.Invoke(ctx, func(c context.Context) error { return forest.Use[nt.Control](c, f, "consumer", "control", fn) })
}
func status(t *testing.T, f *forest.Forest) nt.Status {
	t.Helper()
	var s nt.Status
	if err := control(f, context.Background(), func(c nt.Control) error { s = c.Status(); return nil }); err != nil {
		t.Fatal(err)
	}
	return s
}
func wait(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func consumeDef(t *testing.T, f *forest.Forest, nc *nats.Conn, name string) forest.Definition {
	t.Helper()
	d, err := nt.ConsumerDefinition(f, nt.ConsumerOptions{TypeID: "consumer", Version: "1", Stream: name, Consumer: "worker", Connection: nt.Connection{Ownership: nt.Borrowed, Connect: func(context.Context) (*nats.Conn, error) { return nc, nil }}, Concurrency: 2, ProcessTimeout: time.Second, RetryDelay: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestExplicitAdmissionHotReplacementAndQuiesce(t *testing.T) {
	nc, js, name := broker(t)
	ctx := context.Background()
	f := forest.New(forest.Options{DrainTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { c, x := context.WithTimeout(ctx, 3*time.Second); defer x(); f.Close(c) })
	entered := make(chan uint64, 1)
	release := make(chan struct{})
	results := make(chan string, 4)
	v1 := hdef("1", func(c context.Context, m nt.Message) error {
		g, _ := f.Generation(c)
		entered <- g
		<-release
		after, _ := f.Generation(c)
		if g != after {
			return errors.New("mixed generation")
		}
		results <- "v1"
		return nil
	})
	v2 := hdef("2", func(c context.Context, m nt.Message) error {
		g, _ := f.Generation(c)
		results <- fmt.Sprintf("v2:%d", g)
		return nil
	})
	d := consumeDef(t, f, nc, name)
	for _, def := range []forest.Definition{v1, v2, d} {
		if err := f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	apply(t, f, forest.AddRoot("handler", v1.Ref()), forest.AddRoot("consumer", d.Ref()), forest.Connect(forest.Link{ID: "handle", Consumer: "consumer", Port: "handler", Provider: "handler", Capability: "handle"}))
	_, err := f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Unlink("handle")}})
	if !errors.Is(err, forest.ErrDependencyInUse) || f.Snapshot().Generation != 1 {
		t.Fatalf("required dependency disconnected: %v", err)
	}
	_, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("consumer", d.Ref(), false)}})
	if !errors.Is(err, forest.ErrUnsupportedHotReplace) {
		t.Fatalf("unsafe consumer replacement accepted: %v", err)
	}
	if _, err := js.Publish(ctx, name, []byte("one"), jetstream.WithMsgID("one")); err != nil {
		t.Fatal(err)
	}
	native, _ := js.Consumer(ctx, name, "worker")
	info, err := native.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Delivered.Consumer != 0 {
		t.Fatal("consumed before Enable")
	}
	if err = control(f, ctx, func(c nt.Control) error { return c.Enable(ctx) }); !errors.Is(err, forest.ErrScopeClosed) {
		t.Fatalf("unscoped control context accepted: %v", err)
	}
	enable := func() error {
		return f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[nt.Control](c, f, "consumer", "control", func(w nt.Control) error { return w.Enable(c) })
		})
	}
	if err = enable(); err != nil {
		t.Fatal(err)
	}
	select {
	case g := <-entered:
		if g != 1 {
			t.Fatal(g)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message")
	}
	apply(t, f, forest.Replace("handler", v2.Ref(), false))
	if _, err = js.Publish(ctx, name, []byte("two"), jetstream.WithMsgID("two")); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-results:
		if result != "v2:2" {
			t.Fatal(result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("new call blocked by old message")
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	err = f.Invoke(short, func(c context.Context) error {
		return forest.Use[nt.Control](c, f, "consumer", "control", func(w nt.Control) error { return w.Quiesce(c) })
	})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if s := status(t, f); !s.Quiescing || s.Inflight != 1 {
		t.Fatalf("unreleased work hidden: %+v", s)
	}
	if err = enable(); !errors.Is(err, nt.ErrQuiescing) {
		t.Fatal(err)
	}
	close(release)
	select {
	case r := <-results:
		if r != "v1" {
			t.Fatal(r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old call did not finish")
	}
	wait(t, func() bool { return !status(t, f).Quiescing })
	apply(t, f, forest.Remove("consumer"))
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("borrowed connection closed: %v", err)
	}
	if _, err = js.Publish(ctx, name, []byte("three")); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateRollbackNeverConsumes(t *testing.T) {
	nc, js, name := broker(t)
	ctx := context.Background()
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	h := hdef("1", func(context.Context, nt.Message) error { t.Error("candidate processed business message"); return nil })
	d := consumeDef(t, f, nc, name)
	fail := forest.Definition{TypeID: "failure", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStart: func(context.Context) error { return errors.New("no start") }}, nil
	}}
	for _, def := range []forest.Definition{d, h, fail} {
		if err := f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := js.Publish(ctx, name, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	_, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("a-consumer", d.Ref()), forest.AddRoot("handler", h.Ref()), forest.AddRoot("z-fail", fail.Ref()), forest.Connect(forest.Link{ID: "h", Consumer: "a-consumer", Port: "handler", Provider: "handler", Capability: "handle"})}})
	if !errors.Is(err, forest.ErrPrepareFailed) || f.Snapshot().Generation != 0 {
		t.Fatal(err)
	}
	cons, _ := js.Consumer(ctx, name, "worker")
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Delivered.Consumer != 0 {
		t.Fatal("rollback consumed message")
	}
}
