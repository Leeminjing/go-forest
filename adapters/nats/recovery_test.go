package natsforest_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRedeliveryIsBoundedAndFailuresObservable(t *testing.T) {
	nc, js, name := broker(t)
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	h := hdef("fail", func(context.Context, nt.Message) error { return errors.New("deliberate failure") })
	d := consumeDef(t, f, nc, name)
	for _, def := range []forest.Definition{h, d} {
		if err := f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	apply(t, f, forest.AddRoot("consumer", d.Ref()), forest.AddRoot("handler", h.Ref()), forest.Connect(forest.Link{ID: "handler", Consumer: "consumer", Port: "handler", Provider: "handler", Capability: "handle"}))
	ctx := context.Background()
	if err := f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, f, "consumer", "control", func(w nt.Control) error { return w.Enable(c) })
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, name, []byte("fail"), jetstream.WithMsgID("bad-event")); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return status(t, f).Terminal })
	s := status(t, f)
	if s.Failed != 3 || s.Handled != 0 || s.LastEventID != "bad-event" || s.LastStage != "handler" {
		t.Fatalf("failure hidden: %+v", s)
	}
}

func TestInvalidNativeConsumerContractFailsPreparation(t *testing.T) {
	nc, js, name := broker(t)
	ctx := context.Background()
	_, err := js.UpdateConsumer(ctx, name, jetstream.ConsumerConfig{Durable: "worker", FilterSubject: name, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second, MaxDeliver: -1, MaxAckPending: 2})
	if err != nil {
		t.Fatal(err)
	}
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	d := consumeDef(t, f, nc, name)
	h := hdef("1", func(context.Context, nt.Message) error { t.Error("invalid consumer entered"); return nil })
	for _, def := range []forest.Definition{d, h} {
		if err = f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	_, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("consumer", d.Ref()), forest.AddRoot("handler", h.Ref()), forest.Connect(forest.Link{ID: "h", Consumer: "consumer", Port: "handler", Provider: "handler", Capability: "handle"})}})
	if !errors.Is(err, nt.ErrInvalidOptions) || f.Snapshot().Generation != 0 {
		t.Fatal(err)
	}
}
func TestBrokerRestartRecoversWithoutRebuildingForest(t *testing.T) {
	container := os.Getenv("FOREST_NATS_RESTART_CONTAINER")
	if container == "" {
		t.Skip("requires an explicitly owned restartable NATS container")
	}
	nc, js, name := broker(t)
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	ctx := context.Background()
	calls := make(chan string, 4)
	h := hdef("1", func(_ context.Context, m nt.Message) error { calls <- m.ID; return nil })
	d := consumeDef(t, f, nc, name)
	for _, def := range []forest.Definition{h, d} {
		if err := f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	apply(t, f, forest.AddRoot("consumer", d.Ref()), forest.AddRoot("handler", h.Ref()), forest.Connect(forest.Link{ID: "h", Consumer: "consumer", Port: "handler", Provider: "handler", Capability: "handle"}))
	if err := f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, f, "consumer", "control", func(w nt.Control) error { return w.Enable(c) })
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, name, []byte("one"), jetstream.WithMsgID("before")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
	case <-time.After(3 * time.Second):
		t.Fatal("no first message")
	}
	commandCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(commandCtx, "docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("restart: %v %s", err, output)
	}
	wait(t, func() bool { return nc.IsConnected() })
	if _, err := js.Publish(commandCtx, name, []byte("two"), jetstream.WithMsgID("after")); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-calls:
		if id != "after" {
			t.Fatal(id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not recover")
	}
	if f.Snapshot().Generation != 1 {
		t.Fatal("forest was rebuilt")
	}
}
