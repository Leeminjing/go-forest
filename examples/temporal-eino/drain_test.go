package einodemo_test

import (
	"context"
	"sync"
	"testing"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
)

func TestWorkerStopAndStreamCloseDoNotReleaseLiveNativeProducer(t *testing.T) {
	ctx, o := config(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, free sync.Once
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "stream.producer" {
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	defer free.Do(func() { close(release) })
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Start(ctx, o.Queue, input(o)); e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual streaming producer did not enter")
	}
	short, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	e = r.Close(short)
	cancel()
	if e == nil {
		t.Fatal("close ignored native work")
	}
	until(t, ctx, func() bool {
		s, e := tf.Inspect(ctx, r.Forest, "app/worker")
		return e == nil && s.NativeStopped && s.Inflight > 0
	})
	if p := r.Resources(); !p.Claim || !p.NativeOpen || !p.BusinessOpen || p.NativeCloses != 0 || p.BusinessCloses != 0 {
		t.Fatal("resources released before producer", p)
	}
	free.Do(func() { close(release) })
	if e = r.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if p := r.Resources(); p.Claim || p.NativeOpen || p.BusinessOpen || p.NativeCloses != 1 || p.BusinessCloses != 1 {
		t.Fatal(p)
	}
}
