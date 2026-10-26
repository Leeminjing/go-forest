package einodemo_test

import (
	"context"
	"sync"
	"testing"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestServerCancellationReachesStreamButCannotReleaseNoncooperativeWork(t *testing.T) {
	ctx, o := config(t)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once, free sync.Once
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "stream.producer" {
			once.Do(func() { close(entered) })
			<-c.Done()
			close(canceled)
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
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e = r.Cancel(ctx, id); e != nil {
		t.Fatal(e)
	}
	select {
	case <-canceled:
	case <-time.After(6 * time.Second):
		t.Fatal("server cancellation did not reach native stream")
	}
	if p := r.Resources(); !p.NativeOpen || !p.BusinessOpen || !p.Claim {
		t.Fatal("cancellation released live producer", p)
	}
	free.Do(func() { close(release) })
	_, e = r.Result(ctx, id)
	if !temporal.IsCanceledError(e) {
		t.Fatal("native cancellation lost", e)
	}
	if r.Counts().Query != 0 || r.Counts().Apply != 0 {
		t.Fatal("cancelled stream entered Agent")
	}
}
