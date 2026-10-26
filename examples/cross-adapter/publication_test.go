package crossdemo_test

import (
	"context"
	"sync"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/forest"
)

// Err observes the real publication boundary, avoiding a timing race between
// a watcher and the constructor's first post-commit inspection.
type cancelOnPublication struct {
	context.Context
	f      *forest.Forest
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelOnPublication) Err() error {
	for _, event := range c.f.Events() {
		if event.Generation == 1 && event.Result == "success" {
			c.once.Do(c.cancel)
		}
	}
	return c.Context.Err()
}

func TestCallerCancellationAfterPublicationKeepsBorrowedForestManageable(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	o.Forest = f
	caller, cancel := context.WithCancel(ctx)
	defer cancel()
	publication := &cancelOnPublication{Context: caller, f: f, cancel: cancel}
	r, err := demo.New(publication, o)
	if r == nil || err != nil {
		t.Fatalf("committed runtime became inaccessible: runtime=%v error=%v", r != nil, err)
	}
	if publication.Err() != context.Canceled || f.Snapshot().Generation != 1 {
		t.Fatal("did not exercise committed/cancelled boundary")
	}
	if p := r.Resources(ctx); !p.FileOpen || !p.PoolResponding {
		t.Fatal("committed resources missing", p)
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.Snapshot().Trees) != 0 {
		t.Fatal("borrowed forest retained composition")
	}
	if p := r.Resources(ctx); p.FileOpen || p.FileCloses != 1 || p.PoolResponding {
		t.Fatal("committed resources leaked", p)
	}
	if err = f.Invoke(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal("borrowed forest was closed", err)
	}
}
