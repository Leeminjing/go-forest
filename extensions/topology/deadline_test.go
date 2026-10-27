package topology

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func TestCloseDeadlineDoesNotWaitForAnUncooperativePreparation(t *testing.T) {
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	r := catalog.entries[m.Trees[0].Definition]
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.Definition.Build = func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStart: func(context.Context) error { close(entered); <-release; return nil }}, forest.Provide(env, "number", value(7))
	}
	catalog.entries[r.Definition.Ref()] = r
	s, e := Open(t.TempDir(), Options{})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	c, e := NewController(f, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		once.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, e := c.Restore(context.Background(), Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"})
		done <- e
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation did not enter")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- c.Close(deadline) }()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected pending management timeout: %v", err)
		}
	case <-time.After(time.Second):
		once.Do(func() { close(release) })
		<-done
		<-closed
		t.Fatal("Close ignored its deadline while another management operation prepared")
	}
	if e = s.Close(); !errors.Is(e, ErrStoreBusy) {
		t.Fatalf("timeout released ownership: %v", e)
	}
	once.Do(func() { close(release) })
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
