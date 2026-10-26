package fxforest_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func TestIndependentCancellationAndConcurrentQuiesceJoinAllCalls(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	const count = 24
	entered, cancelled := make(chan struct{}, count), make(chan int, count)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var stopped atomic.Int32
	type requestID struct{}
	d, err := ff.Definition[service](f, ff.Options{TypeID: "concurrent", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		_, e := b.Defer(func(context.Context) error { stopped.Add(1); return nil })
		return fx.Provide(func() service {
			return serviceFunc(func(c context.Context) (string, error) {
				entered <- struct{}{}
				<-c.Done()
				cancelled <- c.Value(requestID{}).(int)
				<-release
				return "", c.Err()
			})
		}), e
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
	if err = ff.Enable(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, count)
	cancelOne := func() {}
	others := make([]context.Context, 0, count-1)
	for i := 0; i < count; i++ {
		c := context.WithValue(ctx, requestID{}, i)
		if i == 0 {
			c, cancelOne = context.WithCancel(c)
		} else {
			others = append(others, c)
		}
		go func() { _, e := value(c, f, "fx"); results <- e }()
	}
	defer cancelOne()
	for i := 0; i < count; i++ {
		wait(t, ctx, entered)
	}
	cancelOne()
	select {
	case id := <-cancelled:
		if id != 0 {
			t.Fatalf("wrong cancelled request=%d", id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, c := range others {
		if c.Err() != nil {
			t.Fatal("one request cancelled another")
		}
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err = ff.Quiesce(short, f, "fx"); !errors.Is(err, ff.ErrDrainTimeout) {
		t.Fatal(err)
	}
	for i := 1; i < count; i++ {
		select {
		case <-cancelled:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	s, err := ff.Inspect(ctx, f, "fx")
	if err != nil || s.Inflight != count || s.Accepting {
		t.Fatalf("status=%+v %v", s, err)
	}
	if stopped.Load() != 0 {
		t.Fatal("resources closed under active calls")
	}
	once.Do(func() { close(release) })
	for i := 0; i < count; i++ {
		select {
		case e := <-results:
			if !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = ff.Quiesce(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	apply(t, ctx, f, 1, forest.Remove("fx"))
	drained(t, ctx, f)
	if stopped.Load() != 1 {
		t.Fatal("resource release count", stopped.Load())
	}
}
