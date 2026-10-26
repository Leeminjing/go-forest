package fxforest_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func wait(t *testing.T, ctx context.Context, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// A previously reported Core drain timeout remains observable until its async
// Stop goroutine records actual completion. Re-observe rather than hiding it.
func drained(t *testing.T, ctx context.Context, f *forest.Forest) {
	t.Helper()
	for {
		err := f.Wait(ctx)
		if err == nil {
			return
		}
		if !errors.Is(err, forest.ErrDrainTimeout) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
func TestCancelledStartJoinsRealHookBeforeRollback(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	d, err := ff.Definition[service](f, ff.Options{TypeID: "slow", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		_, e := b.Defer(func(context.Context) error { closed.Add(1); return nil })
		if e != nil {
			return nil, e
		}
		return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ready", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
			l.Append(fx.Hook{OnStart: func(c context.Context) error { close(entered); <-c.Done(); close(cancelled); <-release; return nil }})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	prep, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := f.Apply(prep, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("fx", d.Ref())}})
		result <- err
	}()
	wait(t, ctx, entered)
	cancel()
	wait(t, ctx, cancelled)
	select {
	case err = <-result:
		t.Fatalf("abandoned OnStart: %v", err)
	default:
	}
	if closed.Load() != 0 {
		t.Fatal("resource closed during hook")
	}
	close(release)
	select {
	case err = <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatal("resource not reclaimed")
	}
	if err = f.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStopDeadlineDoesNotProveHookOrResourceRelease(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{DrainTimeout: 20 * time.Millisecond})
	cancelled, release := make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	d, err := ff.Definition[service](f, ff.Options{TypeID: "slow-stop", Version: "1", TreeID: "fx", StopTimeout: 20 * time.Millisecond, BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		_, e := b.Defer(func(context.Context) error { closed.Add(1); return nil })
		if e != nil {
			return nil, e
		}
		return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ready", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
			l.Append(fx.Hook{OnStop: func(c context.Context) error { <-c.Done(); close(cancelled); <-release; return nil }})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
	apply(t, ctx, f, 1, forest.Remove("fx"))
	wait(t, ctx, cancelled)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err = f.Wait(short); !errors.Is(err, forest.ErrDrainTimeout) {
		t.Fatal("premature drain", err)
	}
	if closed.Load() != 0 {
		t.Fatal("resource freed during OnStop")
	}
	close(release)
	drained(t, ctx, f)
	if closed.Load() != 1 {
		t.Fatal("cleanup not completed after physical exit")
	}
	if err = f.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
