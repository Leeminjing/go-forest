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

func TestChildTaskKeepsScopeAfterServiceReturnsAndRejectsLateWork(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	returned, childCancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var saved context.Context
	var closed atomic.Int32
	d, err := ff.Definition[service](f, ff.Options{TypeID: "tasks", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		_, err := b.Defer(func(context.Context) error { closed.Add(1); return nil })
		if err != nil {
			return nil, err
		}
		return fx.Provide(func() service {
			return serviceFunc(func(c context.Context) (string, error) {
				saved = c
				err := ff.Go(c, func(c context.Context) error {
					<-c.Done()
					close(childCancelled)
					<-release
					if _, err := f.Generation(c); err != nil {
						return err
					}
					return nil
				})
				close(returned)
				return "ok", err
			})
		}), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
	if err = ff.Enable(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := value(ctx, f, "fx"); done <- e }()
	wait(t, ctx, returned)
	wait(t, ctx, childCancelled)
	if err = ff.Go(saved, func(context.Context) error { return nil }); !errors.Is(err, forest.ErrScopeClosed) {
		t.Fatalf("late Go=%v", err)
	}
	if err = ff.Guard(saved, func(context.Context) error { return nil }); !errors.Is(err, forest.ErrScopeClosed) {
		t.Fatalf("late Guard=%v", err)
	}
	apply(t, ctx, f, 1, forest.Remove("fx"))
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err = f.Wait(short); !errors.Is(err, forest.ErrDrainTimeout) {
		t.Fatal(err)
	}
	if closed.Load() != 0 {
		t.Fatal("App resource freed while child still alive")
	}
	close(release)
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drained(t, ctx, f)
	if closed.Load() != 1 {
		t.Fatal("resource not released")
	}
}
