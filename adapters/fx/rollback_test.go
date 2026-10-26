package fxforest_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func TestNativeRollbackWaitsForPreparedIngressBeforeStoppingDependencies(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	stopping, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var resourceClosed atomic.Bool
	boom := errors.New("second hook failed")
	d, err := ff.Definition[service](f, ff.Options{TypeID: "rollback", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
			l.Append(fx.Hook{OnStart: func(context.Context) error {
				if err := b.OwnIngress(func() error { close(stopping); return nil }, done); err != nil {
					return err
				}
				go func() { <-release; close(done) }()
				return nil
			}, OnStop: func(context.Context) error { resourceClosed.Store(true); return nil }})
			l.Append(fx.Hook{OnStart: func(context.Context) error { return boom }})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	result := make(chan error, 1)
	go func() {
		_, e := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("fx", d.Ref())}})
		result <- e
	}()
	wait(t, ctx, stopping)
	if resourceClosed.Load() {
		t.Fatal("native rollback closed dependency before ingress exited")
	}
	select {
	case e := <-result:
		t.Fatal("startup abandoned ingress", e)
	default:
	}
	once.Do(func() { close(release) })
	select {
	case e := <-result:
		if !errors.Is(e, boom) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drained(t, ctx, f)
	if !resourceClosed.Load() {
		t.Fatal("native rollback did not finish")
	}
}

func TestNativeRollbackErrorsAreNotErasedByLaterNoopStop(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	failedStart, failedStop := errors.New("start failed"), errors.New("rollback failed")
	d, err := ff.Definition[service](f, ff.Options{TypeID: "failed-rollback", Version: "1", TreeID: "fx", BuildModule: func(*forest.BuildEnv, *ff.Bridge) (fx.Option, error) {
		return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
			l.Append(fx.Hook{OnStop: func(context.Context) error { return failedStop }})
			l.Append(fx.Hook{OnStart: func(context.Context) error { return failedStart }})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	_, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("fx", d.Ref())}})
	if !errors.Is(err, failedStart) || !errors.Is(err, failedStop) {
		t.Fatal(err)
	}
	if err = f.Wait(ctx); !errors.Is(err, ff.ErrOwnership) || !errors.Is(err, failedStop) {
		t.Fatal("rollback error erased", err)
	}
	if err = f.Close(ctx); !errors.Is(err, failedStop) {
		t.Fatal(err)
	}
}
