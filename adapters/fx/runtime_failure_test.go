package fxforest_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func TestLoopFailureIsCheckableWithoutPoisoningResourceCleanup(t *testing.T) {
	for _, panicCase := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicCase], func(t *testing.T) {
			ctx := testContext(t)
			f := forest.New(forest.Options{})
			boom := errors.New("loop failed")
			exited := make(chan struct{})
			var released atomic.Int32
			d, err := ff.Definition[service](f, ff.Options{TypeID: "runtime-failure", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
				_, e := b.Defer(func(context.Context) error { released.Add(1); return nil })
				if e != nil {
					return nil, e
				}
				return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
					l.Append(fx.Hook{OnStart: func(context.Context) error {
						return b.Loop(func(context.Context) error {
							defer close(exited)
							if panicCase {
								panic("private payload")
							}
							return boom
						})
					}})
				})), nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			register(t, f, d)
			apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
			if err = ff.Enable(ctx, f, "fx"); err != nil {
				t.Fatal(err)
			}
			wait(t, ctx, exited)
			err = ff.Quiesce(ctx, f, "fx")
			if !errors.Is(err, ff.ErrRuntime) {
				t.Fatalf("lost runtime failure: %v", err)
			}
			cause := boom
			if panicCase {
				cause = ff.ErrPanic
			}
			if !errors.Is(err, cause) {
				t.Fatal(err)
			}
			var failure *ff.Failure
			if !errors.As(err, &failure) || failure.Stage != "loop" || failure.Tree != "fx" {
				t.Fatal("missing runtime context", err)
			}
			s, e := ff.Inspect(ctx, f, "fx")
			if e != nil || !s.RuntimeFailed || s.OwnershipFailed {
				t.Fatalf("status=%+v %v", s, e)
			}
			apply(t, ctx, f, 1, forest.Remove("fx"))
			drained(t, ctx, f)
			if released.Load() != 1 {
				t.Fatal("runtime error prevented resource cleanup")
			}
			if err = f.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
