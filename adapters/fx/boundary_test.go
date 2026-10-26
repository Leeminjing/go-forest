package fxforest_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func TestInvalidExportsAndConstructorPanicDoNotPublish(t *testing.T) {
	for _, kind := range []string{"private", "nil", "missing", "panic"} {
		t.Run(kind, func(t *testing.T) {
			ctx := testContext(t)
			f := forest.New(forest.Options{})
			defer f.Close(ctx)
			d, err := ff.Definition[service](f, ff.Options{TypeID: kind, Version: "1", TreeID: "fx", BuildModule: func(*forest.BuildEnv, *ff.Bridge) (fx.Option, error) {
				ctor := func() service {
					if kind == "panic" {
						panic("constructor panic")
					}
					if kind == "nil" {
						return nil
					}
					return serviceFunc(func(context.Context) (string, error) { return "ok", nil })
				}
				switch kind {
				case "private":
					return fx.Module("hidden", fx.Provide(ctor, fx.Private)), nil
				case "missing":
					return fx.Options(), nil
				default:
					return fx.Provide(ctor), nil
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			register(t, f, d)
			g, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("fx", d.Ref())}})
			if g != 0 || !errors.Is(err, forest.ErrPrepareFailed) {
				t.Fatalf("g=%d error=%v", g, err)
			}
			drained(t, ctx, f)
		})
	}
	if _, e := ff.Definition[service](nil, ff.Options{}); !errors.Is(e, ff.ErrInvalid) {
		t.Fatal(e)
	}
}

func TestManagementBoundariesAndNativeActuatorStayPrivate(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	var bridge *ff.Bridge
	d, err := ff.Definition[service](f, ff.Options{TypeID: "managed", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		bridge = b
		return fx.Options(fx.Provide(func() service {
			return serviceFunc(func(c context.Context) (string, error) {
				if e := ff.Quiesce(c, f, "fx"); !errors.Is(e, ff.ErrNestedScope) {
					return "", errors.New("managed call can wait on itself")
				}
				if e := b.Run(c, func(context.Context) error { return nil }); !errors.Is(e, ff.ErrNestedScope) {
					return "", errors.New("nested ingress changed version")
				}
				return "ok", nil
			})
		}), fx.Invoke(func(l fx.Lifecycle) {
			if _, ok := l.(interface{ Start(context.Context) error }); ok {
				t.Error("native actuator escaped")
			}
			l.Append(fx.Hook{OnStart: func(c context.Context) error {
				if e := ff.Enable(c, f, "fx"); !errors.Is(e, ff.ErrNestedScope) {
					return errors.New("candidate management admitted")
				}
				return nil
			}})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
	if _, e := f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("fx", d.Ref(), false)}}); !errors.Is(e, forest.ErrUnsupportedHotReplace) {
		t.Fatal(e)
	}
	if err = ff.Enable(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	if v, e := value(ctx, f, "fx"); e != nil || v != "ok" {
		t.Fatal(v, e)
	}
	if _, e := bridge.Defer(func(context.Context) error { return nil }); !errors.Is(e, ff.ErrUnavailable) {
		t.Fatal("late ownership registration", e)
	}
	if e := bridge.Loop(func(context.Context) error { return nil }); !errors.Is(e, ff.ErrUnavailable) {
		t.Fatal("late loop registration", e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- ff.Quiesce(ctx, f, "fx") }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if err = ff.Enable(ctx, f, "fx"); !errors.Is(err, ff.ErrUnavailable) {
		t.Fatal("re-enabled stopped ingress", err)
	}
}

func TestCleanupErrorRemainsUnreleasedInsteadOfBeingRetried(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	boom := errors.New("cleanup failed")
	calls := 0
	d, err := ff.Definition[service](f, ff.Options{TypeID: "cleanup", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		_, e := b.Defer(func(context.Context) error { calls++; return boom })
		return fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), e
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
	apply(t, ctx, f, 1, forest.Remove("fx"))
	if err = f.Wait(ctx); !errors.Is(err, ff.ErrOwnership) || !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("cleanup was repeated")
	}
	if err = f.Close(ctx); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	for _, i := range f.Snapshot().Instances {
		if i.TreeID == "fx" && i.State == forest.Stopped {
			t.Fatal("failed release marked stopped")
		}
	}
}

func TestConsumerRequiredLinkStillProtectsFxRemoval(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	d, e := ff.Definition[service](f, ff.Options{TypeID: "required", Version: "1", TreeID: "fx", BuildModule: func(*forest.BuildEnv, *ff.Bridge) (fx.Option, error) {
		return fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	c := forest.Definition{TypeID: "consumer", Version: "1", Ports: []forest.PortDescriptor{forest.Port[serviceAccess]("fx", forest.Required())}, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	register(t, f, d, c)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()), forest.AddRoot("c", c.Ref()), forest.Connect(forest.Link{ID: "fx", Consumer: "c", Port: "fx", Provider: "fx", Capability: "service"}))
	if _, err := f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Remove("fx")}}); !errors.Is(err, forest.ErrDependencyInUse) {
		t.Fatal(err)
	}
	if f.Snapshot().Generation != 1 {
		t.Fatal("failed removal changed generation")
	}
	apply(t, ctx, f, 1, forest.Remove("c"), forest.Remove("fx"))
	drained(t, ctx, f)
}

func TestConfigurationRejectsInvalidMetadata(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	_, err := ff.Definition[service](f, ff.Options{TypeID: strings.Repeat("x", 257), Version: "1", TreeID: "fx", BuildModule: func(*forest.BuildEnv, *ff.Bridge) (fx.Option, error) { return fx.Options(), nil }})
	if !errors.Is(err, ff.ErrInvalid) {
		t.Fatal(err)
	}
}
