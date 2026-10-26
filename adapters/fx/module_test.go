package fxforest_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

type service interface {
	Value(context.Context) (string, error)
}
type serviceFunc func(context.Context) (string, error)

func (s serviceFunc) Value(ctx context.Context) (string, error) { return s(ctx) }

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func register(t *testing.T, f *forest.Forest, defs ...forest.Definition) {
	t.Helper()
	for _, d := range defs {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
}
func apply(t *testing.T, ctx context.Context, f *forest.Forest, g uint64, ops ...forest.Operation) {
	t.Helper()
	if _, err := f.Apply(ctx, forest.ChangePlan{BaseGeneration: g, Operations: ops}); err != nil {
		t.Fatal(err)
	}
}
func value(ctx context.Context, f *forest.Forest, id forest.TreeID) (out string, err error) {
	err = f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[ff.Access[service]](c, f, id, "service", func(a ff.Access[service]) error {
			return a.Use(c, func(call context.Context, s service) error { var e error; out, e = s.Value(call); return e })
		})
	})
	return
}

func TestNativeModulesKeepPrivateNamedGroupedAndIndependentObjects(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	t.Cleanup(func() {
		if e := f.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	var ids, lazy atomic.Int32
	var mu sync.Mutex
	var lifecycle []string
	record := func(s string) { mu.Lock(); defer mu.Unlock(); lifecycle = append(lifecycle, s) }
	type private struct{ name string }
	type ignored struct{}
	type input struct {
		fx.In
		Private *private
		Label   string `name:"label"`
		Parts   []int  `group:"parts"`
	}
	module := func(_ *forest.BuildEnv, _ *ff.Bridge) (fx.Option, error) {
		id := ids.Add(1)
		return fx.Module("native",
			fx.Provide(func() *private { return &private{name: "hidden"} }, fx.Private),
			fx.Provide(fx.Annotate(func() string { return "oak" }, fx.ResultTags(`name:"label"`))),
			fx.Provide(fx.Annotate(func() int { return 7 }, fx.ResultTags(`group:"parts"`))),
			fx.Provide(func() *ignored { lazy.Add(1); return &ignored{} }),
			fx.Provide(fx.Annotate(func(p input) service {
				return serviceFunc(func(context.Context) (string, error) {
					return fmt.Sprintf("%s:%s:%d:%d", p.Private.name, p.Label, p.Parts[0], id), nil
				})
			},
				fx.OnStart(func(context.Context, service) error { record(fmt.Sprintf("start%d", id)); return nil }),
				fx.OnStop(func(context.Context, service) error { record(fmt.Sprintf("stop%d", id)); return nil }))),
		), nil
	}
	defs := make([]forest.Definition, 2)
	for i, id := range []forest.TreeID{"one", "two"} {
		var err error
		defs[i], err = ff.Definition[service](f, ff.Options{TypeID: string(id), Version: "1", TreeID: id, BuildModule: module})
		if err != nil {
			t.Fatal(err)
		}
	}
	register(t, f, defs...)
	apply(t, ctx, f, 0, forest.AddRoot("one", defs[0].Ref()), forest.AddRoot("two", defs[1].Ref()))
	if _, err := value(ctx, f, "one"); err == nil {
		t.Fatal("business admitted before Enable")
	}
	for _, id := range []forest.TreeID{"one", "two"} {
		if err := ff.Enable(ctx, f, id); err != nil {
			t.Fatal(err)
		}
		if err := ff.Enable(ctx, f, id); err != nil {
			t.Fatal(err)
		}
	}
	a, err := value(ctx, f, "one")
	if err != nil || a != "hidden:oak:7:1" {
		t.Fatalf("one=%q %v", a, err)
	}
	b, err := value(ctx, f, "two")
	if err != nil || b != "hidden:oak:7:2" {
		t.Fatalf("two=%q %v", b, err)
	}
	if lazy.Load() != 0 {
		t.Fatal("unused constructor was forced")
	}
	if err = ff.Quiesce(ctx, f, "one"); err != nil {
		t.Fatal(err)
	}
	apply(t, ctx, f, 1, forest.Remove("one"))
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err = value(ctx, f, "two"); err != nil || b != "hidden:oak:7:2" {
		t.Fatalf("other App damaged: %q %v", b, err)
	}
	mu.Lock()
	got := append([]string(nil), lifecycle...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"start1", "start2", "stop1"}) {
		t.Fatalf("lifecycle=%v", got)
	}
}
