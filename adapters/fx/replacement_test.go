package fxforest_test

import (
	"context"
	"sync/atomic"
	"testing"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

// This dependency belongs to the consumer; the provider never imports Fx.
type serviceAccess interface {
	Use(context.Context, func(context.Context, service) error) error
}

func TestDeclaredConsumerAndFxProxyKeepOneVersionAcrossRebindAndReplace(t *testing.T) {
	for _, mode := range []string{"rebind", "replace"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			f := forest.New(forest.Options{})
			defer f.Close(ctx)
			entered, release := make(chan struct{}), make(chan struct{})
			var oldStopped, builds atomic.Int32
			provider := func(version string) forest.Definition {
				return forest.Definition{TypeID: "provider", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[service]("value", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
					return forest.Hooks{OnStop: func(context.Context) error {
						if version == "1" {
							oldStopped.Add(1)
						}
						return nil
					}}, forest.Provide(e, "value", serviceFunc(func(context.Context) (string, error) { return version, nil }))
				}}
			}
			p1, p2 := provider("1"), provider("2")
			d, err := ff.Definition[service](f, ff.Options{TypeID: "fx", Version: "1", TreeID: "fx", Ports: []forest.PortDescriptor{forest.Port[service]("source", forest.Required())}, BuildModule: func(e *forest.BuildEnv, _ *ff.Bridge) (fx.Option, error) {
				ref, err := forest.Reference[service](e, "source")
				if err != nil {
					return nil, err
				}
				return fx.Provide(func() service {
					builds.Add(1)
					return serviceFunc(func(c context.Context) (out string, err error) {
						err = ref.Use(c, func(s service) error { var e error; out, e = s.Value(c); return e })
						if err != nil {
							return
						}
						if out == "1" {
							close(entered)
							<-release
						}
						err = ref.Use(c, func(s service) error { v, e := s.Value(c); out += "/" + v; return e })
						return
					})
				}), nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			caller := forest.Definition{TypeID: "caller", Version: "1", Ports: []forest.PortDescriptor{forest.Port[serviceAccess]("fx", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[service]("value", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
				r, err := forest.Reference[serviceAccess](e, "fx")
				if err != nil {
					return nil, err
				}
				return forest.Hooks{}, forest.Provide(e, "value", serviceFunc(func(c context.Context) (out string, err error) {
					err = r.Use(c, func(a serviceAccess) error {
						return a.Use(c, func(sc context.Context, s service) error { var e error; out, e = s.Value(sc); return e })
					})
					return
				}))
			}}
			register(t, f, p1, p2, d, caller)
			link := forest.Link{ID: "source", Consumer: "fx", Port: "source", Provider: "p", Capability: "value"}
			apply(t, ctx, f, 0, forest.AddRoot("p", p1.Ref()), forest.AddRoot("fx", d.Ref()), forest.AddRoot("caller", caller.Ref()), forest.Connect(link), forest.Connect(forest.Link{ID: "fx", Consumer: "caller", Port: "fx", Provider: "fx", Capability: "service"}))
			if err = ff.Enable(ctx, f, "fx"); err != nil {
				t.Fatal(err)
			}
			read := func() (out string, err error) {
				err = f.Invoke(ctx, func(c context.Context) error {
					return forest.Use[service](c, f, "caller", "value", func(s service) error { var e error; out, e = s.Value(c); return e })
				})
				return
			}
			type answer struct {
				value string
				err   error
			}
			old := make(chan answer, 1)
			go func() { v, e := read(); old <- answer{v, e} }()
			wait(t, ctx, entered)
			if mode == "rebind" {
				link.Provider = "new"
				apply(t, ctx, f, 1, forest.AddRoot("new", p2.Ref()), forest.Rebind(link), forest.Remove("p"))
			} else {
				apply(t, ctx, f, 1, forest.Replace("p", p2.Ref(), false))
			}
			if v, e := read(); e != nil || v != "2/2" {
				t.Fatalf("new=%q %v", v, e)
			}
			if oldStopped.Load() != 0 {
				t.Fatal("provider released while old scope is alive")
			}
			close(release)
			select {
			case a := <-old:
				if a.err != nil || a.value != "1/1" {
					t.Fatalf("old=%q %v", a.value, a.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			drained(t, ctx, f)
			if oldStopped.Load() != 1 || builds.Load() != 1 {
				t.Fatalf("stopped=%d Fx builds=%d", oldStopped.Load(), builds.Load())
			}
			if err = ff.Quiesce(ctx, f, "fx"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
