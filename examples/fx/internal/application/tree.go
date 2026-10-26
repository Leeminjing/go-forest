// Package application owns the consuming contracts, independently of Fx's DI
// graph and the concrete business provider implementation.
package application

import (
	"context"

	"github.com/Leeminjing/go-forest/forest"
)

type Source interface {
	Read(context.Context, string) (string, error)
}
type Service interface {
	Render(context.Context, string) (string, error)
}
type FxAccess interface {
	Use(context.Context, func(context.Context, Service) error) error
}

func Definition() forest.Definition {
	return forest.Definition{TypeID: "fx-demo.application", Version: "1", Ports: []forest.PortDescriptor{forest.Port[FxAccess]("fx", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Service]("render", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[FxAccess](e, "fx")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "render", &application{fx: ref})
	}}
}

type application struct{ fx forest.Ref[FxAccess] }

func (a *application) Render(ctx context.Context, text string) (out string, err error) {
	err = a.fx.Use(ctx, func(access FxAccess) error {
		return access.Use(ctx, func(call context.Context, s Service) error { var e error; out, e = s.Render(call, text); return e })
	})
	return
}
