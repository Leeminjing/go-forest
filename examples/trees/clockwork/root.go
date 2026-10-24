// Package clockwork exposes a composed clock/formatter through a root contract.
package clockwork

import (
	"context"
	"github.com/Leeminjing/go-forest/forest"
)

// Formatter is owned by the consuming root, independent of its implementation.
type Formatter interface {
	Format(context.Context) (string, error)
}
type Output interface {
	Format(context.Context) (string, error)
}
type service struct{ formatter forest.Ref[Formatter] }

func (s service) Format(ctx context.Context) (string, error) {
	var result string
	err := s.formatter.Use(ctx, func(f Formatter) error { var err error; result, err = f.Format(ctx); return err })
	return "clockwork: " + result, err
}
func definition() forest.Definition {
	return forest.Definition{TypeID: "example.clockwork", Version: "1", HotSwappable: true, PreserveChildren: true, Reparentable: true,
		Ports:        []forest.PortDescriptor{forest.Port[Formatter]("formatter", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Output]("format", forest.Public|forest.Ancestors)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			r, err := forest.Reference[Formatter](env, "formatter")
			if err != nil {
				return nil, err
			}
			return forest.Hooks{}, forest.Provide(env, "format", service{formatter: r})
		}}
}
