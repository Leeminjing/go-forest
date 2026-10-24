// Package printer depends on a formatting contract, not its implementation.
package printer

import (
	"context"
	"go-forest/forest"
)

type Formatter interface {
	Format(context.Context) (string, error)
}
type Output interface {
	Print(context.Context) (string, error)
}
type service struct{ formatter forest.Ref[Formatter] }

func (s service) Print(ctx context.Context) (string, error) {
	var result string
	err := s.formatter.Use(ctx, func(f Formatter) error { var err error; result, err = f.Format(ctx); return err })
	return "forest: " + result, err
}
func Definition() forest.Definition {
	return forest.Definition{TypeID: "example.printer", Version: "1", HotSwappable: true, Reparentable: true,
		Ports:        []forest.PortDescriptor{forest.Port[Formatter]("formatter", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Output]("print", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[Formatter](env, "formatter")
			if err != nil {
				return nil, err
			}
			return forest.Hooks{}, forest.Provide(env, "print", service{formatter: ref})
		}}
}
