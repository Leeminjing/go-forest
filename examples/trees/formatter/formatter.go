// Package formatter owns the Clock abstraction it consumes.
package formatter

import (
	"context"
	"time"

	"go-forest/forest"
)

type Clock interface {
	Now(context.Context) time.Time
}
type Output interface {
	Format(context.Context) (string, error)
}
type service struct{ clock forest.Ref[Clock] }

func (s service) Format(ctx context.Context) (string, error) {
	var result string
	err := s.clock.Use(ctx, func(c Clock) error { result = c.Now(ctx).UTC().Format(time.RFC3339); return nil })
	return result, err
}
func Definition() forest.Definition {
	return forest.Definition{TypeID: "example.formatter", Version: "1", HotSwappable: true, Reparentable: true, PreserveChildren: true, ExportChildren: true,
		Ports:        []forest.PortDescriptor{forest.Port[Clock]("clock", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Output]("format", forest.Public|forest.Descendants)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[Clock](env, "clock")
			if err != nil {
				return nil, err
			}
			return forest.Hooks{}, forest.Provide(env, "format", service{clock: ref})
		}}
}
