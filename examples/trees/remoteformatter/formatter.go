// Package remoteformatter owns its Clock abstraction. The provider may be local
// or remote; errors are part of the declared capability contract.
package remoteformatter

import (
	"context"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type Clock interface {
	Now(context.Context) (time.Time, error)
}
type Output interface {
	Format(context.Context) (string, error)
}
type formatter struct{ clock forest.Ref[Clock] }

func (f formatter) Format(ctx context.Context) (string, error) {
	var result string
	err := f.clock.Use(ctx, func(clock Clock) error {
		now, err := clock.Now(ctx)
		if err == nil {
			result = now.UTC().Format(time.RFC3339)
		}
		return err
	})
	return result, err
}
func Definition() forest.Definition {
	return forest.Definition{TypeID: "example.remote-formatter", Version: "1",
		Ports:        []forest.PortDescriptor{forest.Port[Clock]("clock", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Output]("format", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[Clock](env, "clock")
			if err != nil {
				return nil, err
			}
			return forest.Hooks{}, forest.Provide(env, "format", formatter{clock: ref})
		}}
}
