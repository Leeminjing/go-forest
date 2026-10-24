// Package clock provides time without importing any consumer's interfaces.
package clock

import (
	"context"
	"time"

	"go-forest/forest"
)

// Source is this provider's promise; consumers define their own interfaces.
type Source interface {
	Now(context.Context) time.Time
}
type service struct{ now func() time.Time }

func (s service) Now(context.Context) time.Time { return s.now() }

func Definition(version string, now func() time.Time) forest.Definition {
	return forest.Definition{TypeID: "example.clock", Version: version, HotSwappable: true, PreserveChildren: true, Reparentable: true, ExportChildren: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Source]("time", forest.Public|forest.Descendants)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{}, forest.Provide(env, "time", service{now: now})
		}}
}
