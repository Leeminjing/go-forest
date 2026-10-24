// Package localtime implements a clock without importing any consumer or bridge.
package localtime

import (
	"context"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type Source interface {
	Now(context.Context) (time.Time, error)
}
type clock struct{}

func (clock) Now(context.Context) (time.Time, error) { return time.Now(), nil }
func Definition() forest.Definition {
	return forest.Definition{TypeID: "example.local-time", Version: "1", HotSwappable: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Source]("clock", forest.Public, forest.Contract{ID: "example.clock.time", Version: 1})},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{}, forest.Provide(env, "clock", clock{})
		}}
}
