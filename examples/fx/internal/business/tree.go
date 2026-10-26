// Package business implicitly satisfies consumers without importing their types
// or depending on the Fx framework.
package business

import (
	"context"

	"github.com/Leeminjing/go-forest/forest"
)

type service struct{ version string }

func (s *service) Read(_ context.Context, text string) (string, error) {
	return s.version + ":" + text, nil
}
func Definition(version string) forest.Definition {
	return forest.Definition{TypeID: "fx-demo.business", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[*service]("read", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "read", &service{version: version})
	}}
}
