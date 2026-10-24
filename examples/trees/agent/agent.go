// Package agent is an ordinary tree. Forest knows nothing about agents.
package agent

import (
	"context"
	"github.com/Leeminjing/go-forest/forest"
)

type Model interface {
	Generate(context.Context, string) (string, error)
}
type Responder interface {
	Reply(context.Context, string) (string, error)
}
type service struct{ model forest.Ref[Model] }

func (s service) Reply(ctx context.Context, input string) (string, error) {
	var result string
	err := s.model.Use(ctx, func(m Model) error { var err error; result, err = m.Generate(ctx, input); return err })
	return "agent: " + result, err
}
func Definition() forest.Definition {
	return forest.Definition{TypeID: "example.agent", Version: "1", HotSwappable: true,
		Ports:        []forest.PortDescriptor{forest.Port[Model]("model", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Responder]("reply", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[Model](env, "model")
			if err != nil {
				return nil, err
			}
			return forest.Hooks{}, forest.Provide(env, "reply", service{model: ref})
		}}
}
