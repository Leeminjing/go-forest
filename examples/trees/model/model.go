// Package model is a deterministic model provider for the domain example.
package model

import (
	"context"
	"go-forest/forest"
)

type Generator interface {
	Generate(context.Context, string) (string, error)
}
type service struct{}

func (service) Generate(_ context.Context, input string) (string, error) {
	return "model: " + input, nil
}
func Definition() forest.Definition {
	return forest.Definition{TypeID: "example.model", Version: "1", HotSwappable: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Generator]("generate", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{}, forest.Provide(env, "generate", service{})
		}}
}
