// Package catalog owns a deterministic capability, independent of both agents.
package catalog

import (
	"context"
	"github.com/Leeminjing/go-forest/forest"
)

type Service struct {
	Version string
	Before  func(context.Context, string) error
}

func (s *Service) Lookup(ctx context.Context, key string) (string, error) {
	if s.Before != nil {
		if err := s.Before(ctx, key); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return s.Version + ":" + key, nil
}
func Definition(version string, before func(context.Context, string) error) forest.Definition {
	return forest.Definition{TypeID: "eino-example.catalog", Version: version, HotSwappable: true, PreserveChildren: true, ExportChildren: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Service]("lookup", forest.Public|forest.Descendants)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "lookup", &Service{Version: version, Before: before})
	}}
}
