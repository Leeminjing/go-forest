// Package business contains no transport or observation dependencies.
package business

import (
	"context"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
)

type Service struct {
	Label   string
	Entered chan struct{}
	Release <-chan struct{}
	once    sync.Once
}

func (s *Service) Work(ctx context.Context, hold bool) (string, error) {
	if hold && s.Release != nil {
		s.once.Do(func() { close(s.Entered) })
		select {
		case <-s.Release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.Label, nil
}

type Output interface {
	Work(context.Context, bool) (string, error)
}

func Definition(version string, service *Service) forest.Definition {
	return forest.Definition{TypeID: "demo.business", Version: version, HotSwappable: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Output]("work", forest.Public, forest.Contract{ID: "demo.work", Version: 1})},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{}, forest.Provide(env, "work", service)
		},
	}
}
