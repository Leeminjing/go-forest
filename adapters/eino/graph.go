package einoforest

import (
	"context"
	"errors"

	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type Graph[I, O any] interface {
	Invoke(context.Context, I, GraphCall) (O, error)
	Stream(context.Context, I, GraphCall, func(*schema.StreamReader[O]) error) error
	Status() Status
}
type GraphCall struct {
	CheckpointID string
	Options      []compose.Option
}
type GraphSession[I, O any] struct {
	Runnable compose.Runnable[I, O]
	Lifetime Lifetime
}
type GraphHost[I, O any] struct {
	Lifecycle  forest.Lifecycle
	NewSession func(context.Context) (GraphSession[I, O], error)
	// True whenever the native graph is compiled with a checkpoint store.
	// Native checkpoint identity/write options are reserved by this adapter.
	Checkpoints bool
}
type GraphOptions[I, O any] struct {
	Config Config
	Build  func(context.Context, *forest.BuildEnv) (*GraphHost[I, O], error)
}

func GraphDefinition[I, O any](o GraphOptions[I, O]) (forest.Definition, error) {
	if !o.Config.valid() || o.Build == nil {
		return forest.Definition{}, ErrInvalid
	}
	o.Config.Ports = append([]forest.PortDescriptor(nil), o.Config.Ports...)
	return forest.Definition{TypeID: o.Config.TypeID, Version: o.Config.Version, HotSwappable: o.Config.HotSwappable, Ports: o.Config.Ports, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Graph[I, O]]("graph", forest.Public)}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		host, err := o.Build(ctx, env)
		if host == nil {
			return nil, errors.Join(ErrUnmanaged, err)
		}
		r := &graph[I, O]{runtime: runtime{config: o.Config}, host: host}
		if err != nil {
			return r, err
		}
		if host.NewSession == nil || host.Lifecycle == nil {
			return r, ErrUnmanaged
		}
		return r, forest.Provide(env, "graph", r)
	}}, nil
}

type graph[I, O any] struct {
	runtime
	host *GraphHost[I, O]
}

func (g *graph[I, O]) Start(ctx context.Context) error { return g.host.Lifecycle.Start(ctx) }
func (g *graph[I, O]) Ready(ctx context.Context) error {
	if r, ok := g.host.Lifecycle.(forest.Readiness); ok {
		return r.Ready(ctx)
	}
	return nil
}
func (g *graph[I, O]) Stop(ctx context.Context) error {
	return g.runtime.stop(ctx, g.host.Lifecycle)
}
func (g *graph[I, O]) execute(ctx context.Context, c GraphCall, fn func(context.Context, compose.Runnable[I, O], []compose.Option) error) error {
	if g.host.Checkpoints {
		if !validKey(c.CheckpointID) {
			return ErrInvalid
		}
	} else if c.CheckpointID != "" {
		return ErrInvalid
	}
	return g.run(ctx, c.CheckpointID, func(ctx context.Context) (err error) {
		session, err := g.host.NewSession(ctx)
		if session.Lifetime != nil {
			defer func() { err = errors.Join(err, g.join(session.Lifetime)) }()
		}
		if err != nil {
			return err
		}
		if session.Runnable == nil || session.Lifetime == nil {
			return ErrUnmanaged
		}
		opts := append([]compose.Option(nil), c.Options...)
		if g.host.Checkpoints {
			opts = append(opts, compose.WithCheckPointID(c.CheckpointID), compose.WithWriteToCheckPointID(c.CheckpointID))
		}
		return fn(ctx, session.Runnable, opts)
	})
}
func (g *graph[I, O]) Invoke(ctx context.Context, input I, c GraphCall) (out O, err error) {
	err = g.execute(ctx, c, func(ctx context.Context, r compose.Runnable[I, O], opts []compose.Option) error {
		var err error
		out, err = r.Invoke(ctx, input, opts...)
		return err
	})
	return
}

// The callback has a single reader and must not Close, Copy or retain it.
func (g *graph[I, O]) Stream(ctx context.Context, input I, c GraphCall, visit func(*schema.StreamReader[O]) error) error {
	if visit == nil {
		return ErrInvalid
	}
	return g.execute(ctx, c, func(ctx context.Context, r compose.Runnable[I, O], opts []compose.Option) error {
		reader, err := r.Stream(ctx, input, opts...)
		if reader != nil {
			defer reader.Close()
		}
		if err != nil {
			return err
		}
		if reader == nil {
			return ErrInvalid
		}
		return visit(reader)
	})
}
