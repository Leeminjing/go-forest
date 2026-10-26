package fxmodule

import (
	"context"
	"os"
	"sync"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/activitybridge"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/trace"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

type business interface {
	Observe(context.Context) (string, error)
	Apply(context.Context, protocol.Request) (protocol.Result, error)
	Inspect(context.Context, protocol.Request) (protocol.Observation, error)
}
type Resource struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	closes int
}

func (r *Resource) open(directory string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(directory, "native-fx-")
	if err == nil {
		r.file = f
		r.path = f.Name()
	}
	return err
}
func (r *Resource) Check() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return ff.ErrUnavailable
	}
	_, err := r.file.Stat()
	return err
}
func (r *Resource) Close(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return nil
	}
	if err := r.file.Close(); err != nil {
		return err
	}
	r.closes++
	return os.Remove(r.path)
}
func (r *Resource) Closes() int { r.mu.Lock(); defer r.mu.Unlock(); return r.closes }

type Options struct {
	Process, Directory string
	Resource           *Resource
	BetweenReads       func(context.Context, protocol.Request) error
	Child              func(context.Context) error
}
type service struct {
	f       *forest.Forest
	source  forest.Ref[business]
	options Options
	native  *Resource
}

func Definition(f *forest.Forest, o Options) (forest.Definition, error) {
	return ff.Definition[activitybridge.Processor](f, ff.Options{TypeID: "cross.fx", Version: "1", TreeID: "app/fx", Ports: []forest.PortDescriptor{forest.Port[business]("business", forest.Required())}, BuildModule: func(env *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		ref, err := forest.Reference[business](env, "business")
		if err != nil {
			return nil, err
		}
		return fx.Module("native-command-module", fx.Provide(func() *Resource { return o.Resource }), fx.Provide(func(native *Resource) activitybridge.Processor {
			return &service{f: f, source: ref, options: o, native: native}
		}), fx.Invoke(func(l fx.Lifecycle, native *Resource) {
			var ticket *ff.Ticket
			l.Append(fx.Hook{OnStart: func(context.Context) error {
				if err := native.open(o.Directory); err != nil {
					return err
				}
				var e error
				ticket, e = b.Defer(native.Close)
				if e != nil {
					native.Close(context.Background())
				}
				return e
			}, OnStop: func(c context.Context) error {
				if ticket != nil {
					return ticket.Close(c)
				}
				return nil
			}})
		})), nil
	}})
}
func (s *service) Process(ctx context.Context, in protocol.Request) (out protocol.Result, err error) {
	if err = s.native.Check(); err != nil {
		return
	}
	g, err := s.f.Generation(ctx)
	if err != nil {
		return
	}
	trace.Add(ctx, protocol.Point{Stage: "fx", Process: s.options.Process, Generation: g})
	var before string
	err = s.source.Use(ctx, func(b business) error { var e error; before, e = b.Observe(ctx); return e })
	if err != nil {
		return
	}
	if s.options.BetweenReads != nil {
		if err = s.options.BetweenReads(ctx, in); err != nil {
			return
		}
	}
	err = s.source.Use(ctx, func(b business) error { var e error; out, e = b.Apply(ctx, in); return e })
	out.VersionBefore = before
	if err == nil && s.options.Child != nil {
		err = ff.Go(ctx, s.options.Child)
	}
	if e := s.native.Check(); err == nil {
		err = e
	}
	return
}
func (s *service) Inspect(ctx context.Context, in protocol.Request) (out protocol.Observation, err error) {
	err = s.source.Use(ctx, func(b business) error { var e error; out, e = b.Inspect(ctx, in); return e })
	return
}
