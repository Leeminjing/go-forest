// Package fxdemo composes a native Fx Module and ordinary Forest business trees.
package fxdemo

import (
	"context"
	"errors"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/examples/fx/internal/application"
	"github.com/Leeminjing/go-forest/examples/fx/internal/business"
	"github.com/Leeminjing/go-forest/forest"
)

type Options struct{ BetweenReads func(context.Context, string) }
type Runtime struct {
	Forest  *forest.Forest
	options Options
	address string
	jobs    chan job
	next    forest.DefinitionRef
}
type Response struct {
	Text       string `json:"text"`
	Generation uint64 `json:"generation"`
}

func New(ctx context.Context, o Options) (r *Runtime, err error) {
	r = &Runtime{Forest: forest.New(forest.Options{}), options: o, jobs: make(chan job)}
	f := r.Forest
	defer func() {
		if err != nil {
			c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			f.Close(c)
		}
	}()
	native, err := ff.Definition[application.Service](f, ff.Options{TypeID: "fx-demo.module", Version: "1", TreeID: "host/fx", Ports: []forest.PortDescriptor{forest.Port[application.Source]("source", forest.Required())}, BuildModule: r.module})
	if err != nil {
		return nil, err
	}
	owner := forest.Definition{TypeID: "fx-demo.host", Version: "1", ExportChildren: true, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	app, p1, p2 := application.Definition(), business.Definition("v1"), business.Definition("v2")
	r.next = p2.Ref()
	for _, d := range []forest.Definition{owner, native, app, p1, p2} {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	_, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{
		forest.AddRoot("host", owner.Ref()), forest.Nest("host", "host/fx", native.Ref()), forest.AddRoot("business", p1.Ref()), forest.AddRoot("application", app.Ref()),
		forest.Connect(forest.Link{ID: "source", Consumer: "host/fx", Port: "source", Provider: "business", Capability: "read"}), forest.Connect(forest.Link{ID: "fx", Consumer: "application", Port: "fx", Provider: "host/fx", Capability: "service"}),
	}})
	if err != nil {
		return nil, err
	}
	if err = ff.Enable(ctx, f, "host/fx"); err != nil {
		return nil, err
	}
	return r, nil
}
func (r *Runtime) Address() string { return r.address }
func (r *Runtime) Render(ctx context.Context, text string) (out string, err error) {
	err = r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[application.Service](c, r.Forest, "application", "render", func(s application.Service) error { var e error; out, e = s.Render(c, text); return e })
	})
	return
}
func (r *Runtime) Background(ctx context.Context, text string) (string, error) {
	work := job{ctx: ctx, text: text, reply: make(chan answer, 1)}
	select {
	case r.jobs <- work:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	select {
	case out := <-work.reply:
		return out.text, out.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
func (r *Runtime) ReplaceBusiness(ctx context.Context) error {
	_, err := r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("business", r.next, false)}})
	return err
}
func (r *Runtime) Close(ctx context.Context) error {
	if err := ff.Quiesce(ctx, r.Forest, "host/fx"); err != nil {
		if errors.Is(err, forest.ErrClosed) {
			return r.Forest.Close(ctx)
		}
		return err
	}
	return r.Forest.Close(ctx)
}
