package fxdemo

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/examples/fx/internal/application"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

type nativeService struct {
	source  forest.Ref[application.Source]
	between func(context.Context, string)
}

func (s *nativeService) Render(ctx context.Context, text string) (string, error) {
	var first, second string
	if err := s.source.Use(ctx, func(p application.Source) error { var err error; first, err = p.Read(ctx, text); return err }); err != nil {
		return "", err
	}
	if s.between != nil {
		s.between(ctx, text)
	}
	err := s.source.Use(ctx, func(p application.Source) error { var err error; second, err = p.Read(ctx, text); return err })
	return first + "/" + second, err
}

type job struct {
	ctx   context.Context
	text  string
	reply chan answer
}
type answer struct {
	text string
	err  error
}

func (r *Runtime) module(env *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
	ref, err := forest.Reference[application.Source](env, "source")
	if err != nil {
		return nil, err
	}
	return fx.Module("existing-renderer",
		fx.Provide(func() application.Service { return &nativeService{source: ref, between: r.options.BetweenReads} }),
		fx.Invoke(func(l fx.Lifecycle, s application.Service) {
			var ticket *ff.Ticket
			l.Append(fx.Hook{OnStart: func(context.Context) error {
				ln, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					return e
				}
				server := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: b.Handler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					text, e := s.Render(req.Context(), req.URL.Query().Get("text"))
					if e != nil {
						http.Error(w, "render failed", 500)
						return
					}
					g, e := r.Forest.Generation(req.Context())
					if e != nil {
						http.Error(w, "scope closed", 500)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(Response{Text: text, Generation: g})
				}))}
				ticket, e = b.Defer(func(context.Context) error { return server.Close() })
				if e != nil {
					ln.Close()
					return e
				}
				done := make(chan struct{})
				if e = b.OwnIngress(func() error { return ticket.Close(context.Background()) }, done); e != nil {
					ln.Close()
					return e
				}
				r.address = "http://" + ln.Addr().String()
				go func() { defer close(done); server.Serve(ln) }()
				return b.Loop(func(c context.Context) error {
					for {
						select {
						case <-c.Done():
							return nil
						case work := <-r.jobs:
							out := ""
							e := b.Run(work.ctx, func(call context.Context) error { var err error; out, err = s.Render(call, work.text); return err })
							work.reply <- answer{text: out, err: e}
						}
					}
				})
			}, OnStop: func(c context.Context) error {
				if ticket != nil {
					return ticket.Close(c)
				}
				return nil
			}})
		}),
	), nil
}
