// Package web adapts HTTP requests into Forest invocation scopes.
package web

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type Agent interface {
	Reply(context.Context, string) (string, error)
}
type Endpoint interface{ URL() string }
type endpoint struct{ url string }

func (e endpoint) URL() string { return e.url }

// Definition accepts Forest as an application entry adapter. Its business
// dependency uses a declared Ref rather than an agent implementation.
func Definition(f *forest.Forest) forest.Definition {
	return forest.Definition{TypeID: "example.web", Version: "1",
		Ports:        []forest.PortDescriptor{forest.Port[Agent]("agent", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Endpoint]("http", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[Agent](env, "agent")
			if err != nil {
				return nil, err
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			server := &http.Server{ReadHeaderTimeout: time.Second}
			server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var result string
				err := f.Invoke(r.Context(), func(callCtx context.Context) error {
					return ref.Use(callCtx, func(a Agent) error { var err error; result, err = a.Reply(callCtx, r.URL.Query().Get("q")); return err })
				})
				if err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte(result))
			})
			serveResult := make(chan error, 1)
			started := false
			hooks := forest.Hooks{OnStart: func(context.Context) error {
				started = true
				go func() { serveResult <- server.Serve(listener) }()
				return nil
			}, OnStop: func(ctx context.Context) error {
				if !started {
					return listener.Close()
				}
				if err := server.Shutdown(ctx); err != nil {
					return err
				}
				if err := <-serveResult; !errors.Is(err, http.ErrServerClosed) {
					return err
				}
				return nil
			}}
			if err := forest.Provide(env, "http", endpoint{url: "http://" + listener.Addr().String()}); err != nil {
				return hooks, err
			}
			return hooks, nil
		}}
}
