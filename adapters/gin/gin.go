// Package ginforest hosts unmodified Gin routers as ordinary Forest trees.
package ginforest

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
)

var ErrInvalidOptions = errors.New("invalid Gin tree options")

type Endpoint interface {
	URL() string
	ServeError() error
}

type Options struct {
	TypeID  string
	Version string
	Address string
	Ports   []forest.PortDescriptor
	// BuildRouter runs once per instance, only during preparation. Build Refs
	// here; configure all routes before returning. Do not retain env or ctx.
	BuildRouter func(context.Context, *forest.BuildEnv) (*gin.Engine, error)
}

func Definition(f *forest.Forest, options Options) (forest.Definition, error) {
	if f == nil || options.TypeID == "" || options.Version == "" || options.BuildRouter == nil {
		return forest.Definition{}, ErrInvalidOptions
	}
	if options.Address == "" {
		options.Address = "127.0.0.1:0"
	}
	if _, _, err := net.SplitHostPort(options.Address); err != nil {
		return forest.Definition{}, errors.Join(ErrInvalidOptions, err)
	}
	options.Ports = append([]forest.PortDescriptor(nil), options.Ports...)
	return forest.Definition{TypeID: options.TypeID, Version: options.Version, Ports: options.Ports,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Endpoint]("http", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			router, err := options.BuildRouter(ctx, env)
			if err != nil {
				return nil, err
			}
			if router == nil {
				return nil, ErrInvalidOptions
			}
			// Request lifetime is owned by the server, never the preparation Scope.
			lifetime, cancel := context.WithCancel(context.Background())
			s := &server{address: options.Address, cancel: cancel}
			s.http = &http.Server{Handler: router, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10,
				BaseContext: func(net.Listener) context.Context { return lifetime },
				// Never log recovered request contents or HTTP panic payloads here.
				ErrorLog: log.New(io.Discard, "", 0),
			}
			return s, forest.Provide(env, "http", s)
		},
	}, nil
}

// Scope pins a generation for a finite route chain. Install before business
// middleware. Do not install on SSE or other indefinite observation streams.
func Scope(f *forest.Forest) gin.HandlerFunc {
	return func(c *gin.Context) {
		if f == nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		err := f.Invoke(c.Request.Context(), func(ctx context.Context) error {
			c.Request = c.Request.WithContext(ctx)
			c.Next()
			return nil
		})
		if err != nil {
			if !c.Writer.Written() {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "unavailable"})
			} else {
				c.Abort()
			}
		}
	}
}

type server struct {
	address string
	http    *http.Server
	cancel  context.CancelFunc
	mu      sync.Mutex
	url     string
	done    chan struct{}
	err     error
	stopped bool
}

func (s *server) URL() string       { s.mu.Lock(); defer s.mu.Unlock(); return s.url }
func (s *server) ServeError() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.done != nil {
		return ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.url = "http://" + listener.Addr().String()
	s.done = make(chan struct{})
	go func() {
		err := s.http.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.done)
	}()
	return nil
}
func (s *server) Ready(context.Context) error { return s.ServeError() }
func (s *server) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	done := s.done
	s.mu.Unlock()
	s.cancel()
	if done == nil {
		return nil
	}
	if err := s.http.Shutdown(ctx); err != nil {
		return err
	}
	select {
	case <-done:
		return s.ServeError()
	case <-ctx.Done():
		return ctx.Err()
	}
}
