package temporalforest

import (
	"context"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/client"
)

// ClientAccess borrows native objects only for the duration of the callback.
// Retain workflow identities/plain values, never native clients, runs or iterators.
type ClientAccess interface {
	Use(context.Context, func(client.Client) error) error
}
type ClientOptions struct {
	TypeID, Version string
	Native          client.Options
	ConnectTimeout  time.Duration
}

func ClientDefinition(f *forest.Forest, o ClientOptions) (forest.Definition, error) {
	if f == nil || !valid(o.TypeID) || !valid(o.Version) || o.ConnectTimeout < 0 {
		return forest.Definition{}, ErrInvalid
	}
	if !supportedClient(o.Native) {
		return forest.Definition{}, ErrUnsupported
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 5 * time.Second
	}
	return forest.Definition{TypeID: o.TypeID, Version: o.Version, Capabilities: []forest.CapabilityDescriptor{forest.Capability[ClientAccess]("client", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		c := &clientTree{f: f, options: o}
		return c, forest.Provide(e, "client", c)
	}}, nil
}

type clientTree struct {
	f       *forest.Forest
	options ClientOptions
	mu      sync.RWMutex
	native  client.Client
	closed  bool
}

func (c *clientTree) Start(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	defer cancel()
	o := c.options.Native
	if o.Logger == nil {
		o.Logger = diagnosticLog{}
	}
	c.native, err = client.DialContext(ctx, o)
	if err != nil {
		return err
	}
	_, err = c.native.CheckHealth(ctx, &client.CheckHealthRequest{})
	return err
}
func (c *clientTree) Use(ctx context.Context, fn func(client.Client) error) error {
	if fn == nil {
		return ErrInvalid
	}
	if _, err := c.f.Generation(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed || c.native == nil {
		return ErrUnavailable
	}
	return fn(c.native)
}
func (c *clientTree) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		if c.native != nil {
			c.native.Close()
		}
	}
	return nil
}

// Default SDK logging retains only aggregate error counts, never payloads.
// An explicitly provided native Logger follows the application's own policy.
type diagnosticLog struct{ record func() }

func (d diagnosticLog) Debug(string, ...any) {}
func (d diagnosticLog) Info(string, ...any)  {}
func (d diagnosticLog) Warn(string, ...any) {
	if d.record != nil {
		d.record()
	}
}
func (d diagnosticLog) Error(string, ...any) {
	if d.record != nil {
		d.record()
	}
}
