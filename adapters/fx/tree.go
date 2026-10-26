// Package fxforest composes a native Fx application as one Forest tree.
// Modules remain responsible for explicit resource ownership and gated ingress.
package fxforest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

var (
	ErrInvalid      = errors.New("invalid Fx tree configuration")
	ErrUnavailable  = errors.New("Fx business admission closed")
	ErrNestedScope  = errors.New("Fx management or ingress requires a fresh context")
	ErrPanic        = errors.New("Fx callback panicked")
	ErrDrainTimeout = errors.New("Fx drain incomplete")
	ErrOwnership    = errors.New("Fx resource release not proven")
	ErrRuntime      = errors.New("Fx background loop failed")
)

type Failure struct {
	Tree            forest.TreeID
	Identity, Stage string
	Err             error
}

func (e *Failure) Error() string {
	return "Fx tree " + string(e.Tree) + " " + e.Stage + ": " + e.Err.Error()
}
func (e *Failure) Unwrap() error { return e.Err }

type Options struct {
	TypeID, Version           string
	TreeID                    forest.TreeID
	Ports                     []forest.PortDescriptor
	StartTimeout, StopTimeout time.Duration
	BuildModule               func(*forest.BuildEnv, *Bridge) (fx.Option, error)
}

// Access borrows the exported service until its callback and registered tasks exit.
// Propagate callCtx to native methods and dynamic Forest dependencies.
type Access[T any] interface {
	Use(context.Context, func(callCtx context.Context, service T) error) error
}
type Status struct {
	Identity, Phase, ErrorStage                         string
	Accepting, OwnershipFailed, RuntimeFailed           bool
	Inflight, Loops, Tasks, HookRunning, CleanupPending int
}
type Control interface{ Status() Status }
type control struct{ b *Bridge }

func (c *control) Status() Status { return c.b.status() }

func valid(s string) bool {
	return s != "" && len(s) <= 256 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func safe(fn func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPanic
		}
	}()
	return fn()
}
func nilValue(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return r.IsNil()
	}
	return false
}

func Definition[T any](f *forest.Forest, o Options) (forest.Definition, error) {
	if f == nil || !valid(o.TypeID) || !valid(o.Version) || !valid(string(o.TreeID)) || o.BuildModule == nil || o.StartTimeout < 0 || o.StopTimeout < 0 {
		return forest.Definition{}, ErrInvalid
	}
	if o.StartTimeout == 0 {
		o.StartTimeout = 15 * time.Second
	}
	if o.StopTimeout == 0 {
		o.StopTimeout = 15 * time.Second
	}
	o.Ports = append([]forest.PortDescriptor(nil), o.Ports...)
	return forest.Definition{TypeID: o.TypeID, Version: o.Version, Ports: o.Ports, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Access[T]]("service", forest.Public), forest.Capability[Control]("control", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}
		b := &Bridge{f: f, options: o, calls: make(map[*callState]context.CancelFunc), quiesced: make(chan struct{}), enabled: make(chan struct{}), statusValue: Status{Identity: hex.EncodeToString(id), Phase: "building"}}
		b.cond = sync.NewCond(&b.mu)
		b.root, b.cancel = context.WithCancel(context.Background())
		b.control = &control{b: b}
		t := &tree[T]{b: b}
		err := safe(func() error {
			module, err := o.BuildModule(env, b)
			if err != nil {
				return err
			}
			if module == nil {
				return ErrInvalid
			}
			t.app = fx.New(module, fx.NopLogger, fx.RecoverFromPanics(), fx.Decorate(func(l fx.Lifecycle) fx.Lifecycle { return &hooks{b: b, native: l} }), fx.Populate(&t.exported))
			if err = t.app.Err(); err != nil {
				return err
			}
			if nilValue(t.exported) {
				return ErrInvalid
			}
			return nil
		})
		if err != nil {
			return t, b.failure("build", err)
		}
		if err = forest.Provide(env, "control", b.control); err == nil {
			err = forest.Provide(env, "service", &access[T]{b: b, value: t.exported})
		}
		return t, err
	}}, nil
}

type tree[T any] struct {
	b        *Bridge
	app      *fx.App
	exported T
}

func (t *tree[T]) Start(ctx context.Context) error {
	b := t.b
	b.mu.Lock()
	b.statusValue.Phase = "preparing"
	b.startCtx, b.startCancel = context.WithTimeout(ctx, b.options.StartTimeout)
	b.mu.Unlock()
	defer b.startCancel()
	// Fx's outer withTimeout must not abandon its lifecycle goroutine. Hooks
	// receive the cooperative context through the decorated public Lifecycle.
	err := t.app.Start(context.Background())
	if err == nil {
		err = b.startCtx.Err()
	}
	if err != nil {
		return b.failure("start", err)
	}
	b.mu.Lock()
	b.statusValue.Phase = "prepared-disabled"
	b.mu.Unlock()
	return nil
}
func (t *tree[T]) Stop(ctx context.Context) error {
	b := t.b
	defer func() {
		b.mu.Lock()
		cancel := b.cleanupCancel
		b.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}()
	b.seal()
	<-b.quiesced
	b.mu.Lock()
	b.statusValue.Phase = "stopping"
	prior := b.cleanupErr
	b.mu.Unlock()
	if prior != nil {
		return b.ownership(prior)
	}
	if t.app != nil {
		b.cleanupContext(ctx)
		if err := t.app.Stop(context.Background()); err != nil {
			return b.ownership(err)
		}
	}
	b.mu.Lock()
	prior = b.cleanupErr
	b.mu.Unlock()
	if prior != nil {
		return b.ownership(prior)
	}
	if err := b.closeResources(ctx); err != nil {
		return b.ownership(err)
	}
	b.mu.Lock()
	b.statusValue.Phase = "stopped"
	b.mu.Unlock()
	return nil
}

type access[T any] struct {
	b     *Bridge
	value T
}

func (a *access[T]) Use(ctx context.Context, fn func(context.Context, T) error) error {
	if fn == nil {
		return ErrInvalid
	}
	return a.b.use(ctx, func(c context.Context) error { return fn(c, a.value) })
}
