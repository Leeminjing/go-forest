package forest

import (
	"context"
	"reflect"
	"sync"
)

type scopeKey struct{ forest *Forest }
type invocation struct {
	forest    *Forest
	graph     *graph
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	users     int
	released  bool
	preparing bool
	phase     Phase
	finished  chan struct{}
}

func (s *invocation) acquire() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrScopeClosed
	}
	s.users++
	return nil
}
func (s *invocation) leave(closeScope bool) {
	s.mu.Lock()
	if closeScope {
		s.closed = true
	} else {
		s.users--
	}
	release := s.closed && s.users == 0 && !s.released
	if release {
		s.released = true
	}
	s.mu.Unlock()
	if closeScope {
		s.cancel()
	}
	if release {
		s.cancel()
		if !s.preparing {
			s.forest.gate.Lock()
			delete(s.forest.scopes, s)
			s.forest.releaseGraphLocked(s.graph)
			s.forest.gate.Unlock()
			s.forest.notify()
		}
		if s.finished != nil {
			close(s.finished)
		}
	}
}

// Invoke establishes one version for the complete callback and all nested
// Ref.Use calls. Pass callCtx onward; replacing it with Background starts a new
// invocation and intentionally loses the outer version and cancellation.
func (f *Forest) Invoke(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return ErrInvalidOperation
	}
	if existing, ok := ctx.Value(scopeKey{f}).(*invocation); ok {
		if err := existing.acquire(); err != nil {
			return err
		}
		defer existing.leave(false)
		return fn(ctx)
	}
	f.gate.Lock()
	if f.closed {
		f.gate.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		f.gate.Unlock()
		return err
	}
	g := f.current.Load()
	g.refs++
	callCtx, cancel := context.WithCancel(ctx)
	s := &invocation{forest: f, graph: g, cancel: cancel}
	f.scopes[s] = struct{}{}
	f.gate.Unlock()
	defer s.leave(true)
	return fn(context.WithValue(callCtx, scopeKey{f}, s))
}

func (f *Forest) withCandidate(ctx context.Context, g *graph, phase Phase, fn func(context.Context) error) error {
	callCtx, cancel := context.WithCancel(ctx)
	s := &invocation{forest: f, graph: g, cancel: cancel, preparing: true, phase: phase, finished: make(chan struct{})}
	// Preparation may join parallel, already-entered Ref callbacks. Cancel on
	// return, but do not proceed to rollback or publication until they release.
	defer func() { s.leave(true); <-s.finished }()
	return fn(context.WithValue(callCtx, scopeKey{f}, s))
}

// Generation returns the pinned version, if this context is an open invocation.
func (f *Forest) Generation(ctx context.Context) (uint64, error) {
	s, ok := ctx.Value(scopeKey{f}).(*invocation)
	if !ok {
		return 0, ErrScopeClosed
	}
	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.leave(false)
	return s.graph.generation, nil
}

// Ref can be copied and retained. Only its callback's raw capability is scoped.
type Ref[T any] struct {
	forest     *Forest
	consumer   TreeID
	instanceID string
	port       PortID
}

func Reference[T any](env *BuildEnv, port PortID) (Ref[T], error) {
	if env == nil {
		return Ref[T]{}, ErrInvalidDefinition
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.sealed {
		return Ref[T]{}, ErrScopeClosed
	}
	p, ok := portOf(env.node.definition, port)
	if !ok || p.Type != reflect.TypeFor[T]() {
		return Ref[T]{}, failure("reference", env.node.id, port, ErrContractMismatch)
	}
	return Ref[T]{forest: env.forest, consumer: env.node.id, instanceID: env.node.instance.id, port: port}, nil
}

func (r Ref[T]) Use(ctx context.Context, fn func(T) error) error {
	if fn == nil {
		return ErrInvalidOperation
	}
	return r.use(ctx, false, func(values []T) error { return fn(values[0]) })
}

// UseAll supplies providers in successful binding insertion order. Rebind keeps
// the original order. Optional unbound ports return ErrBindingUnavailable.
func (r Ref[T]) UseAll(ctx context.Context, fn func([]T) error) error {
	if fn == nil {
		return ErrInvalidOperation
	}
	return r.use(ctx, true, fn)
}

func (r Ref[T]) use(ctx context.Context, many bool, fn func([]T) error) error {
	if r.forest == nil || fn == nil {
		return ErrBindingUnavailable
	}
	if _, ok := ctx.Value(scopeKey{r.forest}).(*invocation); !ok {
		return ErrScopeClosed
	}
	return r.forest.Invoke(ctx, func(callCtx context.Context) error {
		s := callCtx.Value(scopeKey{r.forest}).(*invocation)
		n := s.graph.nodes[r.consumer]
		if n == nil || n.instance == nil || n.instance.id != r.instanceID {
			return ErrBindingUnavailable
		}
		p, ok := portOf(n.definition, r.port)
		if !ok || p.Many != many {
			return failure("resolve", r.consumer, r.port, ErrContractMismatch)
		}
		if s.preparing && (p.Phase == Runtime || (s.phase == Construction && p.Phase != Construction)) {
			return failure("resolve-phase", r.consumer, r.port, ErrBindingUnavailable)
		}
		var values []T
		for _, b := range s.graph.routes[routeKey{consumer: r.consumer, port: r.port}] {
			provider := s.graph.nodes[b.Provider]
			if provider == nil || provider.instance == nil {
				return ErrBindingUnavailable
			}
			value, ok := provider.instance.capabilities[b.Capability].(T)
			if !ok {
				return failure("resolve", r.consumer, r.port, ErrBindingUnavailable)
			}
			values = append(values, value)
		}
		if len(values) == 0 {
			return failure("resolve", r.consumer, r.port, ErrBindingUnavailable)
		}
		return fn(values)
	})
}

// Use is the application's public entry point into a tree. Tree-to-tree
// dependencies must use declared Refs, not this entry API as a service locator.
func Use[T any](ctx context.Context, f *Forest, tree TreeID, capability CapabilityID, fn func(T) error) error {
	if f == nil || fn == nil {
		return ErrInvalidOperation
	}
	if _, ok := ctx.Value(scopeKey{f}).(*invocation); !ok {
		return ErrScopeClosed
	}
	return f.Invoke(ctx, func(callCtx context.Context) error {
		s := callCtx.Value(scopeKey{f}).(*invocation)
		if s.preparing {
			return ErrBindingUnavailable
		}
		n := s.graph.nodes[tree]
		if n == nil {
			return ErrNotFound
		}
		c, ok := capOf(n.definition, capability)
		if !ok || !accessible(s.graph, "", tree, c, false) {
			return ErrMissingCapability
		}
		value, ok := n.instance.capabilities[capability].(T)
		if !ok {
			return ErrContractMismatch
		}
		return fn(value)
	})
}

func typeOf(value any) reflect.Type { return reflect.TypeOf(value) }
func nilValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}
