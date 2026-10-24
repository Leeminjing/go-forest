package forest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type node struct {
	id         TreeID
	parent     TreeID
	definition Definition
	instance   *instance
}

// graph is immutable after publication; reference counters use Forest.gate.
type graph struct {
	generation uint64
	nodes      map[TreeID]*node
	bindings   map[string]Binding
	nextOrder  uint64
	refs       int
}

type Forest struct {
	writer     sync.Mutex
	gate       sync.RWMutex
	current    atomic.Pointer[graph]
	closed     bool
	scopes     map[*invocation]struct{}
	registryMu sync.RWMutex
	registry   map[DefinitionRef]Definition
	serial     atomic.Uint64
	observe    sync.Mutex
	instances  map[string]*instance
	changed    chan struct{}
	events     eventLog
	options    Options
}

func New(options Options) *Forest {
	if options.DrainTimeout <= 0 {
		options.DrainTimeout = 5 * time.Second
	}
	if options.EventCapacity <= 0 {
		options.EventCapacity = 128
	}
	f := &Forest{registry: map[DefinitionRef]Definition{}, scopes: map[*invocation]struct{}{}, instances: map[string]*instance{}, changed: make(chan struct{}), options: options}
	f.events.init(options.EventCapacity)
	f.current.Store(&graph{nodes: map[TreeID]*node{}, bindings: map[string]Binding{}, refs: 1})
	return f
}

func (f *Forest) Register(d Definition) error {
	if err := validateDefinition(d); err != nil {
		return err
	}
	f.gate.RLock()
	defer f.gate.RUnlock()
	if f.closed {
		return ErrClosed
	}
	f.registryMu.Lock()
	defer f.registryMu.Unlock()
	if _, exists := f.registry[d.Ref()]; exists {
		return fmt.Errorf("%w: definition already registered: %v", ErrInvalidDefinition, d.Ref())
	}
	f.registry[d.Ref()] = cloneDefinition(d)
	return nil
}

func (f *Forest) definition(ref DefinitionRef) (Definition, error) {
	f.registryMu.RLock()
	defer f.registryMu.RUnlock()
	d, ok := f.registry[ref]
	if !ok {
		return Definition{}, fmt.Errorf("%w: definition %v", ErrNotFound, ref)
	}
	return cloneDefinition(d), nil
}

// Close stops admission, drains scopes and then releases resources. A deadline
// never frees an instance still used by a normal callback. Close may be retried.
func (f *Forest) Close(ctx context.Context) error {
	f.writer.Lock()
	f.gate.Lock()
	if !f.closed {
		f.closed = true
		f.retireOwnersLocked(f.current.Load(), nil)
		f.releaseGraphLocked(f.current.Load())
	}
	f.gate.Unlock()
	f.writer.Unlock()
	return f.Wait(ctx)
}

// Wait waits for all non-current instances to finish cleanup, including rollback.
func (f *Forest) Wait(ctx context.Context) error {
	for {
		f.observe.Lock()
		pending, failed := false, error(nil)
		for _, i := range f.instances {
			info := i.info()
			// Closed forests must also await owners held by draining dependants.
			if info.State == Draining || info.State == Preparing || info.State == Ready || info.State == Created {
				pending = true
			}
			if info.State == Draining && info.Error != nil {
				failed = info.Error
			}
		}
		ch := f.changed
		f.observe.Unlock()
		// Scopes may keep a removed instance Active until the last release.
		f.gate.RLock()
		if f.closed {
			f.observe.Lock()
			for _, i := range f.instances {
				if i.info().State == Active {
					pending = true
				}
			}
			f.observe.Unlock()
		}
		if f.closed && len(f.scopes) > 0 {
			pending = true
		}
		if !f.closed {
			active := f.current.Load()
			f.observe.Lock()
			for _, i := range f.instances {
				if i.info().State == Active {
					n := active.nodes[i.tree]
					if n == nil || n.instance != i {
						pending = true
					}
				}
			}
			f.observe.Unlock()
		}
		f.gate.RUnlock()
		if !pending {
			return nil
		}
		if failed != nil {
			return failed
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrDrainTimeout, ctx.Err())
		case <-ch:
		}
	}
}

func (f *Forest) notify() {
	f.observe.Lock()
	close(f.changed)
	f.changed = make(chan struct{})
	f.observe.Unlock()
}

// CancelGeneration requests cooperative cancellation of managed invocations.
// It does not end scopes or release their resources until callbacks return.
func (f *Forest) CancelGeneration(generation uint64) int {
	f.gate.RLock()
	defer f.gate.RUnlock()
	count := 0
	for s := range f.scopes {
		if s.graph.generation == generation {
			s.cancel()
			count++
		}
	}
	return count
}
