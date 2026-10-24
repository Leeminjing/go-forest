package forest

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type instance struct {
	id           string
	tree         TreeID
	definition   DefinitionRef
	object       Lifecycle
	capabilities map[CapabilityID]any // sealed when Build returns
	refs         int                  // Forest.gate
	dependencies []*instance          // construction/start resources, then drain ownership
	mu           sync.Mutex
	state        State
	err          error
	done         chan struct{}
}

func (i *instance) info() InstanceInfo {
	i.mu.Lock()
	defer i.mu.Unlock()
	return InstanceInfo{ID: i.id, TreeID: i.tree, Definition: i.definition, State: i.state, Error: i.err}
}
func (f *Forest) setState(i *instance, state State, err error) {
	i.mu.Lock()
	i.state, i.err = state, err
	i.mu.Unlock()
	f.notify()
}

// Each published graph owns one reference to every instance it contains.
func (f *Forest) releaseGraphLocked(g *graph) {
	g.refs--
	if g.refs != 0 {
		return
	}
	var zero []*instance
	for _, n := range g.nodes {
		n.instance.refs--
		if n.instance.refs == 0 {
			zero = append(zero, n.instance)
		}
	}
	// A draining child retains its old owner until Stop actually returns. This
	// also protects parents when Stop ignores its deadline.
	for _, i := range zero {
		n := g.nodes[i.tree]
		if parent := g.nodes[n.parent]; parent != nil {
			i.dependencies = append(i.dependencies, parent.instance)
			parent.instance.refs++
		}
	}
	for _, i := range zero {
		if i.refs == 0 {
			f.drainLocked(i)
		}
	}
}

func (f *Forest) drainLocked(i *instance) {
	f.setState(i, Draining, nil)
	go func() {
		err := f.stop(i)
		if err != nil {
			// Failed Stop has not demonstrated resource release. Keep dependencies
			// retained, report the failure, and leave the instance inspectable.
			f.setState(i, Draining, err)
			return
		}
		f.setState(i, Stopped, nil)
		close(i.done)
		f.gate.Lock()
		for _, dep := range i.dependencies {
			dep.refs--
			if dep.refs == 0 {
				f.drainLocked(dep)
			}
		}
		f.gate.Unlock()
	}()
}

// A timeout is observable even if a tree ignores context cancellation; the
// eventual Stop result is still observed before releasing dependency resources.
func (f *Forest) stop(i *instance) error {
	if i.object == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), f.options.DrainTimeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- safeLifecycle(func() error { return i.object.Stop(ctx) }) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		f.setState(i, Draining, failure("stop", i.tree, "", errors.Join(ErrDrainTimeout, ctx.Err())))
		return <-result
	}
}

func safeLifecycle(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("tree callback panicked: %v", p)
		}
	}()
	return fn()
}

// BuildEnv is valid only during Factory execution. After construction, retain
// typed Refs rather than the environment or its provided raw objects.
type BuildEnv struct {
	forest *Forest
	node   *node
	mu     sync.Mutex
	sealed bool
}

func Provide(env *BuildEnv, id CapabilityID, value any) error {
	if env == nil {
		return ErrInvalidDefinition
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.sealed {
		return ErrScopeClosed
	}
	c, ok := capOf(env.node.definition, id)
	if !ok {
		return failure("provide", env.node.id, "", ErrMissingCapability)
	}
	if nilValue(value) || !compatible(typeOf(value), c.Type) {
		return failure("provide", env.node.id, "", ErrContractMismatch)
	}
	if _, exists := env.node.instance.capabilities[id]; exists {
		return failure("provide", env.node.id, "", ErrInvalidDefinition)
	}
	env.node.instance.capabilities[id] = value
	return nil
}
