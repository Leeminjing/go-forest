package forest

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// Apply prepares a complete candidate without blocking business calls. All
// writers serialize, and admission is locked only for the final pointer swap.
func (f *Forest) Apply(ctx context.Context, plan ChangePlan) (generation uint64, err error) {
	f.writer.Lock()
	defer f.writer.Unlock()
	old := f.current.Load()
	generation = old.generation
	defer func() { f.events.publish(generation, plan.Operations, err) }()
	f.gate.RLock()
	closed := f.closed
	f.gate.RUnlock()
	if closed {
		return generation, ErrClosed
	}
	if plan.BaseGeneration != generation {
		return generation, &GenerationConflict{Expected: plan.BaseGeneration, Actual: generation}
	}
	if err = ctx.Err(); err != nil {
		return generation, err
	}
	if len(plan.Operations) == 0 {
		return generation, nil
	}
	candidate := cloneGraph(old)
	for _, op := range plan.Operations {
		if err = f.applyOperation(candidate, op); err != nil {
			return generation, failure("plan", op.Tree, op.Binding.Port, err)
		}
	}
	if err = validateGraph(candidate, false); err != nil {
		return generation, err
	}
	if err = validateRetained(old, candidate, plan.Operations); err != nil {
		return generation, err
	}
	if err = validateResourceOrder(candidate); err != nil {
		return generation, err
	}
	buildOrder, err := lifecycleOrder(candidate, Construction)
	if err != nil {
		return generation, err
	}
	startOrder, err := lifecycleOrder(candidate, Startup)
	if err != nil {
		return generation, err
	}
	var created []*instance
	committed := false
	defer func() {
		if !committed {
			// Keep failed/prepared instances observable, and clean up asynchronously
			// under the same resource dependency rules as committed instances.
			f.rollback(candidate, created)
		}
	}()
	for _, id := range buildOrder {
		n := candidate.nodes[id]
		if n.instance != nil {
			continue
		}
		i := &instance{id: fmt.Sprintf("instance-%d", f.serial.Add(1)), tree: id, definition: n.definition.Ref(), state: Created, capabilities: map[CapabilityID]any{}, done: make(chan struct{})}
		n.instance = i
		created = append(created, i)
		f.observe.Lock()
		f.instances[i.id] = i
		f.observe.Unlock()
		f.setState(i, Preparing, nil)
		env := &BuildEnv{forest: f, node: n}
		err = f.withCandidate(ctx, candidate, id, Construction, func(callCtx context.Context) error {
			return safeLifecycle(func() error {
				var buildErr error
				i.object, buildErr = n.definition.Build(callCtx, env)
				return buildErr
			})
		})
		env.mu.Lock()
		env.sealed = true
		env.mu.Unlock()
		if err == nil && nilValue(i.object) {
			err = ErrInvalidDefinition
		}
		if err == nil {
			for _, c := range n.definition.Capabilities {
				if _, ok := i.capabilities[c.ID]; !ok {
					err = fmt.Errorf("%w: %s", ErrMissingCapability, c.ID)
					break
				}
			}
		}
		if err != nil {
			return generation, failure("build", id, "", errors.Join(ErrPrepareFailed, err))
		}
	}
	if err = validateGraph(candidate, true); err != nil {
		return generation, err
	}
	if err = validateReplacementContracts(old, candidate); err != nil {
		return generation, err
	}
	for _, id := range startOrder {
		n := candidate.nodes[id]
		if previous := old.nodes[id]; previous != nil && previous.instance == n.instance {
			continue
		}
		err = f.withCandidate(ctx, candidate, id, Startup, func(callCtx context.Context) error {
			if err := safeLifecycle(func() error { return n.instance.object.Start(callCtx) }); err != nil {
				return failure("start", id, "", err)
			}
			if ready, ok := n.instance.object.(Readiness); ok {
				return safeLifecycle(func() error { return ready.Ready(callCtx) })
			}
			return nil
		})
		if err != nil {
			return generation, failure("readiness", id, "", errors.Join(ErrPrepareFailed, err))
		}
		f.setState(n.instance, Ready, nil)
	}
	if err = ctx.Err(); err != nil {
		return generation, err
	}
	f.gate.Lock()
	candidate.generation, candidate.refs = generation+1, 1
	for _, n := range candidate.nodes {
		n.instance.refs++
	}
	for _, i := range created {
		for _, dep := range resourceDependencies(candidate, i.tree) {
			i.dependencies = append(i.dependencies, dep)
			dep.refs++
		}
		f.setState(i, Active, nil)
	}
	f.current.Store(candidate)
	for id, before := range old.nodes {
		if after := candidate.nodes[id]; after == nil || after.instance != before.instance {
			f.setState(before.instance, Draining, nil)
		}
	}
	f.releaseGraphLocked(old)
	f.gate.Unlock()
	committed = true
	return candidate.generation, nil
}

func cloneGraph(old *graph) *graph {
	g := &graph{generation: old.generation, nodes: map[TreeID]*node{}, bindings: map[string]Binding{}, nextOrder: old.nextOrder}
	for id, n := range old.nodes {
		copy := *n
		g.nodes[id] = &copy
	}
	for id, b := range old.bindings {
		g.bindings[id] = b
	}
	return g
}

func (f *Forest) applyOperation(g *graph, op Operation) error {
	switch op.Kind {
	case OpAddRoot, OpNest:
		if op.Tree == "" || g.nodes[op.Tree] != nil || (op.Kind == OpNest && op.Parent == "") {
			return ErrInvalidOperation
		}
		d, err := f.definition(op.Definition)
		if err != nil {
			return err
		}
		g.nodes[op.Tree] = &node{id: op.Tree, parent: op.Parent, definition: d}
	case OpMove, OpUnnest:
		n := g.nodes[op.Tree]
		if n == nil {
			return ErrNotFound
		}
		if op.Kind == OpMove && op.Parent == "" {
			return ErrInvalidOperation
		}
		n.parent = op.Parent
	case OpReplace:
		n := g.nodes[op.Tree]
		if n == nil {
			return ErrNotFound
		}
		d, err := f.definition(op.Definition)
		if err != nil {
			return err
		}
		if !n.definition.HotSwappable || !d.HotSwappable {
			return ErrUnsupportedHotReplace
		}
		n.definition, n.instance = d, nil
	case OpRemove, OpRemoveSubtree:
		if g.nodes[op.Tree] == nil {
			return ErrNotFound
		}
		removed := map[TreeID]bool{op.Tree: true}
		if op.Kind == OpRemoveSubtree {
			for id := range g.nodes {
				if ancestor(g, op.Tree, id) {
					removed[id] = true
				}
			}
		}
		for id := range removed {
			delete(g.nodes, id)
		}
		for id, b := range g.bindings {
			if removed[b.Consumer] || removed[b.Provider] {
				delete(g.bindings, id)
			}
		}
	case OpLink, OpBindNest:
		if op.Binding.ID == "" {
			return ErrInvalidLink
		}
		if _, exists := g.bindings[op.Binding.ID]; exists {
			return ErrInvalidLink
		}
		g.nextOrder++
		g.bindings[op.Binding.ID] = Binding{Link: op.Binding, Local: op.Kind == OpBindNest, Order: g.nextOrder}
	case OpRebind:
		b, exists := g.bindings[op.Binding.ID]
		if !exists {
			return ErrNotFound
		}
		if op.Binding.Consumer != b.Consumer || op.Binding.Port != b.Port {
			return ErrInvalidLink
		}
		b.Link = op.Binding
		g.bindings[b.ID] = b
	case OpUnlink:
		if _, exists := g.bindings[op.Binding.ID]; !exists {
			return ErrNotFound
		}
		delete(g.bindings, op.Binding.ID)
	default:
		return ErrInvalidOperation
	}
	return nil
}

func validateRetained(old, next *graph, ops []Operation) error {
	keep := map[TreeID]bool{}
	for _, op := range ops {
		if op.Kind == OpReplace {
			keep[op.Tree] = op.KeepChildren
		}
	}
	for id, n := range next.nodes {
		before := old.nodes[id]
		if before == nil {
			continue
		}
		if before.parent != n.parent && n.instance != nil {
			for child, sub := range next.nodes {
				if child == id || ancestor(next, id, child) {
					if sub.instance != nil && !sub.definition.Reparentable {
						return failure("move", child, "", ErrReparentUnsupported)
					}
				}
			}
		}
		if n.instance == nil {
			if !before.definition.HotSwappable || !n.definition.HotSwappable {
				return failure("replace", id, "", ErrUnsupportedHotReplace)
			}
			for child, sub := range next.nodes {
				if ancestor(next, id, child) && old.nodes[child] != nil && sub.instance == old.nodes[child].instance {
					if !keep[id] || !n.definition.PreserveChildren {
						return failure("replace-children", id, "", ErrUnsupportedHotReplace)
					}
				}
			}
			continue
		}
		// Construction/start resources cannot be silently changed underneath a
		// retained instance. Replace the consumer in the same plan instead.
		for _, b := range next.bindings {
			if b.Consumer != id {
				continue
			}
			p, _ := portOf(n.definition, b.Port)
			if p.Phase != Runtime {
				if _, existed := old.bindings[b.ID]; !existed {
					return failure("retained-resource", id, b.Port, ErrDependencyInUse)
				}
			}
		}
		for _, b := range old.bindings {
			if b.Consumer != id {
				continue
			}
			p, _ := portOf(before.definition, b.Port)
			if p.Phase == Runtime {
				continue
			}
			after, ok := next.bindings[b.ID]
			provider := next.nodes[after.Provider]
			if !ok || after.Link != b.Link || provider == nil || provider.instance != old.nodes[b.Provider].instance {
				return failure("retained-resource", id, b.Port, ErrDependencyInUse)
			}
		}
	}
	return nil
}

func validateReplacementContracts(old, next *graph) error {
	for id, n := range next.nodes {
		before := old.nodes[id]
		if before == nil || before.instance == n.instance {
			continue
		}
		for _, c := range before.definition.Capabilities {
			if c.Visibility == 0 {
				continue
			}
			after, ok := capOf(n.definition, c.ID)
			value := n.instance.capabilities[c.ID]
			if !ok || after.Visibility&c.Visibility != c.Visibility || !compatible(typeOf(value), c.Type) || after.Contract.ID != c.Contract.ID || after.Contract.Version < c.Contract.Version {
				return failure("replace-contract", id, "", ErrContractMismatch)
			}
		}
	}
	return nil
}

// resourceDependencies retain only explicitly declared lifecycle resources.
// Runtime Link cycles remain legal and do not create reference-count cycles.
func resourceDependencies(g *graph, id TreeID) []*instance {
	seen := map[*instance]bool{}
	for _, b := range g.bindings {
		if b.Consumer != id {
			continue
		}
		p, _ := portOf(g.nodes[id].definition, b.Port)
		if p.Phase != Runtime && g.nodes[b.Provider].instance != nil {
			seen[g.nodes[b.Provider].instance] = true
		}
	}
	var result []*instance
	for dep := range seen {
		result = append(result, dep)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result
}

func (f *Forest) rollback(g *graph, created []*instance) {
	f.gate.Lock()
	defer f.gate.Unlock()
	newSet := map[*instance]bool{}
	for _, i := range created {
		newSet[i] = true
	}
	for _, i := range created {
		for _, dep := range resourceDependencies(g, i.tree) {
			i.dependencies = append(i.dependencies, dep)
			dep.refs++
		}
		if n := g.nodes[i.tree]; n != nil {
			if parent := g.nodes[n.parent]; parent != nil && parent.instance != nil {
				i.dependencies = append(i.dependencies, parent.instance)
				parent.instance.refs++
			}
		}
	}
	for _, i := range created {
		if newSet[i] && i.refs == 0 {
			f.drainLocked(i)
		}
	}
}
