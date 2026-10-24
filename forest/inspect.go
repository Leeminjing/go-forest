package forest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type TreeInfo struct {
	ID           TreeID
	ParentID     TreeID
	InstanceID   string
	TypeID       string
	Version      string
	State        State
	Ports        []PortDescriptor
	Capabilities []CapabilityDescriptor
}
type InstanceInfo struct {
	ID         string
	TreeID     TreeID
	Definition DefinitionRef
	State      State
	Error      error
}
type LinkInfo struct {
	Binding
	State string
}
type Snapshot struct {
	Generation    uint64
	EventSequence uint64
	Trees         map[TreeID]TreeInfo
	Links         map[string]LinkInfo
	NestBindings  map[string]LinkInfo
	Instances     []InstanceInfo
}

// Snapshot returns detached topology and metadata. Mutating it cannot affect
// runtime state. Instance states are live diagnostics rather than topology.
func (f *Forest) Snapshot() Snapshot {
	f.gate.RLock()
	g := f.current.Load()
	s := Snapshot{Generation: g.generation, Trees: map[TreeID]TreeInfo{}, Links: map[string]LinkInfo{}, NestBindings: map[string]LinkInfo{}}
	f.events.mu.Lock()
	s.EventSequence = f.events.sequence
	f.events.mu.Unlock()
	for id, n := range g.nodes {
		i := n.instance.info()
		s.Trees[id] = TreeInfo{ID: id, ParentID: n.parent, InstanceID: i.ID, TypeID: n.definition.TypeID, Version: n.definition.Version, State: i.State, Ports: append([]PortDescriptor(nil), n.definition.Ports...), Capabilities: append([]CapabilityDescriptor(nil), n.definition.Capabilities...)}
	}
	for id, b := range g.bindings {
		info := LinkInfo{Binding: b, State: "bound"}
		if b.Local {
			s.NestBindings[id] = info
		} else {
			s.Links[id] = info
		}
	}
	f.gate.RUnlock()
	f.observe.Lock()
	for _, i := range f.instances {
		s.Instances = append(s.Instances, i.info())
	}
	f.observe.Unlock()
	sort.Slice(s.Instances, func(i, j int) bool { return s.Instances[i].ID < s.Instances[j].ID })
	return s
}
func (f *Forest) InspectTree(id TreeID) (TreeInfo, error) {
	t, ok := f.Snapshot().Trees[id]
	if !ok {
		return TreeInfo{}, ErrNotFound
	}
	return t, nil
}
func (f *Forest) InspectLinks() []LinkInfo {
	s := f.Snapshot()
	result := make([]LinkInfo, 0, len(s.Links))
	for _, l := range s.Links {
		result = append(result, l)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Order < result[j].Order })
	return result
}

type Event struct {
	Sequence   uint64
	Generation uint64
	Operations []OperationKind
	Trees      []TreeID
	Links      []string
	Result     string
	Error      error
	Timestamp  time.Time
}
type eventLog struct {
	mu       sync.Mutex
	sequence uint64
	capacity int
	items    []Event
	changed  chan struct{}
}

func (l *eventLog) init(capacity int) { l.capacity = capacity; l.changed = make(chan struct{}) }
func cloneEvent(e Event) Event {
	e.Operations = append([]OperationKind(nil), e.Operations...)
	e.Trees = append([]TreeID(nil), e.Trees...)
	e.Links = append([]string(nil), e.Links...)
	return e
}
func (l *eventLog) publish(generation uint64, ops []Operation, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sequence++
	e := Event{Sequence: l.sequence, Generation: generation, Result: "success", Error: err, Timestamp: time.Now()}
	if err != nil {
		e.Result = "failed"
		if _, ok := err.(*GenerationConflict); ok {
			e.Result = "conflict"
		}
	}
	trees := map[TreeID]bool{}
	links := map[string]bool{}
	for _, op := range ops {
		e.Operations = append(e.Operations, op.Kind)
		for _, id := range []TreeID{op.Tree, op.Parent, op.Binding.Consumer, op.Binding.Provider} {
			if id != "" {
				trees[id] = true
			}
		}
		if op.Binding.ID != "" {
			links[op.Binding.ID] = true
		}
	}
	for id := range trees {
		e.Trees = append(e.Trees, id)
	}
	sort.Slice(e.Trees, func(i, j int) bool { return e.Trees[i] < e.Trees[j] })
	for id := range links {
		e.Links = append(e.Links, id)
	}
	sort.Strings(e.Links)
	l.items = append(l.items, e)
	if len(l.items) > l.capacity {
		l.items = append([]Event(nil), l.items[len(l.items)-l.capacity:]...)
	}
	close(l.changed)
	l.changed = make(chan struct{})
}
func (f *Forest) Events() []Event {
	f.events.mu.Lock()
	defer f.events.mu.Unlock()
	result := make([]Event, len(f.events.items))
	for i, e := range f.events.items {
		result[i] = cloneEvent(e)
	}
	return result
}

// Watch is cursor-based, with bounded shared history and no subscriber goroutine.
// After ErrEventGap, read Snapshot then create a fresh cursor from Events.
type Watcher struct {
	forest *Forest
	mu     sync.Mutex
	after  uint64
}

func (f *Forest) Watch(after uint64) *Watcher { return &Watcher{forest: f, after: after} }
func (w *Watcher) Next(ctx context.Context) (Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	l := &w.forest.events
	for {
		l.mu.Lock()
		if len(l.items) > 0 && w.after < l.items[0].Sequence-1 {
			oldest := l.items[0].Sequence
			l.mu.Unlock()
			return Event{}, fmt.Errorf("%w: after=%d oldest=%d", ErrEventGap, w.after, oldest)
		}
		for _, e := range l.items {
			if e.Sequence > w.after {
				w.after = e.Sequence
				l.mu.Unlock()
				return cloneEvent(e), nil
			}
		}
		ch := l.changed
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-ch:
		}
	}
}
