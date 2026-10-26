package observe

import (
	"context"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"sync"
	"sync/atomic"
)

type Hook func(context.Context, protocol.Point) error
type Meter struct {
	Model, Apply, Query, Resume, Graph atomic.Int64
	Hook                               Hook
	Process                            string
	Forest                             *forest.Forest
	instancesMu                        sync.Mutex
	instances                          map[forest.TreeID]string
}

func (m *Meter) Counts() protocol.Counts {
	return protocol.Counts{Model: m.Model.Load(), Apply: m.Apply.Load(), Query: m.Query.Load(), Resume: m.Resume.Load(), Graph: m.Graph.Load()}
}

type key struct{}
type engineKey struct{}

func WithEngine(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, engineKey{}, id)
}
func (m *Meter) Engine(tree forest.TreeID) string {
	m.instancesMu.Lock()
	defer m.instancesMu.Unlock()
	if m.instances == nil {
		m.instances = map[forest.TreeID]string{}
	}
	if id := m.instances[tree]; id != "" {
		return id
	}
	if node, ok := m.Forest.Snapshot().Trees[tree]; ok {
		id := m.Process + "/" + node.InstanceID
		m.instances[tree] = id
		return id
	}
	return ""
}

type trace struct {
	mu    sync.Mutex
	items []protocol.Point
}

func Begin(ctx context.Context) context.Context { return context.WithValue(ctx, key{}, &trace{}) }
func Trace(ctx context.Context) []protocol.Point {
	t, _ := ctx.Value(key{}).(*trace)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]protocol.Point(nil), t.items...)
}
func (m *Meter) At(ctx context.Context, stage, version string, in protocol.Input, step int, result *protocol.Result) error {
	g, e := m.Forest.Generation(ctx)
	if e != nil {
		return e
	}
	p := protocol.Point{Stage: stage, Version: version, Process: m.Process, Generation: g, Input: in, Step: step}
	p.Instance, _ = ctx.Value(engineKey{}).(string)
	if t, _ := ctx.Value(key{}).(*trace); t != nil {
		t.mu.Lock()
		if len(t.items) >= 64 {
			t.mu.Unlock()
			return protocol.ErrPartial
		}
		t.items = append(t.items, p)
		t.mu.Unlock()
	}
	if result != nil {
		copy := protocol.CopyResult(*result)
		p.Result = &copy
	}
	if m.Hook != nil {
		return m.Hook(ctx, p)
	}
	return nil
}
