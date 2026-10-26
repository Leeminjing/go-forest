// Package trace records bounded application boundary evidence, not topology.
package trace

import (
	"context"
	"sync"

	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
)

type key struct{}
type Trace struct {
	mu     sync.Mutex
	points []protocol.Point
}

func Attach(ctx context.Context) (context.Context, *Trace) {
	t := &Trace{}
	return context.WithValue(ctx, key{}, t), t
}
func Add(ctx context.Context, p protocol.Point) {
	if t, ok := ctx.Value(key{}).(*Trace); ok {
		t.mu.Lock()
		defer t.mu.Unlock()
		if len(t.points) < 64 {
			t.points = append(t.points, p)
		}
	}
}
func (t *Trace) Points() []protocol.Point {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]protocol.Point(nil), t.points...)
}
