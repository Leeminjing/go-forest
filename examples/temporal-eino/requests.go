package einodemo

import (
	"context"
	"sync"
)

type requestKey struct{}
type request struct{ cancel context.CancelFunc }
type requestGate struct {
	mu     sync.Mutex
	closed bool
	calls  map[*request]struct{}
	done   chan struct{}
}

func (g *requestGate) init()          { g.calls = map[*request]struct{}{}; g.done = make(chan struct{}) }
func (g *requestGate) isClosed() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.closed }
func (g *requestGate) enter(ctx context.Context) (context.Context, func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, nil, ErrUnavailable
	}
	c, cancel := context.WithCancel(ctx)
	r := &request{cancel: cancel}
	g.calls[r] = struct{}{}
	return context.WithValue(c, requestKey{}, r), func() {
		cancel()
		g.mu.Lock()
		defer g.mu.Unlock()
		delete(g.calls, r)
		if g.closed && len(g.calls) == 0 {
			close(g.done)
		}
	}, nil
}
func (g *requestGate) seal() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	cancels := make([]context.CancelFunc, 0, len(g.calls))
	for r := range g.calls {
		cancels = append(cancels, r.cancel)
	}
	if len(g.calls) == 0 {
		close(g.done)
	}
	g.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
