package fxforest

import (
	"context"
	"sync"
)

// Ticket is a once-only resource cleanup receipt shared with native OnStop.
type Ticket struct {
	owner *Bridge
	fn    func(context.Context) error
	once  sync.Once
	err   error
}

// Defer registers owned cleanup immediately after allocation. Registration is
// allowed only during construction or a native startup hook. Borrowed resources
// must not be registered. Tickets are closed in reverse registration order.
func (b *Bridge) Defer(fn func(context.Context) error) (*Ticket, error) {
	if fn == nil {
		return nil, ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed || (b.statusValue.Phase != "building" && b.statusValue.Phase != "preparing") {
		return nil, ErrUnavailable
	}
	t := &Ticket{owner: b, fn: fn}
	b.resources = append(b.resources, t)
	b.statusValue.CleanupPending++
	return t, nil
}

// Close passes cancellation to cleanup but joins its actual return. A deadline
// cannot make another caller run cleanup twice or prove resource release.
func (t *Ticket) Close(ctx context.Context) error {
	t.once.Do(func() {
		t.err = safe(func() error { return t.fn(ctx) })
		if t.err == nil {
			t.owner.mu.Lock()
			t.owner.statusValue.CleanupPending--
			t.owner.mu.Unlock()
		}
	})
	return t.err
}
func (b *Bridge) closeResources(ctx context.Context) error {
	ctx = b.cleanupContext(ctx)
	for i := len(b.resources) - 1; i >= 0; i-- {
		if err := b.resources[i].Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
