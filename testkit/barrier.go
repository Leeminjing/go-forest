// Package testkit coordinates real fixtures and checks their detached evidence.
// It neither runs an execution engine nor manages its resources.
package testkit

import (
	"context"
	"sync"
)

// Barrier holds one designated execution at a real fixture hook.
// Release it in cleanup before joining the fixture, including on test failure.
type Barrier struct {
	mu               sync.Mutex
	used, released   bool
	arrived, release chan struct{}
}

func NewBarrier() *Barrier {
	return &Barrier{arrived: make(chan struct{}), release: make(chan struct{})}
}
func (b *Barrier) Arrived() <-chan struct{} { return b.arrived }
func (b *Barrier) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.released {
		b.released = true
		close(b.release)
	}
}
func (b *Barrier) Hold(ctx context.Context) error {
	b.mu.Lock()
	if b.used {
		b.mu.Unlock()
		return ErrBarrierUsed
	}
	if b.released {
		b.mu.Unlock()
		return ErrBarrierReleased
	}
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return err
	}
	b.used = true
	close(b.arrived)
	b.mu.Unlock()
	return Await(ctx, b.release)
}

// Await has no goroutine or timer of its own. Cancellation already visible on
// entry wins; later simultaneous arrival/cancellation has no imposed ordering.
func Await(ctx context.Context, signal <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-signal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
