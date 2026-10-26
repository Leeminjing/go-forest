package fxforest

import (
	"context"
	"errors"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
)

// Bridge is supplied only to the module factory. It has no service locator.
type Bridge struct {
	f                          *forest.Forest
	options                    Options
	control                    *control
	mu                         sync.Mutex
	cond                       *sync.Cond
	statusValue                Status
	root                       context.Context
	cancel                     context.CancelFunc
	startCtx, cleanupCtx       context.Context
	startCancel, cleanupCancel context.CancelFunc
	cleanupErr                 error
	calls                      map[*callState]context.CancelFunc
	enabled, quiesced          chan struct{}
	sealed                     bool
	resources                  []*Ticket
	ingresses                  []func() error
}

func (b *Bridge) status() Status { b.mu.Lock(); defer b.mu.Unlock(); return b.statusValue }
func (b *Bridge) failure(stage string, err error) error {
	b.mu.Lock()
	b.statusValue.ErrorStage = stage
	id := b.statusValue.Identity
	b.mu.Unlock()
	return &Failure{Tree: b.options.TreeID, Identity: id, Stage: stage, Err: err}
}
func (b *Bridge) ownership(err error) error {
	b.mu.Lock()
	b.statusValue.OwnershipFailed = true
	b.statusValue.Phase = "failed"
	b.mu.Unlock()
	return b.failure("cleanup", errors.Join(ErrOwnership, err))
}
func (b *Bridge) seal() {
	b.mu.Lock()
	if b.sealed {
		b.mu.Unlock()
		return
	}
	b.sealed = true
	b.statusValue.Accepting = false
	b.statusValue.Phase = "quiescing"
	cancels := make([]context.CancelFunc, 0, len(b.calls))
	for _, c := range b.calls {
		cancels = append(cancels, c)
	}
	stops := append([]func() error(nil), b.ingresses...)
	b.mu.Unlock()
	b.cancel()
	for _, c := range cancels {
		c()
	}
	go func() {
		for i := len(stops) - 1; i >= 0; i-- {
			if err := safe(stops[i]); err != nil {
				b.mu.Lock()
				b.cleanupErr = errors.Join(b.cleanupErr, err)
				b.statusValue.OwnershipFailed = true
				b.statusValue.ErrorStage = "ingress-stop"
				b.mu.Unlock()
			}
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		for b.statusValue.Inflight != 0 || b.statusValue.Loops != 0 {
			b.cond.Wait()
		}
		b.statusValue.Phase = "quiesced"
		close(b.quiesced)
	}()
}
func (b *Bridge) enable() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.statusValue.Accepting {
		return nil
	}
	if b.sealed || b.statusValue.Phase != "prepared-disabled" {
		return ErrUnavailable
	}
	b.statusValue.Accepting = true
	b.statusValue.Phase = "enabled"
	close(b.enabled)
	return nil
}

func manage(ctx context.Context, f *forest.Forest, id forest.TreeID, fn func(*Bridge) error) error {
	if f == nil {
		return ErrInvalid
	}
	if _, err := f.Generation(ctx); err == nil {
		return ErrNestedScope
	}
	if ctx.Value(callKey{}) != nil {
		return ErrNestedScope
	}
	if ctx.Value(loopKey{}) != nil {
		return ErrNestedScope
	}
	return f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[Control](c, f, id, "control", func(v Control) error {
			own, ok := v.(*control)
			if !ok || own.b.f != f || own.b.options.TreeID != id {
				return ErrInvalid
			}
			return fn(own.b)
		})
	})
}
func Enable(ctx context.Context, f *forest.Forest, id forest.TreeID) error {
	return manage(ctx, f, id, func(b *Bridge) error { return b.enable() })
}
func Quiesce(ctx context.Context, f *forest.Forest, id forest.TreeID) error {
	return manage(ctx, f, id, func(b *Bridge) error {
		b.seal()
		select {
		case <-b.quiesced:
			b.mu.Lock()
			err := b.cleanupErr
			b.mu.Unlock()
			if err != nil {
				return errors.Join(ErrOwnership, err)
			}
			return nil
		case <-ctx.Done():
			return errors.Join(ErrDrainTimeout, ctx.Err())
		}
	})
}
func Inspect(ctx context.Context, f *forest.Forest, id forest.TreeID) (s Status, err error) {
	err = manage(ctx, f, id, func(b *Bridge) error { s = b.status(); return nil })
	return
}

type callKey struct{}

func (b *Bridge) use(ctx context.Context, fn func(context.Context) error) (err error) {
	if _, err := b.f.Generation(ctx); err != nil {
		return err
	}
	c, cancel := context.WithCancel(ctx)
	call := &callState{owner: b, open: true, cancel: cancel}
	call.cond = sync.NewCond(&call.mu)
	b.mu.Lock()
	if !b.statusValue.Accepting {
		b.mu.Unlock()
		cancel()
		return ErrUnavailable
	}
	b.calls[call] = cancel
	b.statusValue.Inflight++
	b.mu.Unlock()
	defer func() {
		err = errors.Join(err, call.join())
		b.mu.Lock()
		delete(b.calls, call)
		b.statusValue.Inflight--
		b.cond.Broadcast()
		b.mu.Unlock()
	}()
	return forest.Use[Control](c, b.f, b.options.TreeID, "control", func(v Control) error {
		if v != b.control {
			return ErrUnavailable
		}
		return fn(context.WithValue(c, callKey{}, call))
	})
}
