package fxforest

import (
	"context"
	"errors"

	"go.uber.org/fx"
)

// Named fields deliberately expose neither the native App nor an actuator.
type hooks struct {
	b      *Bridge
	native fx.Lifecycle
}

func (h *hooks) Append(v fx.Hook) {
	w := v
	if v.OnStart != nil {
		w.OnStart = func(context.Context) error {
			b := h.b
			b.mu.Lock()
			b.statusValue.HookRunning++
			ctx := b.startCtx
			b.mu.Unlock()
			defer func() { b.mu.Lock(); b.statusValue.HookRunning--; b.mu.Unlock() }()
			err := ctx.Err()
			if err == nil {
				err = safe(func() error { return v.OnStart(ctx) })
			}
			if err == nil {
				err = ctx.Err()
			}
			if err != nil {
				b.seal()
				<-b.quiesced
				return b.failure("hook-start", err)
			}
			return nil
		}
	}
	if v.OnStop != nil {
		w.OnStop = func(context.Context) error {
			b := h.b
			ctx := b.cleanupContext(context.Background())
			b.mu.Lock()
			b.statusValue.HookRunning++
			b.mu.Unlock()
			defer func() { b.mu.Lock(); b.statusValue.HookRunning--; b.mu.Unlock() }()
			err := safe(func() error { return v.OnStop(ctx) })
			if err != nil {
				b.mu.Lock()
				b.cleanupErr = errors.Join(b.cleanupErr, err)
				b.mu.Unlock()
				return b.failure("hook-stop", err)
			}
			return nil
		}
	}
	h.native.Append(w)
}
func (b *Bridge) cleanupContext(parent context.Context) context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cleanupCtx == nil {
		b.cleanupCtx, b.cleanupCancel = context.WithTimeout(parent, b.options.StopTimeout)
	}
	return b.cleanupCtx
}
