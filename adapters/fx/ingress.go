package fxforest

import (
	"context"
	"errors"
	"net/http"
)

type loopKey struct{}

// Run is an external ingress: it opens a fresh invocation and checks that this
// exact physical tree is still present. Internal dependencies must use Refs.
func (b *Bridge) Run(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return ErrInvalid
	}
	if _, err := b.f.Generation(ctx); err == nil {
		return ErrNestedScope
	}
	if ctx.Value(callKey{}) != nil {
		return ErrNestedScope
	}
	return b.f.Invoke(ctx, func(c context.Context) error { return b.use(c, fn) })
}

// Handler retains native routing but intentionally exposes no Hijacker or
// Unwrap path. Only synchronous HTTP resource use is managed in this version.
func (b *Bridge) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if next == nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		started := false
		err := b.Run(r.Context(), func(c context.Context) error {
			started = true
			next.ServeHTTP(responseWriter{w}, r.WithContext(c))
			return nil
		})
		if err != nil && !started {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}
	})
}

type responseWriter struct{ http.ResponseWriter }

// OwnIngress registers an explicit nonblocking stop action and an actual exit
// notification during OnStart, before the native accept/serve goroutine starts.
// Closing a Context alone cannot terminate Accept or Serve.
func (b *Bridge) OwnIngress(stop func() error, done <-chan struct{}) error {
	if stop == nil || done == nil {
		return ErrInvalid
	}
	b.mu.Lock()
	if !b.canStartLoop() {
		b.mu.Unlock()
		return ErrUnavailable
	}
	b.ingresses = append(b.ingresses, stop)
	b.statusValue.Loops++
	b.mu.Unlock()
	go func() { <-done; b.loopExited(nil) }()
	return nil
}
func (b *Bridge) canStartLoop() bool {
	return !b.sealed && b.statusValue.Phase == "preparing" && b.statusValue.HookRunning > 0
}

// Loop owns a background producer, waiting for Enable before calling fn. The
// producer must use Run for each business unit and join everything before return.
func (b *Bridge) Loop(fn func(context.Context) error) error {
	if fn == nil {
		return ErrInvalid
	}
	b.mu.Lock()
	if !b.canStartLoop() {
		b.mu.Unlock()
		return ErrUnavailable
	}
	b.statusValue.Loops++
	b.mu.Unlock()
	go func() {
		var err error
		select {
		case <-b.root.Done():
		case <-b.enabled:
			if b.root.Err() == nil {
				err = safe(func() error { return fn(context.WithValue(b.root, loopKey{}, b)) })
			}
		}
		b.loopExited(err)
	}()
	return nil
}
func (b *Bridge) loopExited(err error) {
	b.mu.Lock()
	b.statusValue.Loops--
	unexpected := err != nil && !errors.Is(err, context.Canceled)
	if unexpected {
		b.statusValue.ErrorStage = "loop"
	}
	b.cond.Broadcast()
	b.mu.Unlock()
	if unexpected {
		b.seal()
	}
}
