package performance

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
)

// held stores only scalar observations and join channels, never an old Context,
// graph pointer or raw capability. The goroutine owns its pinned context.
type held struct {
	generation, first uint64
	release           chan struct{}
	done              chan struct{}
	err               error
	once              sync.Once
}

func hold(ctx context.Context, f *forest.Forest) (*held, error) {
	h := &held{release: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan error, 1)
	go func() {
		signaled := false
		err := f.Invoke(ctx, func(call context.Context) error {
			return forest.Use[Runner](call, f, "consumer", "read", func(r Runner) error {
				var e error
				h.first, e = r.Read(call)
				if e != nil {
					return e
				}
				h.generation, e = f.Generation(call)
				if e != nil {
					return e
				}
				signaled = true
				ready <- nil
				<-h.release
				second, e := r.Read(call)
				if e != nil {
					return e
				}
				g, e := f.Generation(call)
				if e != nil {
					return e
				}
				if second != h.first || g != h.generation {
					return fmt.Errorf("held call changed: %d/%d to %d/%d", h.first, h.generation, second, g)
				}
				return nil
			})
		})
		if !signaled {
			ready <- err
		}
		h.err = err
		close(h.done)
	}()
	select {
	case e := <-ready:
		if e != nil {
			close(h.release)
			<-h.done
			return nil, e
		}
		return h, nil
	case <-ctx.Done():
		close(h.release)
		<-h.done
		return nil, ctx.Err()
	}
}
func releaseHeld(calls []*held) error {
	for _, h := range calls {
		h.once.Do(func() { close(h.release) })
	}
	var err error
	for _, h := range calls {
		<-h.done
		err = errors.Join(err, h.err)
	}
	return err
}
