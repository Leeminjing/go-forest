package fxforest

import (
	"context"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
)

type callState struct {
	owner  *Bridge
	mu     sync.Mutex
	cond   *sync.Cond
	open   bool
	users  int
	err    error
	cancel context.CancelFunc
}

func (s *callState) enter() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return forest.ErrScopeClosed
	}
	s.users++
	s.owner.mu.Lock()
	s.owner.statusValue.Tasks++
	s.owner.mu.Unlock()
	return nil
}
func (s *callState) leave(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
	s.users--
	s.owner.mu.Lock()
	s.owner.statusValue.Tasks--
	s.owner.mu.Unlock()
	s.cond.Broadcast()
}
func (s *callState) join() error {
	s.mu.Lock()
	s.open = false
	s.mu.Unlock()
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.users != 0 {
		s.cond.Wait()
	}
	return s.err
}

// Guard registers synchronous resource work before executing it. It rejects
// callbacks arriving after their enclosing Use/Run callback has sealed.
func Guard(ctx context.Context, fn func(context.Context) error) error {
	s, ok := ctx.Value(callKey{}).(*callState)
	if !ok {
		return forest.ErrScopeClosed
	}
	if fn == nil {
		return ErrInvalid
	}
	if err := s.enter(); err != nil {
		return err
	}
	err := safe(func() error { return fn(ctx) })
	s.leave(err)
	return err
}

// Go registers before spawning. Its actual return, including panic/error, is
// joined before the enclosing Forest invocation releases its generation.
func Go(ctx context.Context, fn func(context.Context) error) error {
	s, ok := ctx.Value(callKey{}).(*callState)
	if !ok {
		return forest.ErrScopeClosed
	}
	if fn == nil {
		return ErrInvalid
	}
	if err := s.enter(); err != nil {
		return err
	}
	go func() { err := safe(func() error { return fn(ctx) }); s.leave(err) }()
	return nil
}
