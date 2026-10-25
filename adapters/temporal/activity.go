package temporalforest

import (
	"context"
	"errors"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
)

type attemptKey struct{}
type tasks struct {
	mu     sync.Mutex
	cond   *sync.Cond
	sealed bool
	active int
	err    error
}

func (t *tasks) enter() (func(), error) {
	if t == nil {
		return nil, ErrUnsupported
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sealed {
		return nil, ErrUnavailable
	}
	t.active++
	return func() { t.mu.Lock(); t.active--; t.cond.Broadcast(); t.mu.Unlock() }, nil
}

// Guard denies resource access after an attempt has sealed its admission.
func Guard(ctx context.Context, fn func() error) error {
	if fn == nil {
		return ErrInvalid
	}
	t, _ := ctx.Value(attemptKey{}).(*tasks)
	leave, err := t.enter()
	if err != nil {
		return err
	}
	defer leave()
	return fn()
}

// Go admits a producer before launch; its real completion is awaited in Scope.
func Go(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return ErrInvalid
	}
	t, _ := ctx.Value(attemptKey{}).(*tasks)
	leave, err := t.enter()
	if err != nil {
		return err
	}
	go func() {
		defer leave()
		err := safe(func() error { return fn(ctx) })
		if err != nil {
			t.mu.Lock()
			if t.err == nil {
				t.err = err
			}
			t.mu.Unlock()
		}
	}()
	return nil
}

type scopeInterceptor struct {
	interceptor.WorkerInterceptorBase
	w *workerTree
}

func (s *scopeInterceptor) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityScope{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, w: s.w}
}

type activityScope struct {
	interceptor.ActivityInboundInterceptorBase
	w *workerTree
}

func (a *activityScope) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (out any, err error) {
	w := a.w
	info := activity.GetInfo(ctx)
	if info.IsLocalActivity || info.WorkflowExecution.ID == "" {
		return nil, temporal.NewNonRetryableApplicationError("unsupported activity mode", "ForestUnsupported", ErrUnsupported)
	}
	w.mu.Lock()
	if !w.accepting {
		w.mu.Unlock()
		return nil, ErrUnavailable
	}
	w.status.Inflight++
	w.status.Started++
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.status.Inflight--
		w.status.Finished++
		if err != nil {
			w.status.Failed++
		}
		w.cond.Broadcast()
		w.mu.Unlock()
	}()
	err = w.f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[Control](c, w.f, w.options.TreeID, "control", func(current Control) (err error) {
			if current != w {
				return ErrUnavailable
			}
			gen, err := w.f.Generation(c)
			if err != nil {
				return err
			}
			w.mu.Lock()
			w.status.Last = Attempt{WorkflowID: bounded(info.WorkflowExecution.ID), RunID: bounded(info.WorkflowExecution.RunID), ActivityID: bounded(info.ActivityID), ActivityType: bounded(info.ActivityType.Name), Number: info.Attempt, Generation: gen}
			w.mu.Unlock()
			t := &tasks{}
			t.cond = sync.NewCond(&t.mu)
			c, cancel := context.WithCancel(context.WithValue(c, attemptKey{}, t))
			defer func() {
				cancel()
				t.mu.Lock()
				t.sealed = true
				for t.active != 0 {
					t.cond.Wait()
				}
				// Temporal's failure converter dispatches on the concrete error.
				// Do not wrap a native ApplicationError/CancelError in errors.Join.
				if err == nil {
					err = t.err
				}
				t.mu.Unlock()
			}()
			err = safe(func() error { var e error; out, e = a.Next.ExecuteActivity(c, in); return e })
			if errors.Is(err, activity.ErrResultPending) {
				w.mu.Lock()
				w.status.OwnershipFailed = true
				w.status.ErrorStage = "async-result-pending"
				w.beginStopLocked()
				w.mu.Unlock()
				// An unsupported escaped execution cannot authorize resource release.
				<-w.ownership
			}
			return err
		})
	})
	return
}
func bounded(s string) string {
	if len(s) > 256 {
		return s[:256]
	}
	return s
}
