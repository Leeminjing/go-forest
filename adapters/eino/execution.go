// Package einoforest hosts explicitly managed native Eino executions as trees.
package einoforest

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
)

var (
	ErrInvalid         = errors.New("invalid Eino adapter configuration or request")
	ErrUnmanaged       = errors.New("execution has no completion ownership contract")
	ErrExecutionClosed = errors.New("execution admission sealed")
	ErrBusy            = errors.New("execution identity already active")
	ErrBudget          = errors.New("execution budget exceeded")
	ErrNativePanic     = errors.New("native execution panicked")
)

type Lifetime interface {
	Cancel()
	Join() error
}
type executionKey struct{}

// Execution tracks every resource-using callback and producer. Builders must
// guard all such work; arbitrary opaque native agents are not implicitly safe.
type Execution struct {
	cancel context.CancelFunc
	mu     sync.Mutex
	cond   *sync.Cond
	sealed bool
	active int
	err    error
}

func newExecution(parent context.Context) (context.Context, *Execution) {
	ctx, cancel := context.WithCancel(parent)
	e := &Execution{cancel: cancel}
	e.cond = sync.NewCond(&e.mu)
	return context.WithValue(ctx, executionKey{}, e), e
}
func ExecutionFrom(ctx context.Context) *Execution {
	e, _ := ctx.Value(executionKey{}).(*Execution)
	return e
}
func (e *Execution) enter() (func(), error) {
	if e == nil {
		return nil, ErrUnmanaged
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sealed {
		return nil, ErrExecutionClosed
	}
	e.active++
	return func() { e.mu.Lock(); e.active--; e.cond.Broadcast(); e.mu.Unlock() }, nil
}
func (e *Execution) Cancel() { e.cancel() }
func (e *Execution) Join() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sealed = true
	for e.active != 0 {
		e.cond.Wait()
	}
	return e.err
}

// Guard denies late resource access after sealing. A producer that outlives a
// synchronous method must acquire ownership before it is launched via Go.
func Guard(ctx context.Context, fn func() error) error {
	if fn == nil {
		return ErrInvalid
	}
	leave, err := ExecutionFrom(ctx).enter()
	if err != nil {
		return err
	}
	defer leave()
	return fn()
}
func Go(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return ErrInvalid
	}
	e := ExecutionFrom(ctx)
	leave, err := e.enter()
	if err != nil {
		return err
	}
	go func() {
		defer leave()
		err := safe(func() error { return fn(ctx) })
		if err != nil {
			e.mu.Lock()
			e.err = errors.Join(e.err, err)
			e.mu.Unlock()
		}
	}()
	return nil
}
func safe(fn func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrNativePanic
		}
	}()
	return fn()
}

type leaseKey struct{ namespace, id string }

// Owner is explicitly shared by all versions of one execution domain. It is a
// process-local admission gate, not a distributed lock or dependency locator.
type Owner struct {
	forest *forest.Forest
	mu     sync.Mutex
	active map[leaseKey]bool
	limit  int
}

func NewOwner(f *forest.Forest, limit int) (*Owner, error) {
	if f == nil || limit < 1 || limit > 4096 {
		return nil, ErrInvalid
	}
	return &Owner{forest: f, active: map[leaseKey]bool{}, limit: limit}, nil
}
func (o *Owner) acquire(namespace, id string) (func(), error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.active) >= o.limit {
		return nil, ErrBusy
	}
	key := leaseKey{namespace, id}
	if id != "" && o.active[key] {
		return nil, ErrBusy
	}
	if id == "" {
		for n := 0; ; n++ {
			key.id = fmt.Sprintf("\x00%d", n)
			if !o.active[key] {
				break
			}
		}
	}
	o.active[key] = true
	return func() { o.mu.Lock(); delete(o.active, key); o.mu.Unlock() }, nil
}

type Config struct {
	TypeID, Version, Namespace string
	Owner                      *Owner
	Ports                      []forest.PortDescriptor
	HotSwappable               bool
}

func (c Config) valid() bool {
	return c.TypeID != "" && c.Version != "" && validKey(c.Namespace) && c.Owner != nil
}

type Status struct {
	Started, Finished, Failed uint64
	Active, Joining           int
	LastPhase                 string
	OwnershipFailed           bool
}

// A native iterator may block until an external resource receives Cancel.
// Forward Context cancellation independently of iteration, then join that
// forwarding callback as well. Cancel is invoked exactly once on every path.
type watchedLifetime struct {
	runtime   *runtime
	child     Lifetime
	once      sync.Once
	cancelErr error
	stopWatch func() bool
	watchDone chan struct{}
}

func (r *runtime) watch(ctx context.Context, child Lifetime) Lifetime {
	w := &watchedLifetime{runtime: r, child: child, watchDone: make(chan struct{})}
	w.stopWatch = context.AfterFunc(ctx, func() { defer close(w.watchDone); w.Cancel() })
	return w
}
func (w *watchedLifetime) Cancel() {
	w.once.Do(func() { w.cancelErr = w.runtime.lifetimeCall(func() error { w.child.Cancel(); return nil }) })
}
func (w *watchedLifetime) Join() error {
	if !w.stopWatch() {
		<-w.watchDone
	}
	w.Cancel()
	return errors.Join(w.cancelErr, w.runtime.lifetimeCall(w.child.Join))
}
func (r *runtime) lifetimeCall(fn func() error) (err error) {
	defer func() {
		if recover() != nil {
			r.mu.Lock()
			r.status.OwnershipFailed = true
			r.mu.Unlock()
			err = errors.Join(ErrUnmanaged, ErrNativePanic)
		}
	}()
	return fn()
}

func (r *runtime) join(l Lifetime) (err error) {
	r.mu.Lock()
	r.status.Joining++
	r.status.LastPhase = "joining"
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.status.Joining--; r.mu.Unlock() }()
	defer func() {
		if recover() != nil {
			r.mu.Lock()
			r.status.OwnershipFailed = true
			r.mu.Unlock()
			err = errors.Join(ErrUnmanaged, ErrNativePanic)
		}
	}()
	l.Cancel()
	return l.Join()
}
func (r *runtime) stop(ctx context.Context, l forest.Lifecycle) error {
	r.mu.Lock()
	unsafe := r.status.OwnershipFailed
	r.mu.Unlock()
	if unsafe {
		return ErrUnmanaged
	}
	if l == nil {
		return nil
	}
	return l.Stop(ctx)
}

type runtime struct {
	config Config
	mu     sync.Mutex
	status Status
}

func (r *runtime) Status() Status { r.mu.Lock(); defer r.mu.Unlock(); return r.status }
func (r *runtime) run(ctx context.Context, id string, fn func(context.Context) error) (err error) {
	r.mu.Lock()
	unsafe := r.status.OwnershipFailed
	r.mu.Unlock()
	if unsafe {
		return ErrUnmanaged
	}
	if id != "" && !validKey(id) {
		return ErrInvalid
	}
	if _, err = r.config.Owner.forest.Generation(ctx); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	release, err := r.config.Owner.acquire(r.config.Namespace, id)
	if err != nil {
		return err
	}
	defer func() {
		r.mu.Lock()
		unsafe := r.status.OwnershipFailed
		r.mu.Unlock()
		if !unsafe {
			release()
		}
	}()
	caller := ctx
	ctx, execution := newExecution(ctx)
	r.mu.Lock()
	r.status.Started++
	r.status.Active++
	r.status.LastPhase = "running"
	r.mu.Unlock()
	defer func() {
		execution.Cancel()
		r.mu.Lock()
		r.status.Joining++
		r.status.LastPhase = "joining"
		r.mu.Unlock()
		err = errors.Join(err, execution.Join(), caller.Err())
		r.mu.Lock()
		r.status.Active--
		r.status.Joining--
		r.status.Finished++
		if err != nil {
			r.status.Failed++
		}
		r.status.LastPhase = "finished"
		r.mu.Unlock()
	}()
	return safe(func() error { return fn(ctx) })
}
