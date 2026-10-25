package temporalforest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

var (
	ErrInvalid      = errors.New("invalid Temporal tree configuration")
	ErrUnsupported  = errors.New("unsupported Temporal execution ownership")
	ErrUnavailable  = errors.New("Temporal worker admission closed")
	ErrNestedScope  = errors.New("management requires a fresh top-level context")
	ErrBusy         = errors.New("Temporal worker transition in progress")
	ErrPanic        = errors.New("Temporal callback panicked")
	ErrDrainTimeout = errors.New("Temporal worker drain incomplete")
)

type Registry interface {
	worker.WorkflowRegistry
	worker.ActivityRegistry
}
type Register func(Registry) error
type WorkerOptions struct {
	TypeID, Version   string
	TreeID            forest.TreeID
	TaskQueue         string
	Client            client.Options
	Native            worker.Options
	ConnectTimeout    time.Duration
	Ports             []forest.PortDescriptor
	BuildRegistration func(*forest.BuildEnv) (Register, error)
}
type Attempt struct {
	WorkflowID, RunID, ActivityID, ActivityType string
	Number                                      int32
	Generation                                  uint64
}
type Status struct {
	Identity, Namespace, TaskQueue, Phase, ErrorStage string
	NativeStopped, OwnershipFailed                    bool
	Inflight                                          int
	Started, Finished, Failed, NativeErrors           uint64
	Last                                              Attempt
}
type Control interface{ Status() Status }
type managedControl interface {
	Control
	enable(context.Context) error
	quiesce(context.Context) error
}

func valid(s string) bool {
	return s != "" && len(s) <= 256 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func safe(fn func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPanic
		}
	}()
	return fn()
}
func supportedClient(o client.Options) bool {
	if len(o.Plugins) != 0 {
		return false
	}
	for _, i := range o.Interceptors {
		if _, ok := i.(interceptor.WorkerInterceptor); ok {
			return false
		}
	}
	return true
}

func cloneClient(o client.Options) client.Options {
	o.Interceptors = slices.Clone(o.Interceptors)
	o.ContextPropagators = slices.Clone(o.ContextPropagators)
	o.ConnectionOptions.DialOptions = slices.Clone(o.ConnectionOptions.DialOptions)
	if o.ConnectionOptions.TLS != nil {
		o.ConnectionOptions.TLS = o.ConnectionOptions.TLS.Clone()
	}
	return o
}

func WorkerDefinition(f *forest.Forest, o WorkerOptions) (forest.Definition, error) {
	if f == nil || !valid(o.TypeID) || !valid(o.Version) || !valid(string(o.TreeID)) || !valid(o.TaskQueue) || o.BuildRegistration == nil || o.ConnectTimeout < 0 || (o.Client.Namespace != "" && !valid(o.Client.Namespace)) {
		return forest.Definition{}, ErrInvalid
	}
	if !supportedClient(o.Client) || o.Native.OnFatalError != nil || o.Native.BackgroundActivityContext != nil || len(o.Native.Plugins) != 0 || o.Native.EnableSessionWorker || o.Native.LocalActivityWorkerOnly {
		return forest.Definition{}, ErrUnsupported
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 5 * time.Second
	}
	o.Ports = append([]forest.PortDescriptor(nil), o.Ports...)
	o.Client = cloneClient(o.Client)
	o.Native.Interceptors = append([]interceptor.WorkerInterceptor(nil), o.Native.Interceptors...)
	return forest.Definition{TypeID: o.TypeID, Version: o.Version, Ports: o.Ports, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Control]("control", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		root, cancel := context.WithCancel(context.Background())
		identity := make([]byte, 16)
		if _, err := rand.Read(identity); err != nil {
			cancel()
			return nil, err
		}
		w := &workerTree{f: f, options: o, root: root, cancel: cancel, done: make(chan struct{}), startDone: make(chan struct{}), ownership: make(chan struct{}), status: Status{Identity: hex.EncodeToString(identity), Namespace: o.Client.Namespace, TaskQueue: o.TaskQueue, Phase: "preparing"}}
		if w.status.Namespace == "" {
			w.status.Namespace = "default"
		}
		w.cond = sync.NewCond(&w.mu)
		close(w.startDone)
		register, err := o.BuildRegistration(env)
		w.register = register
		if err != nil {
			return w, err
		}
		if register == nil {
			return w, ErrInvalid
		}
		return w, forest.Provide(env, "control", w)
	}}, nil
}

type workerTree struct {
	f                          *forest.Forest
	options                    WorkerOptions
	root                       context.Context
	cancel                     context.CancelFunc
	register                   Register
	mu                         sync.Mutex
	cond                       *sync.Cond
	native                     worker.Worker
	client                     client.Client
	status                     Status
	accepting, attempted       bool
	startDone, done, ownership chan struct{}
	stopOnce, closeOnce        sync.Once
}

func (w *workerTree) Start(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, w.options.ConnectTimeout)
	defer cancel()
	options := w.options.Client
	if options.Logger == nil {
		options.Logger = diagnosticLog{record: func() { w.mu.Lock(); w.status.NativeErrors++; w.mu.Unlock() }}
	}
	w.client, err = client.DialContext(ctx, options)
	if err != nil {
		return err
	}
	if _, err = w.client.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
		return err
	}
	return safe(func() error {
		o := w.options.Native
		o.BackgroundActivityContext = w.root
		o.DisableEagerActivities = true
		o.Interceptors = append([]interceptor.WorkerInterceptor{&scopeInterceptor{w: w}}, o.Interceptors...)
		o.OnFatalError = func(error) { w.mu.Lock(); w.status.ErrorStage = "native-fatal"; w.beginStopLocked(); w.mu.Unlock() }
		w.native = worker.New(w.client, w.options.TaskQueue, o)
		if err := w.register(registry{workflows: w.native, activities: w.native}); err != nil {
			return err
		}
		w.mu.Lock()
		w.status.Phase = "disabled"
		w.mu.Unlock()
		return nil
	})
}
func (w *workerTree) Status() Status { w.mu.Lock(); defer w.mu.Unlock(); return w.status }
func (w *workerTree) enable(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	w.mu.Lock()
	if w.status.Phase == "running" {
		w.mu.Unlock()
		return nil
	}
	if w.status.Phase == "starting" {
		w.mu.Unlock()
		return ErrBusy
	}
	if w.status.Phase != "disabled" || w.native == nil {
		w.mu.Unlock()
		return ErrUnavailable
	}
	w.attempted, w.accepting = true, true
	w.status.Phase = "starting"
	w.startDone = make(chan struct{})
	w.mu.Unlock()
	err := safe(w.native.Start)
	w.mu.Lock()
	close(w.startDone)
	if err != nil {
		w.status.ErrorStage = "native-start"
		w.beginStopLocked()
	} else if w.accepting {
		w.status.Phase = "running"
	}
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}
func (w *workerTree) beginStopLocked() {
	if w.status.Phase == "stopped" {
		return
	}
	w.accepting = false
	w.status.Phase = "quiescing"
	w.stopOnce.Do(func() {
		go func() {
			<-w.startDone
			err := safe(func() error {
				if w.native != nil {
					w.native.Stop()
				}
				return nil
			})
			w.mu.Lock()
			if err != nil {
				w.status.OwnershipFailed = true
				w.status.ErrorStage = "native-stop"
			}
			w.status.NativeStopped = err == nil
			for w.status.Inflight != 0 || w.status.OwnershipFailed {
				w.cond.Wait()
			}
			w.status.Phase = "stopped"
			close(w.done)
			w.mu.Unlock()
		}()
	})
}
func (w *workerTree) quiesce(ctx context.Context) error {
	w.mu.Lock()
	w.beginStopLocked()
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrDrainTimeout, ctx.Err())
	}
}
func (w *workerTree) Stop(_ context.Context) error {
	// Core observes its deadline while continuing to await this actual cleanup.
	w.mu.Lock()
	w.beginStopLocked()
	w.mu.Unlock()
	<-w.done
	w.closeOnce.Do(func() {
		w.cancel()
		if w.client != nil {
			w.client.Close()
		}
	})
	return nil
}

func controlCall(ctx context.Context, f *forest.Forest, tree forest.TreeID, fn func(managedControl) error) error {
	if f == nil {
		return ErrInvalid
	}
	if _, err := f.Generation(ctx); err == nil {
		return ErrNestedScope
	}
	if ctx.Value(attemptKey{}) != nil {
		return ErrNestedScope
	}
	return f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[managedControl](c, f, tree, "control", func(w managedControl) error {
			node, ok := w.(*workerTree)
			if !ok || node.f != f || node.options.TreeID != tree {
				return ErrInvalid
			}
			return fn(w)
		})
	})
}
func Enable(ctx context.Context, f *forest.Forest, tree forest.TreeID) error {
	return controlCall(ctx, f, tree, func(w managedControl) error { return w.enable(ctx) })
}
func Quiesce(ctx context.Context, f *forest.Forest, tree forest.TreeID) error {
	return controlCall(ctx, f, tree, func(w managedControl) error { return w.quiesce(ctx) })
}
func Inspect(ctx context.Context, f *forest.Forest, tree forest.TreeID) (out Status, err error) {
	err = controlCall(ctx, f, tree, func(w managedControl) error { out = w.Status(); return nil })
	return
}
