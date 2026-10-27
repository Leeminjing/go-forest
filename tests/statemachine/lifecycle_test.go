package statemachine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

type releaseGate struct {
	release chan struct{}
	once    sync.Once
}

func newRelease() releaseGate { return releaseGate{release: make(chan struct{})} }
func (g *releaseGate) open()  { g.once.Do(func() { close(g.release) }) }

type preparation struct {
	gate                                                        releaseGate
	config                                                      m.Action
	started                                                     atomic.Bool
	entered, canceled, readEntered, workExited, childDone, done chan struct{}
	childStarted                                                atomic.Bool
	object, resource                                            *object
	before                                                      projection
	expected                                                    m.State
	issues                                                      []m.Issue
	generation, observed, sequence                              uint64
	err, childErr                                               error
	cancel                                                      context.CancelFunc
	canceledByHost                                              bool
}
type stopping struct {
	gate              releaseGate
	config            m.Action
	enabled           atomic.Bool
	object            *object
	entered, canceled chan struct{}
	guards            []string
}
type lifeController struct {
	h         *harness
	prep      map[string]*preparation
	stops     map[string]*stopping
	mu        sync.Mutex
	violation error
}
type prepContextKey struct{}

func newLifeController(h *harness, actions []m.Action) *lifeController {
	l := &lifeController{h: h, prep: map[string]*preparation{}, stops: map[string]*stopping{}}
	for _, a := range actions {
		switch a.Kind {
		case "start-apply":
			l.prep[a.Actor] = &preparation{gate: newRelease(), config: a, entered: make(chan struct{}), canceled: make(chan struct{}), readEntered: make(chan struct{}), workExited: make(chan struct{}), childDone: make(chan struct{}), done: make(chan struct{})}
		case "arm-stop":
			l.stops[a.Actor] = &stopping{gate: newRelease(), config: a, entered: make(chan struct{}), canceled: make(chan struct{})}
			if a.Target == "error" {
				h.expectStopFailure = true
			}
		}
	}
	for _, a := range actions {
		if a.Kind == "guard-stop" {
			l.stops[a.Actor].guards = append(l.stops[a.Actor].guards, a.Tree)
		}
	}
	if len(l.prep) > 0 {
		h.buildHook = func(c context.Context, o *object) error { return l.preparing(c, o, "build") }
		h.startHook = func(c context.Context, o *object) error { return l.preparing(c, o, "start") }
		h.readyHook = func(c context.Context, o *object) error { return l.preparing(c, o, "readiness") }
		h.readHook = l.read
	}
	if len(l.prep) > 0 || len(l.stops) > 0 {
		h.stopHook = l.stop
	}
	return l
}
func (l *lifeController) mark(err error) error {
	l.mu.Lock()
	if l.violation == nil {
		l.violation = err
	}
	l.mu.Unlock()
	return err
}
func (l *lifeController) observedError() error { l.mu.Lock(); defer l.mu.Unlock(); return l.violation }
func (l *lifeController) latest(tree string) *object {
	l.h.mu.Lock()
	defer l.h.mu.Unlock()
	for i := len(l.h.objects) - 1; i >= 0; i-- {
		if o := l.h.objects[i]; o.tree == tree {
			return o
		}
	}
	return nil
}
func (l *lifeController) preparing(c context.Context, o *object, stage string) error {
	for id, a := range l.prep {
		if !a.started.Load() {
			continue
		}
		if a.config.Target == "parallel" && a.childStarted.Load() && stage != "build" {
			select {
			case <-a.workExited:
			default:
				return l.mark(mismatch("preparation-join", "child callback work exited before Start", stage))
			}
		}
		wanted := a.config.Target
		if wanted == "parallel" {
			wanted = "build"
		}
		if a.config.Tree != o.tree || wanted != stage {
			continue
		}
		a.object = o
		g, err := l.h.f.Generation(c)
		if err != nil {
			return err
		}
		a.observed = g
		if err = l.h.record(nativeEvent{Kind: "prepare.enter", Actor: id, Tree: o.tree, Physical: o.key, Definition: o.definition.ID, Generation: &g}); err != nil {
			return err
		}
		if a.config.Target == "parallel" {
			childCtx := context.WithValue(c, prepContextKey{}, id)
			a.childStarted.Store(true)
			go func() {
				defer close(a.childDone)
				a.childErr = o.values["in"].Use(childCtx, func(v value) error { _, e := v.Read(childCtx); return e })
			}()
			if err = await(c, a.readEntered); err != nil {
				return err
			}
			close(a.entered)
			return nil
		}
		close(a.entered)
		return await(c, a.gate.release)
	}
	return nil
}
func (l *lifeController) read(c context.Context, o *object) error {
	id, _ := c.Value(prepContextKey{}).(string)
	if id == "" {
		return nil
	}
	a := l.prep[id]
	a.resource = o
	close(a.readEntered)
	<-c.Done()
	if err := l.h.record(nativeEvent{Kind: "prepare.child.cancel-observed", Actor: id, Tree: o.tree, Physical: o.key, Definition: o.definition.ID, Error: c.Err().Error()}); err != nil {
		close(a.canceled)
		close(a.workExited)
		return err
	}
	close(a.canceled)
	<-a.gate.release
	var b [1]byte
	_, err := o.file.ReadAt(b[:], 0)
	if err == nil {
		err = l.h.record(nativeEvent{Kind: "prepare.child.work-exited", Actor: id, Tree: o.tree, Physical: o.key, Definition: o.definition.ID})
	}
	close(a.workExited)
	return err
}
func (l *lifeController) stop(c context.Context, o *object) error {
	for _, a := range l.prep {
		if a.started.Load() && a.childStarted.Load() {
			select {
			case <-a.workExited:
			default:
				return l.mark(mismatch("preparation-resource", "entered work completed", o.tree))
			}
		}
	}
	for _, g := range l.stops {
		if !g.enabled.Load() {
			continue
		}
		for _, target := range g.guards {
			if target == o.tree {
				var b [1]byte
				_, err := g.object.file.ReadAt(b[:], 0)
				if g.object.closes.Load() != 1 || !errors.Is(err, os.ErrClosed) {
					return l.mark(mismatch("resource-order", "consumer native handle closed before dependency Stop", o.tree))
				}
			}
		}
		if o != g.object {
			continue
		}
		close(g.entered)
		if g.config.Target == "error" {
			return errInjected
		}
		select {
		case <-g.gate.release:
			return nil
		case <-c.Done():
			close(g.canceled)
			<-g.gate.release
			return nil
		}
	}
	return nil
}
func nativeFile(o *object, open bool) error {
	if o == nil {
		return mismatch("native-probe", "actual created object", nil)
	}
	var b [1]byte
	_, err := o.file.ReadAt(b[:], 0)
	if open {
		if err != nil || o.closes.Load() != 0 {
			return mismatch("native-open", o.key, err)
		}
	} else if !errors.Is(err, os.ErrClosed) || o.closes.Load() != 1 {
		return mismatch("native-closed", o.key, err)
	}
	return nil
}
func (l *lifeController) handle(ctx context.Context, a m.Action) (bool, error) {
	p := l.prep[a.Actor]
	g := l.stops[a.Actor]
	switch a.Kind {
	case "start-apply":
		p.before = project(l.h.f.Snapshot())
		p.expected, p.issues = l.h.model.Next(*a.Plan)
		p.sequence = l.h.f.Snapshot().EventSequence
		childCtx, cancel := context.WithCancel(ctx)
		p.cancel = cancel
		p.started.Store(true)
		go func() { defer close(p.done); p.generation, p.err = l.h.f.Apply(childCtx, l.h.plan(*a.Plan)) }()
	case "await-prepare":
		if err := await(ctx, p.entered); err != nil {
			return true, err
		}
		if p.observed != p.before.Generation {
			return true, mismatch("candidate-generation", p.before.Generation, p.observed)
		}
	case "await-prepare-cancel":
		if err := await(ctx, p.canceled); err != nil {
			return true, err
		}
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
	case "probe-prepared":
		if err := nativeFile(p.object, true); err != nil {
			return true, err
		}
		if p.config.Target == "parallel" {
			if err := nativeFile(p.resource, true); err != nil {
				return true, err
			}
		}
		if !reflect.DeepEqual(p.before, project(l.h.f.Snapshot())) {
			return true, mismatch("candidate-publication", p.before, project(l.h.f.Snapshot()))
		}
	case "cancel-apply":
		p.canceledByHost = true
		p.cancel()
	case "release-prepare":
		p.gate.open()
	case "join-apply":
		if err := await(ctx, p.done); err != nil {
			return true, err
		}
		if p.childStarted.Load() {
			if err := await(ctx, p.childDone); err != nil {
				return true, err
			}
			if p.childErr != nil {
				return true, p.childErr
			}
		}
		p.cancel()
		rejected := len(p.issues) > 0 || p.canceledByHost
		p.started.Store(false)
		if rejected != (p.err != nil) {
			return true, mismatch("prepared-acceptance", p.issues, p.err)
		}
		if rejected {
			if !reflect.DeepEqual(p.before, project(l.h.f.Snapshot())) {
				return true, mismatch("prepared-rollback", p.before, project(l.h.f.Snapshot()))
			}
			if p.canceledByHost && !errors.Is(p.err, context.Canceled) {
				return true, mismatch("canceled-prepare", context.Canceled, p.err)
			}
		} else {
			l.h.model.State = p.expected
		}
		if p.generation != l.h.model.State.Generation {
			return true, mismatch("prepared-generation", l.h.model.State.Generation, p.generation)
		}
		events := l.h.f.Events()
		if last := events[len(events)-1]; last.Sequence != p.sequence+1 || last.Generation != p.generation {
			return true, mismatch("prepared-event", p.sequence+1, last)
		}
		if err := l.h.f.Wait(ctx); err != nil {
			return true, err
		}
		if rejected {
			if err := nativeFile(p.object, false); err != nil {
				return true, err
			}
			if p.config.Target == "parallel" {
				if err := nativeFile(p.resource, false); err != nil {
					return true, err
				}
			}
		}
		if err := l.h.check(); err != nil {
			return true, err
		}
		if err := l.observedError(); err != nil {
			return true, err
		}
	case "arm-stop":
		g.object = l.latest(a.Tree)
		if g.object == nil {
			return true, mismatch("stop-target", a.Tree, nil)
		}
		g.enabled.Store(true)
		if a.Target == "error" {
			l.h.stopRetained = []*object{g.object}
			for _, tree := range g.guards {
				l.h.stopRetained = append(l.h.stopRetained, l.latest(tree))
			}
		}
	case "guard-stop": // Immutable guard declaration already installed before actors start.
	case "await-stop":
		if err := await(ctx, g.entered); err != nil {
			return true, err
		}
	case "await-stop-timeout":
		if err := await(ctx, g.canceled); err != nil {
			return true, err
		}
	case "release-stop":
		g.gate.open()
	case "await-file-closed":
		o := l.latest(a.Tree)
		if o == nil {
			return true, mismatch("close-target", a.Tree, nil)
		}
		if err := await(ctx, o.closed); err != nil {
			return true, err
		}
		if err := nativeFile(o, false); err != nil {
			return true, err
		}
	case "probe-file":
		if err := nativeFile(l.latest(a.Tree), a.Target == "open"); err != nil {
			return true, err
		}
	case "wait-stop-timeout":
		err := l.h.f.Wait(ctx)
		if !errors.Is(err, forest.ErrDrainTimeout) {
			return true, mismatch("stop-timeout", forest.ErrDrainTimeout, err)
		}
	case "wait-stop-error":
		err := l.h.f.Wait(ctx)
		if !errors.Is(err, errInjected) {
			return true, mismatch("stop-error", errInjected, err)
		}
		var detail *forest.Error
		if !errors.As(err, &detail) || detail.Stage != "stop" || string(detail.Tree) != g.config.Tree {
			return true, mismatch("stop-error-context", g.config.Tree, err)
		}
		if err := nativeFile(g.object, true); err != nil {
			return true, err
		}
		for _, tree := range g.guards {
			if err := nativeFile(l.latest(tree), true); err != nil {
				return true, err
			}
		}
	default:
		return false, nil
	}
	return true, nil
}
func (l *lifeController) cleanup(ctx context.Context) error {
	for _, p := range l.prep {
		if p.started.Load() {
			p.gate.open()
			p.cancel()
		}
	}
	for _, g := range l.stops {
		g.gate.open()
	}
	for _, p := range l.prep {
		if p.started.Load() {
			if err := await(ctx, p.done); err != nil {
				return err
			}
			if p.childStarted.Load() {
				if err := await(ctx, p.childDone); err != nil {
					return err
				}
			}
			p.started.Store(false)
		}
	}
	return l.observedError()
}

func lifeKind(kind string) bool {
	switch kind {
	case "start-apply", "await-prepare", "await-prepare-cancel", "probe-prepared", "cancel-apply", "release-prepare", "join-apply", "arm-stop", "guard-stop", "await-stop", "await-stop-timeout", "release-stop", "await-file-closed", "probe-file", "wait-stop-timeout", "wait-stop-error":
		return true
	}
	return false
}

func validateLifeActions(actions []m.Action) error {
	type stage struct {
		started, awaited, released, joined bool
		target                             string
	}
	prep, stop := map[string]stage{}, map[string]stage{}
	pending := ""
	for _, a := range actions {
		switch a.Kind {
		case "start-apply":
			if _, ok := prep[a.Actor]; ok || pending != "" || a.Plan == nil || a.Tree == "" {
				return fmt.Errorf("invalid prepared writer")
			}
			switch a.Target {
			case "build", "start", "readiness", "parallel":
			default:
				return fmt.Errorf("invalid preparation stage")
			}
			prep[a.Actor] = stage{started: true, target: a.Target}
			pending = a.Actor
		case "await-prepare":
			p, ok := prep[a.Actor]
			if !ok || p.released {
				return fmt.Errorf("missing preparation")
			}
			p.awaited = true
			prep[a.Actor] = p
		case "await-prepare-cancel":
			p := prep[a.Actor]
			if !p.awaited || p.target != "parallel" || p.released {
				return fmt.Errorf("missing parallel preparation")
			}
		case "probe-prepared", "cancel-apply":
			p := prep[a.Actor]
			if !p.awaited || p.released {
				return fmt.Errorf("missing held preparation")
			}
		case "release-prepare":
			p := prep[a.Actor]
			if !p.awaited || p.released {
				return fmt.Errorf("invalid preparation release")
			}
			p.released = true
			prep[a.Actor] = p
		case "join-apply":
			p := prep[a.Actor]
			if !p.released || p.joined {
				return fmt.Errorf("invalid prepared join")
			}
			p.joined = true
			prep[a.Actor] = p
			pending = ""
		case "arm-stop":
			if _, ok := stop[a.Actor]; ok || a.Tree == "" {
				return fmt.Errorf("invalid Stop actor")
			}
			stop[a.Actor] = stage{started: true, target: a.Target}
		case "guard-stop":
			if !stop[a.Actor].started || a.Tree == "" {
				return fmt.Errorf("missing Stop owner")
			}
		case "await-stop":
			s, ok := stop[a.Actor]
			if !ok || s.released {
				return fmt.Errorf("missing held Stop")
			}
			s.awaited = true
			stop[a.Actor] = s
		case "await-stop-timeout", "wait-stop-timeout", "wait-stop-error":
			if !stop[a.Actor].awaited {
				return fmt.Errorf("missing Stop entry")
			}
		case "release-stop":
			s := stop[a.Actor]
			if !s.awaited || s.released {
				return fmt.Errorf("invalid Stop release")
			}
			s.released = true
			stop[a.Actor] = s
		default:
			if pending != "" && (a.Kind == "apply" || a.Kind == "race" || a.Kind == "prepared-race" || a.Kind == "start-call") {
				return fmt.Errorf("unsupported mutation while candidate held")
			}
		}
	}
	for _, p := range prep {
		if !p.joined {
			return fmt.Errorf("unjoined preparation")
		}
	}
	for _, s := range stop {
		if s.target != "error" && !s.released {
			return fmt.Errorf("unreleased Stop")
		}
	}
	return nil
}
