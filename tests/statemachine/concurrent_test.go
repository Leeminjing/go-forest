package statemachine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

type callActor struct {
	graph                            []*object
	entered, canceled, release, done chan struct{}
	first, second                    []reading
	generation                       uint64
	err                              error
	released, joined                 bool
}

func await(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *harness) observe(ctx context.Context, tree, port string) (out []reading, err error) {
	err = forest.Use[inspector](ctx, h.f, forest.TreeID(tree), "inspect", func(i inspector) error { var e error; out, e = i.Inspect(ctx, port); return e })
	return
}
func (h *harness) held(values []reading) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range values {
		found := false
		for _, o := range h.objects {
			if o.key != v.Physical {
				continue
			}
			found = true
			var b [1]byte
			_, err := o.file.ReadAt(b[:], 0)
			if err != nil || o.closes.Load() != 0 || o.stops.Load() != 0 {
				return mismatch("premature-release", v, err)
			}
		}
		if !found {
			return mismatch("missing-native-probe", v, nil)
		}
	}
	return nil
}
func (h *harness) schedule(ctx context.Context, actions []m.Action) (err error) {
	if err = validateActions(actions); err != nil {
		return &failure{Rule: "invalid-schedule", Detail: err.Error()}
	}
	h.noWait = true
	defer func() { h.noWait = false }()
	life := newLifeController(h, actions)
	actors := map[string]*callActor{}
	defer func() {
		ids := make([]string, 0, len(actors))
		for id := range actors {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			a := actors[id]
			if !a.released {
				close(a.release)
				a.released = true
			}
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if e := life.cleanup(cleanup); e != nil && err == nil {
			err = e
		}
		for _, id := range ids {
			if e := await(cleanup, actors[id].done); e != nil && err == nil {
				err = e
			}
		}
	}()
	for index, action := range actions {
		h.currentStep = action.ID
		if h.currentStep == "" {
			h.currentStep = fmt.Sprintf("action-%d", index)
		}
		if err = h.record(nativeEvent{Kind: "action.begin", Actor: h.currentStep}); err != nil {
			return err
		}
		a := actors[action.Actor]
		switch action.Kind {
		case "apply":
			if err = h.apply(*action.Plan); err != nil {
				return err
			}
		case "start-call":
			a = &callActor{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}), generation: h.model.State.Generation}
			actors[action.Actor] = a
			for _, node := range h.model.State.Nodes {
				if o := h.nativeInstances[node.Instance]; o != nil {
					a.graph = append(a.graph, o)
				}
			}
			tree, port, actorID := action.Tree, action.Port, action.Actor
			go func(a *callActor) {
				defer close(a.done)
				a.err = h.f.Invoke(ctx, func(c context.Context) error {
					var e error
					a.first, e = h.observe(c, tree, port)
					if e != nil {
						return e
					}
					g, e := h.f.Generation(c)
					if e != nil {
						return e
					}
					if g != a.generation {
						return mismatch("scope-entry", a.generation, g)
					}
					if e = h.record(nativeEvent{Kind: "call.enter", Actor: actorID, Tree: tree, Generation: &g, Values: a.first}); e != nil {
						return e
					}
					close(a.entered)
					select {
					case <-a.release:
					case <-c.Done():
						if e = h.record(nativeEvent{Kind: "call.cancel-observed", Actor: actorID, Tree: tree, Generation: &g, Values: a.first, Error: c.Err().Error()}); e != nil {
							return e
						}
						close(a.canceled)
						<-a.release
					}
					a.second, e = h.observe(c, tree, port)
					if e != nil {
						return e
					}
					if !reflect.DeepEqual(a.first, a.second) {
						return mismatch("scope-pinned", a.first, a.second)
					}
					if e = h.record(nativeEvent{Kind: "call.return", Actor: actorID, Tree: tree, Generation: &g, Values: a.second}); e != nil {
						return e
					}
					return nil
				})
			}(a)
		case "await-call":
			select {
			case <-a.entered:
			case <-a.done:
				if a.err != nil {
					return a.err
				}
				return mismatch("missing-call-entry", action.Actor, nil)
			case <-ctx.Done():
				return ctx.Err()
			}
		case "probe-held":
			if err = h.held(a.first); err != nil {
				return err
			}
			for _, o := range a.graph {
				if e := nativeFile(o, true); e != nil {
					return e
				}
				if o.stops.Load() != 0 {
					return mismatch("pinned-graph-stop", o.key, o.stops.Load())
				}
			}
		case "cancel":
			expected := 0
			for _, other := range actors {
				select {
				case <-other.entered:
					if !other.joined && other.generation == a.generation {
						expected++
					}
				default:
				}
			}
			if n := h.f.CancelGeneration(a.generation); n != expected {
				return mismatch("cancel-count", expected, n)
			}
		case "await-cancel":
			if err = await(ctx, a.canceled); err != nil {
				return err
			}
		case "release-call":
			close(a.release)
			a.released = true
		case "join-call":
			if err = await(ctx, a.done); err != nil {
				return err
			}
			a.joined = true
			if a.err != nil {
				return a.err
			}
		case "check":
			if err = h.check(); err != nil {
				return err
			}
		case "wait-pending", "wait-current":
			short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			e := h.f.Wait(short)
			cancel()
			if action.Kind == "wait-pending" && !errors.Is(e, forest.ErrDrainTimeout) {
				return mismatch("wait-held-old-scope", forest.ErrDrainTimeout, e)
			}
			if action.Kind == "wait-current" && e != nil {
				return mismatch("wait-current-scope", nil, e)
			}
		case "wait":
			if err = h.f.Wait(ctx); err != nil {
				return err
			}
		case "race", "prepared-race":
			if err = h.writerRace(ctx, action.Plans, action.Kind == "prepared-race"); err != nil {
				return err
			}
		case "close-held":
			short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			e := h.f.Close(short)
			cancel()
			if !errors.Is(e, forest.ErrDrainTimeout) {
				return mismatch("close-held", forest.ErrDrainTimeout, e)
			}
			if e = h.f.Invoke(ctx, func(context.Context) error { return nil }); !errors.Is(e, forest.ErrClosed) {
				return mismatch("close-admission", forest.ErrClosed, e)
			}
		default:
			handled, e := life.handle(ctx, action)
			if e != nil {
				return e
			}
			if !handled {
				return &failure{Rule: "invalid-schedule", Detail: "unknown action: " + action.Kind}
			}
		}
		if err = h.record(nativeEvent{Kind: "action.end", Actor: h.currentStep}); err != nil {
			return err
		}
	}
	return nil
}

func validateActions(actions []m.Action) error {
	type state struct{ entered, released, joined, canceled bool }
	actors := map[string]state{}
	closed := false
	for _, a := range actions {
		if closed && (a.Kind == "apply" || a.Kind == "race" || a.Kind == "prepared-race" || a.Kind == "start-call" || a.Kind == "start-apply" || a.Kind == "check") {
			return fmt.Errorf("unsupported new work after Close")
		}
		if a.Kind == "close-held" {
			closed = true
		}
		s, ok := actors[a.Actor]
		switch a.Kind {
		case "start-call":
			if ok || a.Actor == "" || a.Tree == "" || a.Port == "" {
				return fmt.Errorf("invalid start %q", a.Actor)
			}
			actors[a.Actor] = state{}
		case "await-call":
			if !ok || s.joined {
				return fmt.Errorf("missing live actor %q", a.Actor)
			}
			s.entered = true
			actors[a.Actor] = s
		case "release-call":
			if !ok || !s.entered || s.released {
				return fmt.Errorf("invalid release %q", a.Actor)
			}
			s.released = true
			actors[a.Actor] = s
		case "join-call":
			if !ok || !s.released || s.joined {
				return fmt.Errorf("join without release %q", a.Actor)
			}
			s.joined = true
			actors[a.Actor] = s
		case "cancel":
			if !ok || !s.entered || s.released {
				return fmt.Errorf("invalid cancel %q", a.Actor)
			}
			s.canceled = true
			actors[a.Actor] = s
		case "await-cancel":
			if !ok || !s.canceled || s.released {
				return fmt.Errorf("missing cancellation %q", a.Actor)
			}
		case "probe-held":
			if !ok || !s.entered || s.released {
				return fmt.Errorf("invalid held probe %q", a.Actor)
			}
		case "apply":
			if a.Plan == nil {
				return errors.New("missing Apply plan")
			}
		case "race", "prepared-race":
			if len(a.Plans) != 2 {
				return errors.New("race requires two plans")
			}
		case "check", "wait", "wait-pending", "wait-current", "close-held":
		default:
			if !lifeKind(a.Kind) {
				return fmt.Errorf("unknown action %q", a.Kind)
			}
		}
	}
	for id, s := range actors {
		if !s.joined {
			return fmt.Errorf("actor not joined %q", id)
		}
	}
	return validateLifeActions(actions)
}

func (h *harness) writerRace(ctx context.Context, plans []m.Plan, prepared bool) error {
	next := make([]m.State, 2)
	for i, p := range plans {
		state, issues := h.model.Next(p)
		if len(issues) > 0 || len(p.Ops) == 0 {
			return &failure{Rule: "invalid-schedule", Detail: "race requires separately valid nonempty plans"}
		}
		next[i] = state
	}
	ctx, cancel := context.WithCancel(ctx)
	var joined sync.WaitGroup
	var releaseOnce sync.Once
	type result struct {
		index      int
		generation uint64
		err        error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	ready, release := make(chan struct{}), make(chan struct{})
	defer func() { cancel(); releaseOnce.Do(func() { close(release) }); joined.Wait(); h.buildHook = nil }()
	if prepared {
		target := plans[0].Ops[0].Tree
		h.buildHook = func(c context.Context, o *object) error {
			if o.tree == target {
				close(ready)
				return await(c, release)
			}
			return nil
		}
	}
	begin := []chan struct{}{make(chan struct{}), make(chan struct{})}
	arrived := []chan struct{}{make(chan struct{}), make(chan struct{})}
	launch := func(index int) {
		joined.Add(1)
		go func() {
			defer joined.Done()
			close(arrived[index])
			<-start
			e := h.record(nativeEvent{Kind: "writer.begin", Actor: fmt.Sprint(index)})
			close(begin[index])
			var g uint64
			if e == nil {
				g, e = h.f.Apply(ctx, h.plan(plans[index]))
			}
			end := nativeEvent{Kind: "writer.return", Actor: fmt.Sprint(index), Generation: &g}
			if e != nil {
				end.Error = e.Error()
			}
			if logErr := h.record(end); logErr != nil && e == nil {
				e = logErr
			}
			results <- result{index, g, e}
		}()
	}
	sequence := h.f.Snapshot().EventSequence
	launch(0)
	if prepared {
		close(start)
		if err := await(ctx, ready); err != nil {
			releaseOnce.Do(func() { close(release) })
			return err
		}
		launch(1)
		if err := await(ctx, begin[1]); err != nil {
			return err
		}
		releaseOnce.Do(func() { close(release) })
	} else {
		launch(1)
		if err := await(ctx, arrived[0]); err != nil {
			close(start)
			return err
		}
		if err := await(ctx, arrived[1]); err != nil {
			close(start)
			return err
		}
		close(start)
	}
	success, conflicts := 0, 0
	var winner result
	var unexpected error
	for range 2 {
		select {
		case r := <-results:
			if r.err == nil {
				success++
				winner = r
			} else if errors.Is(r.err, forest.ErrGenerationConflict) {
				conflicts++
				var detail *forest.GenerationConflict
				if !errors.As(r.err, &detail) || detail.Expected != plans[r.index].Base || detail.Actual != h.model.State.Generation+1 || r.generation != detail.Actual {
					unexpected = mismatch("conflict-detail", h.model.State.Generation+1, r)
				}
			} else {
				unexpected = r.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if unexpected != nil {
		return unexpected
	}
	if success != 1 || conflicts != 1 {
		return mismatch("lost-update", "one commit and one conflict", []int{success, conflicts})
	}
	if prepared && winner.index != 0 {
		return mismatch("prepared-writer-winner", 0, winner.index)
	}
	h.model.State = next[winner.index]
	if winner.generation != h.model.State.Generation || h.f.Snapshot().EventSequence != sequence+2 {
		return mismatch("writer-generation", h.model.State.Generation, winner.generation)
	}
	return h.check()
}
