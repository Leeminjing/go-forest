package statemachine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

var errInjected = errors.New("controlled fixture failure")

func isUnavailable(err error) bool { return errors.Is(err, forest.ErrBindingUnavailable) }

type failure struct {
	Role             string
	Rule, Detail     string
	Expected, Actual any
}

func withRole(err error, role string) error {
	var f *failure
	if errors.As(err, &f) {
		f.Role = role
	}
	return err
}

func (e *failure) Error() string { return e.Rule + ": " + e.Detail }
func mismatch(rule string, a, b any) error {
	return &failure{Rule: rule, Detail: fmt.Sprintf("expected=%+v actual=%+v", a, b), Expected: a, Actual: b}
}

type harness struct {
	cleanupFailure  error
	stopRetained    []*object
	nativeInstances map[uint64]*object
	stopOwners      map[*object]*object
	hooks
	t                                 *testing.T
	f                                 *forest.Forest
	model                             *m.Model
	dir                               string
	instances                         map[uint64]string
	physical                          map[uint64]string
	lastApplyErr                      error
	ctx                               context.Context
	noWait                            bool
	testFault, currentStep, faultStep string
	expectStopFailure                 bool
}

func newHarness(t *testing.T, defs []m.Definition, ids []string, options ...forest.Options) *harness {
	t.Helper()
	option := forest.Options{EventCapacity: 2048}
	if len(options) > 0 {
		option = options[0]
		option.EventCapacity = 2048
	}
	dir := ""
	if root := os.Getenv("FOREST_MODEL_RESOURCE_ROOT"); root != "" {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
		var err error
		dir, err = os.MkdirTemp(root, "forest-")
		if err != nil {
			t.Fatal(err)
		}
	} else {
		dir = t.TempDir()
	}
	h := &harness{t: t, f: forest.New(option), model: m.New(defs), dir: dir, instances: map[uint64]string{}, physical: map[uint64]string{}}
	h.ctx = context.Background()
	h.nativeInstances = map[uint64]*object{}
	h.stopOwners = map[*object]*object{}
	for _, id := range ids {
		for _, d := range defs {
			if err := h.f.Register(h.definition(id, d)); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() {
		problem := func(err error) {
			if h.cleanupFailure == nil {
				h.cleanupFailure = err
			}
			if os.Getenv("FOREST_MODEL_WORKER_INPUT") == "" {
				t.Error(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		h.installOwners(h.model.State, m.State{Nodes: map[string]m.Node{}})
		err := h.f.Close(ctx)
		if h.expectStopFailure {
			if !errors.Is(err, errInjected) {
				problem(mismatch("failed-stop-cause", errInjected, err))
			}
			for i, o := range h.stopRetained {
				if err := nativeFile(o, true); err != nil {
					problem(err)
				}
				wanted := int32(0)
				if i == 0 {
					wanted = 1
				}
				if o.stops.Load() != wanted {
					problem(withRole(mismatch("failed-stop-retention", wanted, o.stops.Load()), o.tree))
				}
			}
			return
		}
		if err != nil {
			problem(err)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, o := range h.objects {
			if o.closes.Load() != 1 || o.stops.Load() != 1 {
				problem(withRole(mismatch("native-exact-close", []int32{1, 1}, []int32{o.stops.Load(), o.closes.Load()}), o.tree+"/"+o.definition.ID))
			}
		}
	})
	return h
}
func (h *harness) plan(p m.Plan) forest.ChangePlan {
	out := forest.ChangePlan{BaseGeneration: p.Base}
	for _, op := range p.Ops {
		b := forest.Link{ID: op.Binding.ID, Consumer: forest.TreeID(op.Binding.Consumer), Port: forest.PortID(op.Binding.Port), Provider: forest.TreeID(op.Binding.Provider), Capability: forest.CapabilityID(op.Binding.Capability)}
		x := forest.Operation{Kind: forest.OperationKind(op.Kind), Tree: forest.TreeID(op.Tree), Parent: forest.TreeID(op.Parent), Binding: b, KeepChildren: op.KeepChildren}
		if op.Definition != "" {
			d, ok := h.model.Catalog[op.Definition]
			if ok {
				x.Definition = definitionRef(op.Tree, d)
			} else {
				x.Definition = forest.DefinitionRef{TypeID: "missing", Version: "missing"}
			}
		}
		out.Operations = append(out.Operations, x)
	}
	return out
}

type topNode struct {
	Parent, Instance, Type, Version string
	Ports                           []forest.PortDescriptor
	Caps                            []forest.CapabilityDescriptor
}
type projection struct {
	Generation uint64
	Nodes      map[string]topNode
	Bindings   map[string]forest.Binding
}

func project(s forest.Snapshot) projection {
	p := projection{Generation: s.Generation, Nodes: map[string]topNode{}, Bindings: map[string]forest.Binding{}}
	for id, n := range s.Trees {
		p.Nodes[string(id)] = topNode{string(n.ParentID), n.InstanceID, n.TypeID, n.Version, n.Ports, n.Capabilities}
	}
	for id, b := range s.Links {
		p.Bindings[id] = b.Binding
	}
	for id, b := range s.NestBindings {
		p.Bindings[id] = b.Binding
	}
	return p
}
func (h *harness) step(p m.Plan) {
	h.t.Helper()
	if err := h.apply(p); err != nil {
		h.t.Fatal(err)
	}
}
func (h *harness) apply(p m.Plan) error {
	oldState := h.model.State.Copy()
	expected, issues := h.model.Next(p)
	if len(issues) == 0 {
		h.installOwners(h.model.State, expected)
	}
	before := project(h.f.Snapshot())
	sequence := h.f.Snapshot().EventSequence
	ctx, cancel := context.WithTimeout(h.ctx, 3*time.Second)
	defer cancel()
	generation, err := h.f.Apply(ctx, h.plan(p))
	h.lastApplyErr = err
	if (err == nil) != (len(issues) == 0) {
		return mismatch("acceptance", issues, err)
	}
	if generation != expected.Generation {
		return mismatch("generation", expected.Generation, generation)
	}
	after := h.f.Snapshot()
	if len(issues) > 0 && !reflect.DeepEqual(before, project(after)) {
		return mismatch("rollback-topology", before, project(after))
	}
	if err != nil && p.Base != h.model.State.Generation && !errors.Is(err, forest.ErrGenerationConflict) {
		return mismatch("conflict-error", forest.ErrGenerationConflict, err)
	}
	events := h.f.Events()
	last := events[len(events)-1]
	result := "success"
	if err != nil {
		result = "failed"
		if errors.Is(err, forest.ErrGenerationConflict) {
			result = "conflict"
		}
	}
	if last.Sequence != sequence+1 || last.Generation != generation || last.Result != result {
		return mismatch("event", result, last)
	}
	h.model.State = expected
	if err := h.check(); err != nil {
		return err
	}
	if len(issues) == 0 {
		if err := h.checkLifecycleOrder(oldState, expected); err != nil {
			return err
		}
	}
	if h.noWait {
		return nil
	}
	return h.f.Wait(ctx)
}
func (h *harness) check() error {
	s := h.f.Snapshot()
	expected := h.model.State
	if s.Generation != expected.Generation || len(s.Trees) != len(expected.Nodes) {
		return mismatch("nodes", expected, s)
	}
	actualBindings := project(s).Bindings
	if len(actualBindings) != len(expected.Bindings) {
		return mismatch("binding-count", expected.Bindings, actualBindings)
	}
	for id, b := range expected.Bindings {
		got := actualBindings[id]
		if got.ID != b.ID || string(got.Consumer) != b.Consumer || string(got.Port) != b.Port || string(got.Provider) != b.Provider || string(got.Capability) != b.Capability || got.Local != b.Local || got.Order != b.Order {
			return mismatch("binding", b, got)
		}
	}
	used := map[string]uint64{}
	for id, n := range expected.Nodes {
		got, ok := s.Trees[forest.TreeID(id)]
		if !ok {
			return mismatch("missing-tree", id, s.Trees)
		}
		d := h.model.Catalog[n.Definition]
		ref := definitionRef(id, d)
		if string(got.ParentID) != n.Parent || got.TypeID != ref.TypeID || got.Version != ref.Version {
			return mismatch("tree-declaration", n, got)
		}
		declared := h.definition(id, d)
		if !reflect.DeepEqual(got.Ports, declared.Ports) || !reflect.DeepEqual(got.Capabilities, declared.Capabilities) {
			return mismatch("descriptors", declared, got)
		}
		if prior, ok := h.instances[n.Instance]; ok && prior != got.InstanceID {
			return mismatch("retained-instance", prior, got.InstanceID)
		}
		for token, physical := range h.instances {
			if physical == got.InstanceID && token != n.Instance {
				return mismatch("reused-instance", n.Instance, token)
			}
		}
		if prior, ok := used[got.InstanceID]; ok && prior != n.Instance {
			return mismatch("shared-instance", n.Instance, prior)
		}
		used[got.InstanceID] = n.Instance
		h.instances[n.Instance] = got.InstanceID
		if h.nativeInstances[n.Instance] == nil {
			h.mu.Lock()
			for i := len(h.objects) - 1; i >= 0; i-- {
				o := h.objects[i]
				if o.tree == id && o.definition.ID == n.Definition && o.ready.Load() {
					h.nativeInstances[n.Instance] = o
					break
				}
			}
			h.mu.Unlock()
			if h.nativeInstances[n.Instance] == nil {
				return mismatch("missing-created-instance", n, nil)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, id := range m.IDs(expected) {
		n := expected.Nodes[id]
		d := h.model.Catalog[n.Definition]
		cap, _ := d.Cap("inspect")
		err := h.f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[inspector](c, h.f, forest.TreeID(id), "inspect", func(i inspector) error {
				for _, p := range d.Ports {
					values, err := i.Inspect(c, p.ID)
					bs := m.Ordered(expected, id, p.ID)
					if len(bs) == 0 {
						if !isUnavailable(err) {
							return mismatch("optional-unavailable", forest.ErrBindingUnavailable, err)
						}
						continue
					}
					if err != nil {
						return err
					}
					if len(values) != len(bs) {
						return mismatch("resolution-count", bs, values)
					}
					if h.testFault == "swap-resolution" && h.currentStep == h.faultStep && len(values) > 1 {
						values[0], values[1] = values[1], values[0]
					}
					for k, b := range bs {
						provider := expected.Nodes[b.Provider]
						r := values[k]
						if r.Tree != b.Provider || r.Definition != provider.Definition || r.Generation != expected.Generation {
							return withRole(mismatch("resolution", b, r), id+"."+p.ID+"/"+string(p.Phase)+"/"+fmt.Sprint(b.Local))
						}
						if key, ok := h.physical[provider.Instance]; ok && key != r.Physical {
							return mismatch("provider-physical", key, r.Physical)
						}
						h.physical[provider.Instance] = r.Physical
					}
				}
				return nil
			})
		})
		if !h.model.Accessible(expected, "", id, cap, false) {
			if !errors.Is(err, forest.ErrMissingCapability) {
				return mismatch("entry-visibility", forest.ErrMissingCapability, err)
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}
