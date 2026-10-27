package topologymodel

import (
	"fmt"
	"math/rand"
	"sort"
)

const FormatVersion = 1
const GeneratorVersion = "topology-v2"

type BaseIntent struct {
	Kind  string
	Value uint64
	Step  string
}
type Step struct {
	ID     string
	Base   BaseIntent
	Plan   Plan
	Expect *bool
}
type Action struct {
	ID                              string
	Kind, Actor, Tree, Port, Target string
	Plan                            *Plan
	Generation                      uint64
	Plans                           []Plan
	Base                            BaseIntent
}
type Trace struct {
	PlanBudget, NodeLimit int
	DrainMillis           int
	Name                  string
	Format                int
	Generator             string
	Seed                  int64
	Catalog               []Definition
	Steps                 []Step
	Actions               []Action
	TestFault             string
	FaultStep             string
}

func (t Trace) IDs() []string {
	ids := map[string]bool{}
	put := func(p Plan) {
		for _, o := range p.Ops {
			if o.Tree != "" {
				ids[o.Tree] = true
			}
			if o.Parent != "" {
				ids[o.Parent] = true
			}
			if o.Binding.Consumer != "" {
				ids[o.Binding.Consumer] = true
			}
			if o.Binding.Provider != "" {
				ids[o.Binding.Provider] = true
			}
		}
	}
	for _, s := range t.Steps {
		put(s.Plan)
	}
	for _, a := range t.Actions {
		for _, p := range a.Plans {
			put(p)
		}
		if a.Plan != nil {
			put(*a.Plan)
		}
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
func Generate(defs []Definition, seed int64, count, limit int) Trace {
	rng := rand.New(rand.NewSource(seed))
	model := New(defs)
	trace := Trace{Format: FormatVersion, Generator: GeneratorVersion, Seed: seed, Catalog: defs, PlanBudget: count, NodeLimit: limit}
	names := make([]string, limit)
	for i := range names {
		names[i] = fmt.Sprintf("n%02d", i)
	}
	for step := 0; step < count; step++ {
		wantValid := rng.Intn(10) < 6
		var plan Plan
		found := false
		for attempt := 0; attempt < 24; attempt++ {
			plan = randomPlan(rng, model, names)
			if step == 0 {
				plan = Plan{Ops: []Op{{Kind: "add-root", Tree: names[0], Definition: "provider-v1"}, {Kind: "add-root", Tree: names[1], Definition: "provider-next"}, {Kind: "add-root", Tree: names[2], Definition: "runtime-many"}, {Kind: "link", Binding: Binding{ID: "seed-a", Consumer: names[2], Port: "in", Provider: names[0], Capability: "value"}}, {Kind: "link", Binding: Binding{ID: "seed-b", Consumer: names[2], Port: "in", Provider: names[1], Capability: "value"}}}}
				wantValid = true
			}
			state, issues := model.Next(plan)
			kept := true
			for _, id := range names[:3] {
				if _, ok := state.Nodes[id]; !ok {
					kept = false
				}
			}
			if (len(issues) == 0) == wantValid && len(state.Nodes) <= limit && (len(issues) > 0 || kept) {
				found = true
				break
			}
		}
		if !found {
			plan = Plan{Base: model.State.Generation}
			if !wantValid {
				plan.Ops = []Op{{Kind: "remove", Tree: "absent"}}
			}
		}
		intent := BaseIntent{Kind: "current"}
		if plan.Base != model.State.Generation {
			intent = BaseIntent{Kind: "absolute", Value: plan.Base}
		}
		trace.Steps = append(trace.Steps, Step{ID: fmt.Sprintf("step-%04d", step), Base: intent, Plan: plan})
		next, _ := model.Next(plan)
		model.State = next
	}
	return trace
}
func randomPlan(rng *rand.Rand, model *Model, names []string) Plan {
	s := model.State
	p := Plan{Base: s.Generation}
	pick := func(vs []string) string {
		if len(vs) == 0 {
			return "absent"
		}
		return vs[rng.Intn(len(vs))]
	}
	ids := IDs(s)
	tree := pick(ids)
	fresh := pick(names)
	defs := make([]string, 0, len(model.Catalog))
	for id := range model.Catalog {
		defs = append(defs, id)
	}
	sort.Strings(defs)
	d := pick(defs)
	bindings := make([]string, 0, len(s.Bindings))
	for id := range s.Bindings {
		bindings = append(bindings, id)
	}
	sort.Strings(bindings)
	bID := pick(bindings)
	b := s.Bindings[bID]
	consumers := []string{}
	for _, id := range ids {
		if len(model.Catalog[s.Nodes[id].Definition].Ports) > 0 {
			consumers = append(consumers, id)
		}
	}
	consumer := pick(consumers)
	port := "in"
	if n, ok := s.Nodes[consumer]; ok {
		ps := model.Catalog[n.Definition].Ports
		if len(ps) > 0 {
			port = ps[rng.Intn(len(ps))].ID
		}
	}
	binding := Binding{ID: fmt.Sprintf("b%02d", rng.Intn(24)), Consumer: consumer, Port: port, Provider: pick(ids), Capability: "value"}
	switch rng.Intn(16) {
	case 0:
		p.Ops = []Op{{Kind: "add-root", Tree: fresh, Definition: d}}
	case 1:
		p.Ops = []Op{{Kind: "nest", Tree: fresh, Parent: tree, Definition: d}}
	case 2:
		p.Ops = []Op{{Kind: "move", Tree: tree, Parent: pick(ids)}}
	case 3:
		p.Ops = []Op{{Kind: "unnest", Tree: tree}}
	case 4, 5:
		kind := "link"
		if s.Reaches(binding.Consumer, binding.Provider) || s.Reaches(binding.Provider, binding.Consumer) {
			kind = "bind-nest"
		}
		if rng.Intn(4) == 0 {
			if kind == "link" {
				kind = "bind-nest"
			} else {
				kind = "link"
			}
		}
		p.Ops = []Op{{Kind: kind, Binding: binding}}
	case 6:
		b.Provider = pick(ids)
		p.Ops = []Op{{Kind: "rebind", Binding: b}}
	case 7:
		p.Ops = []Op{{Kind: "unlink", Binding: Binding{ID: bID}}}
	case 8:
		p.Ops = []Op{{Kind: "replace", Tree: tree, Definition: d, KeepChildren: rng.Intn(2) == 0}}
	case 9:
		p.Ops = []Op{{Kind: "remove", Tree: tree}}
	case 10:
		p.Ops = []Op{{Kind: "remove-subtree", Tree: tree}}
	case 11:
		p.Ops = []Op{{Kind: "add-root", Tree: fresh, Definition: "provider-v1"}, {Kind: "remove", Tree: fresh}}
	case 12: // Empty is a successful event, not a new generation.
	case 13:
		p.Base = s.Generation + 1
	case 14:
		p.Ops = []Op{{Kind: "replace", Tree: tree, Definition: d}, {Kind: "remove", Tree: pick(ids)}}
	case 15:
		p.Ops = []Op{{Kind: "move", Tree: tree, Parent: pick(ids)}, {Kind: "unlink", Binding: Binding{ID: bID}}, {Kind: "link", Binding: binding}}
	}
	return p
}
