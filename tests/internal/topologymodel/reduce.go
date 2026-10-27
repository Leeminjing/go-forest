package topologymodel

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

func Clone(t Trace) Trace {
	out := t
	out.Steps = append([]Step(nil), t.Steps...)
	for i := range out.Steps {
		out.Steps[i].Plan.Ops = append([]Op(nil), t.Steps[i].Plan.Ops...)
	}
	out.Actions = append([]Action(nil), t.Actions...)
	for i, a := range out.Actions {
		if a.Plan != nil {
			p := *a.Plan
			p.Ops = append([]Op(nil), p.Ops...)
			out.Actions[i].Plan = &p
		}
		out.Actions[i].Plans = append([]Plan(nil), a.Plans...)
		for j := range out.Actions[i].Plans {
			out.Actions[i].Plans[j].Ops = append([]Op(nil), a.Plans[j].Ops...)
		}
	}
	return out
}

// Rebase only expands explicit command intent, never actual Forest results.
func Rebase(t Trace) (Trace, error) {
	out := Clone(t)
	model := New(t.Catalog)
	versions := map[string]uint64{}
	resolve := func(p *Plan, b BaseIntent) error {
		switch b.Kind {
		case "current":
			p.Base = model.State.Generation
		case "stale":
			if model.State.Generation >= b.Value {
				p.Base = model.State.Generation - b.Value
			} else {
				p.Base = model.State.Generation + 1
			}
		case "absolute":
			p.Base = b.Value
		case "step":
			v, ok := versions[b.Step]
			if !ok {
				return fmt.Errorf("missing successful version anchor %q", b.Step)
			}
			p.Base = v
		case "":
		default:
			return fmt.Errorf("unknown version intent %q", b.Kind)
		}
		return nil
	}
	for i := range out.Steps {
		s := &out.Steps[i]
		if err := resolve(&s.Plan, s.Base); err != nil {
			return Trace{}, err
		}
		next, issues := model.Next(s.Plan)
		model.State = next
		if len(issues) == 0 {
			versions[s.ID] = next.Generation
		}
	}
	pending := map[string]Plan{}
	canceled := map[string]bool{}
	for i := range out.Actions {
		a := &out.Actions[i]
		if a.Plan != nil {
			if err := resolve(a.Plan, a.Base); err != nil {
				return Trace{}, err
			}
			if a.Kind == "start-apply" {
				pending[a.Actor] = *a.Plan
			} else {
				next, issues := model.Next(*a.Plan)
				model.State = next
				if len(issues) == 0 {
					versions[a.ID] = next.Generation
				}
			}
		}
		if len(a.Plans) > 0 {
			for j := range a.Plans {
				if err := resolve(&a.Plans[j], a.Base); err != nil {
					return Trace{}, err
				}
			}
			next, _ := model.Next(a.Plans[0])
			model.State = next
		}
		if a.Kind == "cancel-apply" {
			canceled[a.Actor] = true
		}
		if a.Kind == "join-apply" {
			p, ok := pending[a.Actor]
			if !ok {
				return Trace{}, fmt.Errorf("missing prepared actor")
			}
			if !canceled[a.Actor] {
				next, issues := model.Next(p)
				model.State = next
				if len(issues) == 0 {
					versions[a.ID] = next.Generation
				}
			}
			delete(pending, a.Actor)
		}
	}
	return out, nil
}

type Reduction struct {
	Trace    Trace
	Minimal  bool
	Attempts int
}

// Reduce is bounded delta debugging followed by rule-relative 1-minimal passes.
// The caller supplies an isolated, same-fingerprint native replay predicate.
func Reduce(ctx context.Context, input Trace, budget int, sameFailure func(Trace) bool) Reduction {
	result := Reduction{Trace: Clone(input)}
	exhausted := false
	try := func(candidate Trace) bool {
		if ctx.Err() != nil || result.Attempts >= budget {
			exhausted = true
			return false
		}
		rebased, err := Rebase(candidate)
		if err != nil {
			return false
		}
		result.Attempts++
		if !sameFailure(rebased) {
			return false
		}
		result.Trace = rebased
		return true
	}
	// Large blocks first; later exhaustive passes prove single-removal minimality.
	for chunk := len(result.Trace.Steps) / 2; chunk > 0 && !exhausted; chunk /= 2 {
		for at := 0; at+chunk <= len(result.Trace.Steps) && !exhausted; {
			candidate := Clone(result.Trace)
			candidate.Steps = append(candidate.Steps[:at], candidate.Steps[at+chunk:]...)
			if !try(candidate) {
				at++
			}
		}
	}
	for !exhausted {
		changed := false
		for i := 0; i < len(result.Trace.Steps) && !exhausted; i++ {
			candidate := Clone(result.Trace)
			candidate.Steps = append(candidate.Steps[:i], candidate.Steps[i+1:]...)
			if try(candidate) {
				changed = true
				break
			}
		}
		if changed {
			continue
		}
		for i, s := range result.Trace.Steps {
			for j := range s.Plan.Ops {
				candidate := Clone(result.Trace)
				candidate.Steps[i].Plan.Ops = append(candidate.Steps[i].Plan.Ops[:j], candidate.Steps[i].Plan.Ops[j+1:]...)
				if try(candidate) {
					changed = true
					break
				}
				if exhausted {
					break
				}
			}
			if changed || exhausted {
				break
			}
		}
		if changed {
			continue
		}
		for i := range result.Trace.Actions {
			candidate := Clone(result.Trace)
			candidate.Actions = append(candidate.Actions[:i], candidate.Actions[i+1:]...)
			if try(candidate) {
				changed = true
				break
			}
			if exhausted {
				break
			}
		}
		if changed {
			continue
		}
		for i, s := range result.Trace.Steps {
			for j, o := range s.Plan.Ops {
				if o.KeepChildren {
					candidate := Clone(result.Trace)
					candidate.Steps[i].Plan.Ops[j].KeepChildren = false
					if try(candidate) {
						changed = true
						break
					}
				}
				ids := make([]string, 0, len(input.Catalog))
				catalog := New(input.Catalog).Catalog
				current, exists := catalog[o.Definition]
				if exists {
					for _, d := range input.Catalog {
						a, b := current, d
						a.ID, a.Version = "", ""
						b.ID, b.Version = "", ""
						if reflect.DeepEqual(a, b) && (d.Version < current.Version || (d.Version == current.Version && d.ID < current.ID)) {
							ids = append(ids, d.ID)
						}
					}
				}
				sort.Strings(ids)
				for _, id := range ids {
					candidate := Clone(result.Trace)
					candidate.Steps[i].Plan.Ops[j].Definition = id
					if try(candidate) {
						changed = true
						break
					}
					if exhausted {
						break
					}
				}
				if changed || exhausted {
					break
				}
			}
			if changed || exhausted {
				break
			}
		}
		if changed {
			continue
		}
		if !exhausted {
			result.Minimal = true
		}
		break
	}
	return result
}
