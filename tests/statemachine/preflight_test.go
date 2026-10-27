package statemachine

import (
	"fmt"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

// Invalid ChangePlans are legitimate inputs; impossible actor schedules are not.
func validateTrace(trace m.Trace) error {
	if trace.Format != m.FormatVersion {
		return fmt.Errorf("unsupported format")
	}
	if err := validateActions(trace.Actions); err != nil {
		return err
	}
	if err := validateLifeTrace(trace); err != nil {
		return err
	}
	model := m.New(trace.Catalog)
	for _, s := range trace.Steps {
		next, _ := model.Next(s.Plan)
		model.State = next
	}
	type held struct {
		state            m.State
		released, joined bool
	}
	actors := map[string]held{}
	pendingOld := func() bool {
		for _, a := range actors {
			if a.released || a.joined {
				continue
			}
			for id, n := range a.state.Nodes {
				if now, ok := model.State.Nodes[id]; !ok || now.Instance != n.Instance {
					return true
				}
			}
		}
		return false
	}
	anyHeld := func() bool {
		for _, a := range actors {
			if !a.released && !a.joined {
				return true
			}
		}
		return false
	}
	for _, a := range trace.Actions {
		switch a.Kind {
		case "start-call":
			n, ok := model.State.Nodes[a.Tree]
			if !ok {
				return fmt.Errorf("missing called tree")
			}
			d := model.Catalog[n.Definition]
			cap, _ := d.Cap("inspect")
			if _, ok := d.Port(a.Port); !ok || !model.Accessible(model.State, "", a.Tree, cap, false) || len(m.Ordered(model.State, a.Tree, a.Port)) == 0 {
				return fmt.Errorf("unavailable called port")
			}
			actors[a.Actor] = held{state: model.State.Copy()}
		case "release-call":
			v := actors[a.Actor]
			v.released = true
			actors[a.Actor] = v
		case "join-call":
			v := actors[a.Actor]
			v.joined = true
			actors[a.Actor] = v
		case "apply":
			next, _ := model.Next(*a.Plan)
			model.State = next
		case "race", "prepared-race":
			for _, p := range a.Plans {
				if _, issues := model.Next(p); len(issues) > 0 || len(p.Ops) == 0 {
					return fmt.Errorf("invalid write contender")
				}
			}
			if a.Kind == "race" {
				model.State.Generation++
			} else {
				next, _ := model.Next(a.Plans[0])
				model.State = next
			}
		case "wait-pending":
			if !pendingOld() {
				return fmt.Errorf("no retired graph held by a live call")
			}
		case "wait-current":
			if pendingOld() {
				return fmt.Errorf("Wait may await retired instances")
			}
		case "close-held":
			if !anyHeld() {
				return fmt.Errorf("no live scope for close")
			}
		}
	}
	return nil
}
