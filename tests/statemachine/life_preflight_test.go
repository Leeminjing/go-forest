package statemachine

import (
	"fmt"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func validateLifeTrace(trace m.Trace) error {
	if trace.DrainMillis < 0 || trace.DrainMillis > 1000 {
		return fmt.Errorf("unsupported drain budget")
	}
	seen := map[string]bool{}
	catalog := map[string]m.Definition{}
	for _, d := range trace.Catalog {
		if d.ID == "" || d.Version == "" || seen[d.ID] {
			return fmt.Errorf("invalid native definition catalog")
		}
		seen[d.ID] = true
		catalog[d.ID] = d
		for _, p := range d.Ports {
			if p.Type != "value" && p.Type != "other" {
				return fmt.Errorf("unsupported native interface")
			}
			if p.Phase != m.Runtime && p.Phase != m.Construction && p.Phase != m.Startup {
				return fmt.Errorf("unknown phase")
			}
		}
		for _, c := range d.Caps {
			if c.Type != "value" && c.Type != "other" && c.Type != "inspect" {
				return fmt.Errorf("unknown native capability")
			}
		}
	}
	model := m.New(trace.Catalog)
	seen = map[string]bool{}
	for _, s := range trace.Steps {
		if s.ID == "" || seen[s.ID] {
			return fmt.Errorf("duplicate/empty step ID")
		}
		seen[s.ID] = true
		next, _ := model.Next(s.Plan)
		model.State = next
	}
	if trace.TestFault != "" && (trace.TestFault != "swap-resolution" || !seen[trace.FaultStep]) {
		return fmt.Errorf("invalid controlled selftest fault")
	}
	pending := map[string]m.Plan{}
	canceled := map[string]bool{}
	stopState := map[string]m.State{}
	stopTree := map[string]string{}
	guards := map[string][]string{}
	for _, a := range trace.Actions {
		switch a.Kind {
		case "apply":
			next, _ := model.Next(*a.Plan)
			model.State = next
		case "race", "prepared-race":
			if len(a.Plans) > 0 {
				next, _ := model.Next(a.Plans[0])
				model.State = next
			}
		case "start-apply":
			plain := append([]m.Definition(nil), trace.Catalog...)
			for i := range plain {
				plain[i].Fail = ""
			}
			temporary := m.New(plain)
			temporary.State = model.State.Copy()
			candidate, issues := temporary.Next(*a.Plan)
			if len(issues) > 0 {
				return fmt.Errorf("candidate cannot reach its controlled hook: %v", issues)
			}
			n, ok := candidate.Nodes[a.Tree]
			old, existed := model.State.Nodes[a.Tree]
			if !ok || (existed && n.Instance == old.Instance) {
				return fmt.Errorf("missing rebuilt preparation target")
			}
			d := catalog[n.Definition]
			if (d.Fail == "build" && a.Target != "build") || (d.Fail == "start" && a.Target == "readiness") {
				return fmt.Errorf("failure precedes awaited hook")
			}
			if a.Target == "parallel" {
				p, ok := d.Port("in")
				if !ok || p.Phase != m.Construction || len(m.Ordered(candidate, a.Tree, "in")) != 1 {
					return fmt.Errorf("parallel probe requires one real construction dependency")
				}
			}
			pending[a.Actor] = *a.Plan
		case "cancel-apply":
			canceled[a.Actor] = true
		case "join-apply":
			if !canceled[a.Actor] {
				next, _ := model.Next(pending[a.Actor])
				model.State = next
			}
			delete(pending, a.Actor)
		case "arm-stop":
			if _, ok := model.State.Nodes[a.Tree]; !ok {
				return fmt.Errorf("missing Stop target")
			}
			if a.Target != "hold" && a.Target != "timeout" && a.Target != "error" {
				return fmt.Errorf("unsupported Stop mode")
			}
			if a.Target == "timeout" && trace.DrainMillis == 0 {
				return fmt.Errorf("timeout scenario needs explicit short native budget")
			}
			stopState[a.Actor] = model.State.Copy()
			stopTree[a.Actor] = a.Tree
		case "guard-stop":
			graph := stopState[a.Actor]
			resources := model.Resources(graph, stopTree[a.Actor])
			if !resources[a.Tree] {
				return fmt.Errorf("guard lacks declared lifecycle/ownership relation")
			}
			guards[a.Actor] = append(guards[a.Actor], a.Tree)
		case "await-stop":
			if _, ok := model.State.Nodes[stopTree[a.Actor]]; ok {
				return fmt.Errorf("consumer not retired")
			}
			for _, id := range guards[a.Actor] {
				if _, ok := model.State.Nodes[id]; ok {
					return fmt.Errorf("active graph, rather than consumer, still owns resource")
				}
			}
		}
	}
	return nil
}
