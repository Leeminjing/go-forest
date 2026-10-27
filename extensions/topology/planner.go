package topology

import (
	"sort"

	"github.com/Leeminjing/go-forest/forest"
)

// Plan uses only the native inert operation data, never a Build function.
type Plan struct {
	BaseGeneration uint64             `json:"base_generation"`
	Operations     []forest.Operation `json:"operations"`
}

func makePlan(before, target Manifest, generation uint64, rebuild bool) Plan {
	p := Plan{BaseGeneration: generation, Operations: []forest.Operation{}}
	if rebuild {
		for _, t := range before.Trees {
			if t.ParentID == "" {
				p.Operations = append(p.Operations, forest.RemoveSubtree(t.ID))
			}
		}
	}
	trees := append([]Tree(nil), target.Trees...)
	parents := map[forest.TreeID]forest.TreeID{}
	depths := map[forest.TreeID]int{"": 0}
	for _, t := range trees {
		parents[t.ID] = t.ParentID
	}
	for _, t := range trees {
		var path []forest.TreeID
		id := t.ID
		for {
			if _, ok := depths[id]; ok {
				break
			}
			path = append(path, id)
			id = parents[id]
		}
		for i := len(path) - 1; i >= 0; i-- {
			depths[path[i]] = depths[parents[path[i]]] + 1
		}
	}
	sort.Slice(trees, func(i, j int) bool {
		if depths[trees[i].ID] != depths[trees[j].ID] {
			return depths[trees[i].ID] < depths[trees[j].ID]
		}
		return trees[i].ID < trees[j].ID
	})
	for _, t := range trees {
		if t.ParentID == "" {
			p.Operations = append(p.Operations, forest.AddRoot(t.ID, t.Definition))
		} else {
			p.Operations = append(p.Operations, forest.Nest(t.ParentID, t.ID, t.Definition))
		}
	}
	for _, b := range target.Bindings {
		l := forest.Link{ID: b.ID, Consumer: b.Consumer, Port: b.Port, Provider: b.Provider, Capability: b.Capability}
		if b.Kind == "nest" {
			p.Operations = append(p.Operations, forest.BindNest(l))
		} else {
			p.Operations = append(p.Operations, forest.Connect(l))
		}
	}
	return p
}
