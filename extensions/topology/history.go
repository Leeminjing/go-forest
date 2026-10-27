package topology

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/Leeminjing/go-forest/forest"
)

type TreeChange struct {
	Before Tree `json:"before"`
	After  Tree `json:"after"`
}
type BindingChange struct {
	Before Binding `json:"before"`
	After  Binding `json:"after"`
}
type Diff struct {
	AddedTrees      []Tree          `json:"added_trees"`
	RemovedTrees    []Tree          `json:"removed_trees"`
	ChangedTrees    []TreeChange    `json:"changed_trees"`
	AddedBindings   []Binding       `json:"added_bindings"`
	RemovedBindings []Binding       `json:"removed_bindings"`
	ChangedBindings []BindingChange `json:"changed_bindings"`
}

// Compare reports logical differences only, without allocating any resource.
func Compare(before, after Manifest) (Diff, error) {
	a, e := Normalize(before)
	if e != nil {
		return Diff{}, e
	}
	b, e := Normalize(after)
	if e != nil {
		return Diff{}, e
	}
	d := Diff{AddedTrees: []Tree{}, RemovedTrees: []Tree{}, ChangedTrees: []TreeChange{}, AddedBindings: []Binding{}, RemovedBindings: []Binding{}, ChangedBindings: []BindingChange{}}
	at, bt := map[forest.TreeID]Tree{}, map[forest.TreeID]Tree{}
	for _, t := range a.Trees {
		at[t.ID] = t
	}
	for _, t := range b.Trees {
		bt[t.ID] = t
		if old, ok := at[t.ID]; !ok {
			d.AddedTrees = append(d.AddedTrees, t)
		} else if old != t {
			d.ChangedTrees = append(d.ChangedTrees, TreeChange{old, t})
		}
	}
	for _, t := range a.Trees {
		if _, ok := bt[t.ID]; !ok {
			d.RemovedTrees = append(d.RemovedTrees, t)
		}
	}
	ab, bb := map[string]Binding{}, map[string]Binding{}
	for _, v := range a.Bindings {
		ab[v.ID] = v
	}
	for _, v := range b.Bindings {
		bb[v.ID] = v
		if old, ok := ab[v.ID]; !ok {
			d.AddedBindings = append(d.AddedBindings, v)
		} else if old != v {
			d.ChangedBindings = append(d.ChangedBindings, BindingChange{old, v})
		}
	}
	for _, v := range a.Bindings {
		if _, ok := bb[v.ID]; !ok {
			d.RemovedBindings = append(d.RemovedBindings, v)
		}
	}
	return d, nil
}

type Proposal struct {
	Expected       Position        `json:"expected"`
	Generation     uint64          `json:"generation"`
	BeforeDigest   string          `json:"before_digest"`
	Target         Manifest        `json:"target"`
	Mode           string          `json:"mode"`
	SourceRevision uint64          `json:"source_revision"`
	RebuildTrees   []forest.TreeID `json:"rebuild_trees"`
	Diff           Diff            `json:"diff"`
	Plan           Plan            `json:"plan"`
}

func (c *Controller) previewLocked(target Manifest, mode string, source uint64) (Proposal, error) {
	s := c.store
	if s.closed || s.poisoned {
		return Proposal{}, ErrPersistenceUnknown
	}
	if c.closed || c.closing || !c.ready {
		return Proposal{}, ErrAdmissionClosed
	}
	if s.state.inspection.Pending != nil {
		return Proposal{}, ErrReconciliationRequired
	}
	before, e := c.actual()
	if e != nil {
		c.shutAdmission()
		return Proposal{}, e
	}
	n, e := Normalize(target)
	if e != nil {
		return Proposal{}, e
	}
	if e = c.catalog.check(n); e != nil {
		return Proposal{}, e
	}
	diff, e := Compare(before, n)
	if e != nil {
		return Proposal{}, e
	}
	plan := makePlan(before, n, c.generation, true)
	d, _ := Digest(n)
	rebuild := []forest.TreeID{}
	if d == c.digest {
		plan = Plan{BaseGeneration: c.generation, Operations: []forest.Operation{}}
	} else {
		for _, t := range n.Trees {
			rebuild = append(rebuild, t.ID)
		}
	}
	return Proposal{Expected: s.state.inspection.Position, Generation: c.generation, BeforeDigest: c.digest, Target: n, Mode: mode, SourceRevision: source, RebuildTrees: rebuild, Diff: diff, Plan: plan}, nil
}
func (c *Controller) Preview(target Manifest) (Proposal, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	return c.previewLocked(target, "reconfigure", 0)
}
func (c *Controller) PreviewRollback(revision uint64) (Proposal, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.state.history {
		if v.ID == revision {
			return c.previewLocked(v.Manifest, "rollback", revision)
		}
	}
	return Proposal{}, ErrHistoryConflict
}
func (c *Controller) Reconfigure(ctx context.Context, request Request, proposal Proposal) (Receipt, error) {
	return c.commit(ctx, request, proposal, "reconfigure")
}
func (c *Controller) Rollback(ctx context.Context, request Request, proposal Proposal) (Receipt, error) {
	return c.commit(ctx, request, proposal, "rollback")
}
func (c *Controller) commit(ctx context.Context, request Request, proposal Proposal, mode string) (Receipt, error) {
	if e := c.freshContext(ctx); e != nil {
		return Receipt{}, e
	}
	if !validRequest(request) || proposal.Mode != mode || request.Expected != proposal.Expected {
		return Receipt{}, ErrInvalidManifest
	}
	if e := c.mu.lockContext(ctx); e != nil {
		return Receipt{}, e
	}
	defer c.mu.Unlock()
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned {
		return Receipt{}, ErrPersistenceUnknown
	}
	in := Intent{Request: request, SessionID: c.session, Mode: mode, Target: proposal.Target, SourceRevision: proposal.SourceRevision, BaseGeneration: proposal.Generation, BaseDigest: proposal.BeforeDigest, Plan: proposal.Plan}
	in.TargetDigest, _ = Digest(in.Target)
	h, e := intentHash(in)
	if e != nil {
		return Receipt{}, e
	}
	if r, ok, e := s.duplicate(request.ID, h); ok {
		r = c.cachedReceipt(r)
		return r, recordedError(r, e)
	}
	if e = s.checkLocked(proposal.Expected); e != nil {
		return Receipt{}, e
	}
	if mode == "rollback" {
		found := false
		for _, v := range s.state.history {
			if v.ID == proposal.SourceRevision && v.Digest == in.TargetDigest {
				found = true
			}
		}
		if !found {
			return Receipt{}, ErrHistoryConflict
		}
	} else if proposal.SourceRevision != 0 {
		return Receipt{}, ErrInvalidManifest
	}
	expected, e := c.previewLocked(proposal.Target, mode, proposal.SourceRevision)
	if e != nil {
		return Receipt{}, e
	}
	if !reflect.DeepEqual(expected, proposal) {
		return Receipt{}, ErrHistoryConflict
	}
	if c.cleanupRequired {
		if e = c.f.Wait(ctx); e != nil {
			return Receipt{}, e
		}
		c.cleanupRequired = false
	}
	c.shutAdmission()
	return c.applyLocked(ctx, in, h)
}
func intentHash(in Intent) (string, error) {
	data, e := json.Marshal(struct {
		Request    Request
		Mode       string
		Target     Manifest
		Source     uint64
		Generation uint64
		Before     string
		Plan       Plan
	}{in.Request, in.Mode, in.Target, in.SourceRevision, in.BaseGeneration, in.BaseDigest, in.Plan})
	if e != nil {
		return "", e
	}
	return hash("forest-topology/intent/v1", data), nil
}

type Status struct {
	SessionID string
	Storage   Inspection
	Runtime   forest.Snapshot
	Admission bool
}

// Status does not take the management lock, so preparation barriers remain
// observable. Runtime diagnostics are live; journal observations are historical.
func (c *Controller) Status() Status {
	c.gate.RLock()
	admitted := c.admission
	c.gate.RUnlock()
	return Status{SessionID: c.session, Storage: c.store.Inspect(), Runtime: c.f.Snapshot(), Admission: admitted}
}
