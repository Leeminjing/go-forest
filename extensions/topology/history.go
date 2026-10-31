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
	a, e := canonicalize(before)
	if e != nil {
		return Diff{}, e
	}
	b, e := canonicalize(after)
	if e != nil {
		return Diff{}, e
	}
	return compareCanonical(a, b), nil
}
func compareCanonical(ca, cb *canonicalManifest) Diff {
	a, b := ca.value, cb.value
	d := Diff{AddedTrees: []Tree{}, RemovedTrees: []Tree{}, ChangedTrees: []TreeChange{}, AddedBindings: []Binding{}, RemovedBindings: []Binding{}, ChangedBindings: []BindingChange{}}
	at, bt := ca.nodes, cb.nodes
	for _, t := range b.Trees {
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
	ab, bb := ca.bindingIndex(), cb.bindingIndex()
	for _, v := range b.Bindings {
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
	return d
}

type Proposal struct {
	Expected       Position          `json:"expected"`
	Generation     uint64            `json:"generation"`
	BeforeDigest   string            `json:"before_digest"`
	TargetDigest   string            `json:"target_digest,omitempty"`
	Target         Manifest          `json:"target"`
	Mode           string            `json:"mode"`
	SourceRevision uint64            `json:"source_revision"`
	RebuildTrees   []forest.TreeID   `json:"rebuild_trees"`
	Diff           Diff              `json:"diff"`
	Plan           Plan              `json:"plan"`
	Planning       *PlanningEvidence `json:"planning,omitempty"`
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
	n, e := canonicalize(target)
	if e != nil {
		return Proposal{}, e
	}
	if e = c.catalog.checkCanonical(n); e != nil {
		return Proposal{}, e
	}
	diff := compareCanonical(before, n)
	plan, planning, err := c.catalog.planCanonical(before, n, c.generation)
	targetDigest := n.sum()
	p := Proposal{TargetDigest: targetDigest, Expected: s.state.inspection.Position, Generation: c.generation, BeforeDigest: c.digest, Target: n.value, Mode: mode, SourceRevision: source, RebuildTrees: planning.Impact.RebuildTrees, Diff: diff, Plan: plan, Planning: planning}
	return p, err
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
	in := Intent{Request: request, SessionID: c.session, Mode: mode, Target: proposal.Target, SourceRevision: proposal.SourceRevision, BaseGeneration: proposal.Generation, BaseDigest: proposal.BeforeDigest, Plan: proposal.Plan, Planning: proposal.Planning}
	if in.Planning != nil {
		in.TargetDigest = proposal.TargetDigest
	} else {
		in.TargetDigest, _ = Digest(in.Target)
	}
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
	if in.Planning != nil {
		if in.Planning.Version != 2 {
			return "", ErrUnsupportedPlanVersion
		}
		data, e := json.Marshal(struct {
			Request      Request
			Mode         string
			Target       Manifest
			TargetDigest string
			Source       uint64
			Generation   uint64
			Before       string
			Plan         Plan
			Planning     *PlanningEvidence
		}{in.Request, in.Mode, in.Target, in.TargetDigest, in.SourceRevision, in.BaseGeneration, in.BaseDigest, in.Plan, in.Planning})
		if e != nil {
			return "", e
		}
		return hash("forest-topology/intent/v2", data), nil
	}
	// Exact field order, encoding and hash domain of the released legacy input.
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
