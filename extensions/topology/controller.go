package topology

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
)

type Controller struct {
	mu                               managementLock
	gate                             sync.RWMutex
	f                                *forest.Forest
	store                            *Store
	catalog                          *Catalog
	session                          string
	opened, ready, admission, closed bool
	closing, diverged                bool
	cleanupRequired                  bool
	generation                       uint64
	digest                           string
	// Native test barriers do not replace Apply or the journal.
	barrier func(string)
}

// A waiter can abandon management admission without releasing the active
// operation's ownership or pretending its native work has stopped.
type managementLock struct{ slot chan struct{} }

func (l managementLock) Lock()   { l.slot <- struct{}{} }
func (l managementLock) Unlock() { <-l.slot }
func (l managementLock) lockContext(ctx context.Context) error {
	select {
	case l.slot <- struct{}{}:
		if e := ctx.Err(); e != nil {
			l.Unlock()
			return e
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NewController claims a Store and registers Catalog into a fresh Forest.
// The host must not directly mutate f or bypass this controller's admission.
func NewController(f *forest.Forest, s *Store, catalog *Catalog) (*Controller, error) {
	if f == nil || s == nil || catalog == nil {
		return nil, ErrInvalidManifest
	}
	snapshot := f.Snapshot()
	if snapshot.Generation != 0 || len(snapshot.Trees) != 0 || len(snapshot.Instances) != 0 {
		return nil, ErrManagedForestDiverged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned {
		return nil, ErrPersistenceUnknown
	}
	if s.owned {
		return nil, ErrStoreBusy
	}
	if s.state.inspection.Pending != nil {
		return nil, ErrReconciliationRequired
	}
	if s.state.inspection.Desired == nil {
		return nil, ErrHistoryConflict
	}
	if e := catalog.register(f); e != nil {
		return nil, e
	}
	session, e := randomID()
	if e != nil {
		return nil, e
	}
	d, _ := Digest(Empty())
	s.owned = true
	s.liveSession = session
	return &Controller{mu: managementLock{slot: make(chan struct{}, 1)}, f: f, store: s, catalog: catalog, session: session, digest: d}, nil
}
func (c *Controller) notify(stage string) {
	if c.barrier != nil {
		c.barrier(stage)
	}
}
func (c *Controller) checkpointLocked(stage string) {
	c.store.mu.Unlock()
	defer c.store.mu.Lock()
	c.notify(stage)
}
func (c *Controller) freshContext(ctx context.Context) error {
	if _, e := c.f.Generation(ctx); e == nil {
		return ErrNestedScope
	}
	return ctx.Err()
}
func (c *Controller) shutAdmission() { c.gate.Lock(); c.admission = false; c.gate.Unlock() }
func (c *Controller) actual() (Manifest, error) {
	if c.diverged {
		return Manifest{}, ErrManagedForestDiverged
	}
	snapshot := c.f.Snapshot()
	m, e := Capture(snapshot, c.catalog)
	if e != nil {
		c.diverged = true
		return Manifest{}, errors.Join(ErrManagedForestDiverged, e)
	}
	d, _ := Digest(m)
	if snapshot.Generation != c.generation || d != c.digest {
		c.diverged = true
		return Manifest{}, ErrManagedForestDiverged
	}
	return m, nil
}

// Restore creates new physical objects. Approval documents the host's resource
// reconciliation; it cannot prove that an external effect is safe to repeat.
func (c *Controller) Restore(ctx context.Context, request Request) (Receipt, error) {
	if e := c.freshContext(ctx); e != nil {
		return Receipt{}, e
	}
	if e := c.mu.lockContext(ctx); e != nil {
		return Receipt{}, e
	}
	defer c.mu.Unlock()
	if !validRequest(request) {
		return Receipt{}, ErrInvalidManifest
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned {
		return Receipt{}, ErrPersistenceUnknown
	}
	desired := s.state.inspection.Desired
	if desired == nil {
		return Receipt{}, ErrHistoryConflict
	}
	emptyDigest, _ := Digest(Empty())
	input := Intent{Request: request, SessionID: c.session, Mode: "restore", Target: detached(desired.Manifest), TargetDigest: desired.Digest, SourceRevision: desired.ID, BaseGeneration: 0, BaseDigest: emptyDigest, Plan: makePlan(Empty(), desired.Manifest, 0, false)}
	h, _ := intentHash(input)
	if r, ok, e := s.duplicate(request.ID, h); ok {
		r.HistoricalOnly = r.SessionID != c.session
		return r, recordedError(r, e)
	}
	if c.closed || c.closing || c.ready {
		return Receipt{}, ErrHistoryConflict
	}
	if e := s.checkLocked(request.Expected); e != nil {
		return Receipt{}, e
	}
	if s.state.inspection.Pending != nil {
		return Receipt{}, ErrReconciliationRequired
	}
	if e := c.catalog.check(desired.Manifest); e != nil {
		return Receipt{}, e
	}
	if _, e := c.actual(); e != nil {
		c.shutAdmission()
		return Receipt{}, e
	}
	if c.cleanupRequired {
		if e := c.f.Wait(ctx); e != nil {
			return Receipt{}, e
		}
		c.cleanupRequired = false
	}
	c.shutAdmission()
	if !c.opened {
		policy := Request{ID: "session-" + c.session, Expected: s.state.inspection.Position, Approval: request.Approval}
		if e := s.appendLocked(ctx, record{Kind: "session-opened", SessionID: c.session, Request: &policy}, 0); e != nil {
			return Receipt{}, e
		}
		c.opened = true
	}
	return c.applyLocked(ctx, input, h)
}
func (c *Controller) applyLocked(ctx context.Context, in Intent, requestDigest string) (Receipt, error) {
	s := c.store
	c.checkpointLocked("before-intent")
	if e := s.appendLocked(ctx, record{Kind: "intent", Intent: &in, RequestDigest: requestDigest}, maxFrameBytes+48); e != nil {
		return Receipt{}, e
	}
	intentSequence := s.state.pendingSequence
	// Public inspection remains available while native preparation is blocked.
	s.mu.Unlock()
	generation, e := func() (uint64, error) {
		defer s.mu.Lock()
		c.notify("intent-synced")
		g, err := c.f.Apply(ctx, forest.ChangePlan{BaseGeneration: in.Plan.BaseGeneration, Operations: in.Plan.Operations})
		c.notify("apply-returned")
		return g, err
	}()
	result := Receipt{OperationID: in.Request.ID, SessionID: c.session, Mode: in.Mode, Revision: s.state.inspection.Desired.ID, Generation: generation, ManifestDigest: in.TargetDigest, Outcome: "observed", Instances: []InstanceObservation{}}
	if e != nil {
		c.cleanupRequired = true
		result.Outcome = "failed"
		result.FailureCodes = failureCodes(e)
		result.FailureKind = result.FailureCodes[0]
		result.FailureStage = "apply"
		var detail *forest.Error
		if errors.As(e, &detail) {
			result.FailureStage = detail.Stage
			result.FailureTree = string(detail.Tree)
		}
		var conflict *forest.GenerationConflict
		if errors.As(e, &conflict) {
			copy := *conflict
			result.Conflict = &copy
		}
		result.ManifestDigest = in.BaseDigest
		save := s.appendLocked(context.Background(), record{Kind: "apply-failed", SessionID: c.session, Receipt: &result}, 0)
		wrapped := &Error{Stage: result.FailureStage, Sequence: intentSequence, OperationID: in.Request.ID, TreeID: result.FailureTree, Cause: e}
		if save != nil {
			return result, errors.Join(wrapped, save)
		}
		return result, wrapped
	}
	snapshot := c.f.Snapshot()
	actual, err := Capture(snapshot, c.catalog)
	if err != nil {
		return result, errors.Join(ErrManagedForestDiverged, err)
	}
	d, _ := Digest(actual)
	c.generation = snapshot.Generation
	c.digest = d
	if generation != snapshot.Generation || d != in.TargetDigest {
		return result, ErrManagedForestDiverged
	}
	for _, t := range snapshot.Trees {
		result.Instances = append(result.Instances, InstanceObservation{TreeID: string(t.ID), InstanceID: t.InstanceID})
	}
	sort.Slice(result.Instances, func(i, j int) bool { return result.Instances[i].TreeID < result.Instances[j].TreeID })
	result.Noop = in.Mode != "restore" && in.BaseDigest == in.TargetDigest
	if in.Mode != "restore" && !result.Noop {
		result.Revision = intentSequence
	}
	if err = s.appendLocked(context.Background(), record{Kind: "apply-observed", SessionID: c.session, Receipt: &result}, 0); err != nil {
		return result, err
	}
	c.ready = true
	c.checkpointLocked("observation-synced")
	return detached(result), nil
}
func recordedError(r Receipt, e error) error {
	if e != nil {
		return e
	}
	if r.Outcome == "reconciled" {
		return ErrReconciliationRequired
	}
	if r.Outcome == "failed" {
		causes := []error{ErrOperationFailed}
		for _, code := range r.FailureCodes {
			for _, item := range nativeCodes {
				if item.code == code {
					causes = append(causes, item.err)
				}
			}
		}
		if r.Conflict != nil {
			causes = append(causes, r.Conflict)
		}
		return &Error{Stage: "recorded-" + r.FailureStage, OperationID: r.OperationID, TreeID: r.FailureTree, Cause: errors.Join(causes...)}
	}
	return nil
}

func (c *Controller) OpenAdmission(ctx context.Context) error {
	if e := c.freshContext(ctx); e != nil {
		return e
	}
	if e := c.mu.lockContext(ctx); e != nil {
		return e
	}
	defer c.mu.Unlock()
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.closed || c.closing || !c.ready {
		return ErrAdmissionClosed
	}
	if s.poisoned || s.closed {
		return ErrPersistenceUnknown
	}
	if s.state.inspection.Pending != nil {
		return ErrReconciliationRequired
	}
	if _, e := c.actual(); e != nil {
		c.shutAdmission()
		return e
	}
	if c.cleanupRequired {
		if e := c.f.Wait(ctx); e != nil {
			return e
		}
		c.cleanupRequired = false
	}
	c.gate.Lock()
	c.admission = true
	c.gate.Unlock()
	return nil
}

// Invoke releases the entry gate after Forest has pinned a Scope, before user
// work runs. Nested calls inherit an already-entered native Scope.
func (c *Controller) Invoke(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return forest.ErrInvalidOperation
	}
	if _, e := c.f.Generation(ctx); e == nil {
		return c.f.Invoke(ctx, fn)
	}
	c.gate.RLock()
	locked := true
	defer func() {
		if locked {
			c.gate.RUnlock()
		}
	}()
	if !c.admission {
		return ErrAdmissionClosed
	}
	return c.f.Invoke(ctx, func(call context.Context) error { c.gate.RUnlock(); locked = false; return fn(call) })
}

// Close retains Store ownership until actual native resource cleanup completes.
// Hosts must quiesce and join any ingress not governed by Invoke themselves.
func (c *Controller) Close(ctx context.Context) error {
	if e := c.freshContext(ctx); e != nil {
		return e
	}
	if e := c.mu.lockContext(ctx); e != nil {
		return e
	}
	defer c.mu.Unlock()
	c.shutAdmission()
	c.closing = true
	if c.closed {
		return nil
	}
	if e := c.f.Close(ctx); e != nil {
		return e
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	var e error
	if c.opened && !s.poisoned {
		e = s.appendLocked(context.Background(), record{Kind: "session-closed", SessionID: c.session}, 0)
	}
	c.closed = true
	s.owned = false
	s.liveSession = ""
	return errors.Join(e, s.closeLocked())
}
