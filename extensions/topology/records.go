package topology

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

// reduce validates both newly appended and replayed records. It never infers
// a missing terminal record from an intent, desired hash or process death.
func (s *journalState) reduce(r record, digest string) error {
	if _, e := time.Parse(time.RFC3339Nano, r.Timestamp); e != nil {
		return ErrCorruptJournal
	}
	pos := s.inspection.Position
	if r.Sequence != pos.Sequence+1 || r.PreviousDigest != pos.Digest || !validDigest(digest) {
		return ErrCorruptJournal
	}
	if r.Sequence == 1 {
		if r.Kind != "genesis" || len(r.StoreID) != 32 {
			return ErrCorruptJournal
		}
	} else if r.StoreID != pos.StoreID || r.Kind == "genesis" {
		return ErrCorruptJournal
	}
	switch r.Kind {
	case "genesis":
		if r.Request != nil || r.Target != nil || r.Intent != nil || r.Receipt != nil || r.SessionID != "" {
			return ErrCorruptJournal
		}
	case "seed":
		if len(s.history) != 0 || r.Request == nil || !validRequest(*r.Request) || r.Target == nil {
			return ErrCorruptJournal
		}
		n, e := Normalize(*r.Target)
		if e != nil {
			return e
		}
		h, _ := requestHash("seed", *r.Request, n, 0)
		if h != r.RequestDigest || r.Request.Expected != pos {
			return ErrCorruptJournal
		}
		d, _ := Digest(n)
		revision := Revision{ID: r.Sequence, Source: "desired-only", Manifest: n, Digest: d}
		s.history = append(s.history, revision)
		s.inspection.Desired = &revision
		receipt := Receipt{OperationID: r.Request.ID, Mode: "seed", Outcome: "selected", Revision: r.Sequence, ManifestDigest: d, Instances: []InstanceObservation{}}
		s.requests[r.Request.ID] = requestResult{digest: h, receipt: receipt, request: *r.Request}
		s.inspection.LastResult = detached(&receipt)
	case "session-opened":
		if r.Request == nil || !validRequest(*r.Request) || r.Request.Expected != pos || s.inspection.Desired == nil || s.inspection.Pending != nil || len(r.SessionID) != 32 || s.sessions[r.SessionID] {
			return ErrCorruptJournal
		}
		s.sessions[r.SessionID] = true
		s.inspection.SessionID = r.SessionID
		s.sessionActive = true
		s.sessionBase = pos
		s.hasAttempt = false
		s.generation = 0
		s.runtimeDigest, _ = Digest(Empty())
	case "selected":
		if r.Request == nil || !validRequest(*r.Request) || r.Request.Expected != pos || r.Target == nil || s.inspection.Pending != nil {
			return ErrCorruptJournal
		}
		if _, ok := s.requests[r.Request.ID]; ok {
			return ErrRequestConflict
		}
		n, e := Normalize(*r.Target)
		if e != nil {
			return e
		}
		d, _ := Digest(n)
		found := false
		for _, v := range s.history {
			if v.ID == r.FromRevision && v.Digest == d {
				found = true
			}
		}
		if !found {
			return ErrCorruptJournal
		}
		h, _ := requestHash("select-historical", *r.Request, n, r.FromRevision)
		if h != r.RequestDigest {
			return ErrCorruptJournal
		}
		v := Revision{ID: r.Sequence, Source: "host-selected", Manifest: n, Digest: d, FromRevision: r.FromRevision}
		s.history = append(s.history, v)
		s.inspection.Desired = &v
		s.sessionActive = false
		result := Receipt{OperationID: r.Request.ID, Mode: "select-historical", Outcome: "selected", Revision: r.Sequence, ManifestDigest: d, Instances: []InstanceObservation{}}
		s.requests[r.Request.ID] = requestResult{digest: h, receipt: result, request: *r.Request}
		s.inspection.LastResult = detached(&result)
	case "intent":
		if r.Intent == nil || !s.sessionActive || s.inspection.Pending != nil {
			return ErrCorruptJournal
		}
		in := r.Intent
		if in.SessionID != s.inspection.SessionID || !validRequest(in.Request) || in.BaseGeneration != s.generation || in.BaseDigest != s.runtimeDigest {
			return ErrCorruptJournal
		}
		if in.Request.Expected != pos && !(in.Mode == "restore" && !s.hasAttempt && in.Request.Expected == s.sessionBase) {
			return ErrCorruptJournal
		}
		if _, ok := s.requests[in.Request.ID]; ok {
			return ErrRequestConflict
		}
		n, e := Normalize(in.Target)
		if e != nil {
			return e
		}
		d, _ := Digest(n)
		if d != in.TargetDigest {
			return ErrCorruptJournal
		}
		h, hashErr := intentHash(*in)
		if hashErr != nil {
			return hashErr
		}
		if h != r.RequestDigest {
			return ErrCorruptJournal
		}
		before := Empty()
		switch in.Mode {
		case "restore":
			if s.generation != 0 || s.inspection.Desired == nil || d != s.inspection.Desired.Digest || in.SourceRevision != s.inspection.Desired.ID {
				return ErrCorruptJournal
			}
		case "reconfigure", "rollback":
			if s.inspection.LastObserved == nil || s.inspection.LastObserved.SessionID != in.SessionID || s.inspection.Desired == nil {
				return ErrCorruptJournal
			}
			before = s.inspection.Desired.Manifest
			if in.Mode == "rollback" {
				found := false
				for _, v := range s.history {
					if v.ID == in.SourceRevision && v.Digest == d {
						found = true
					}
				}
				if !found {
					return ErrCorruptJournal
				}
			} else if in.SourceRevision != 0 {
				return ErrCorruptJournal
			}
		default:
			return ErrCorruptJournal
		}
		p := makePlanV1(before, n, in.BaseGeneration, in.Mode != "restore")
		if in.Mode != "restore" && in.BaseDigest == d {
			p = Plan{BaseGeneration: in.BaseGeneration, Operations: []forest.Operation{}}
		}
		if in.Planning != nil {
			if in.Planning.Version != 2 {
				return ErrUnsupportedPlanVersion
			}
			calculated, evidence, e := planV2(before, n, in.BaseGeneration, in.Planning.Facts)
			if e != nil || !reflect.DeepEqual(evidence, in.Planning) {
				return ErrCorruptJournal
			}
			for _, fact := range in.Planning.Facts {
				key := factKey(fact)
				if old, ok := s.facts[key]; ok && !reflect.DeepEqual(old, fact) {
					return ErrCorruptJournal
				}
				s.facts[key] = fact
			}
			p = calculated
		}
		if !reflect.DeepEqual(p, in.Plan) {
			return ErrCorruptJournal
		}
		s.inspection.Pending = detached(in)
		s.pendingSequence = r.Sequence
		s.pendingHash = h
		s.hasAttempt = true
	case "apply-observed", "apply-failed":
		in := s.inspection.Pending
		if in == nil || r.Receipt == nil || r.SessionID != in.SessionID {
			return ErrCorruptJournal
		}
		result := r.Receipt
		if result.OperationID != in.Request.ID || result.SessionID != in.SessionID || result.Mode != in.Mode || result.HistoricalOnly {
			return ErrCorruptJournal
		}
		if r.Kind == "apply-observed" {
			expected := in.BaseGeneration
			if len(in.Plan.Operations) > 0 {
				if expected == ^uint64(0) {
					return ErrCorruptJournal
				}
				expected++
			}
			if result.Generation != expected || result.ManifestDigest != in.TargetDigest || result.Outcome != "observed" || result.FailureKind != "" || len(result.FailureCodes) != 0 || result.FailureStage != "" || result.FailureTree != "" || result.Conflict != nil || len(result.Instances) != len(in.Target.Trees) {
				return ErrCorruptJournal
			}
			ids := map[string]bool{}
			for i, instance := range result.Instances {
				if instance.TreeID != string(in.Target.Trees[i].ID) || !validID(instance.InstanceID) || ids[instance.InstanceID] {
					return ErrCorruptJournal
				}
				ids[instance.InstanceID] = true
			}
			if in.Planning != nil && in.Mode != "restore" {
				if e := checkRetainedInstances(in.Planning.Impact, s.inspection.LastObserved, result); e != nil {
					return e
				}
			}
			noop := in.Mode != "restore" && in.BaseDigest == in.TargetDigest
			if result.Noop != noop {
				return ErrCorruptJournal
			}
			revision := s.inspection.Desired.ID
			if in.Mode != "restore" && !noop {
				revision = s.pendingSequence
				v := Revision{ID: revision, Source: "apply-observed", Manifest: detached(in.Target), Digest: in.TargetDigest, FromRevision: in.SourceRevision}
				s.history = append(s.history, v)
				s.inspection.Desired = &v
			}
			if result.Revision != revision {
				return ErrCorruptJournal
			}
			s.generation = expected
			s.runtimeDigest = in.TargetDigest
			s.inspection.LastObserved = detached(result)
		} else {
			if result.Outcome != "failed" || result.Generation != in.BaseGeneration || result.ManifestDigest != in.BaseDigest || result.Revision != s.inspection.Desired.ID || !validFailureCodes(result.FailureCodes) || result.FailureKind != result.FailureCodes[0] || !validID(result.FailureStage) || len(result.Instances) != 0 || result.Noop {
				return ErrCorruptJournal
			}
		}
		s.requests[result.OperationID] = requestResult{digest: s.pendingHash, receipt: detached(*result), request: in.Request}
		s.inspection.LastResult = detached(result)
		s.inspection.Pending = nil
		s.pendingSequence = 0
		s.pendingHash = ""
	case "resolved":
		in := s.inspection.Pending
		if in == nil || r.Request == nil || !validRequest(*r.Request) || r.Request.Expected != pos || r.PendingID != in.Request.ID || (r.Choice != "previous" && r.Choice != "target") {
			return ErrCorruptJournal
		}
		if _, ok := s.requests[r.Request.ID]; ok || r.Request.ID == in.Request.ID {
			return ErrRequestConflict
		}
		h, _ := requestHash("resolve/"+r.Choice+"/"+r.PendingID, *r.Request, Empty(), 0)
		if h != r.RequestDigest {
			return ErrCorruptJournal
		}
		if r.Choice == "target" {
			v := Revision{ID: r.Sequence, Source: "host-selected", Manifest: detached(in.Target), Digest: in.TargetDigest, FromRevision: s.pendingSequence}
			s.history = append(s.history, v)
			s.inspection.Desired = &v
		}
		result := Receipt{OperationID: r.Request.ID, Mode: "resolve", Outcome: "selected", Revision: s.inspection.Desired.ID, ManifestDigest: s.inspection.Desired.Digest, Instances: []InstanceObservation{}}
		s.requests[r.Request.ID] = requestResult{digest: h, receipt: result, request: *r.Request}
		s.inspection.LastResult = detached(&result)
		s.requests[in.Request.ID] = requestResult{digest: s.pendingHash, receipt: Receipt{OperationID: in.Request.ID, SessionID: in.SessionID, Mode: in.Mode, Outcome: "reconciled", Revision: s.inspection.Desired.ID, ManifestDigest: s.inspection.Desired.Digest, Instances: []InstanceObservation{}}, request: in.Request}
		s.inspection.Pending = nil
		s.pendingSequence = 0
		s.pendingHash = ""
		s.sessionActive = false
	case "session-closed":
		if !s.sessionActive || r.SessionID != s.inspection.SessionID {
			return ErrCorruptJournal
		}
		s.sessionActive = false
	default:
		return fmt.Errorf("%w: record kind %q", ErrUnsupportedFormat, r.Kind)
	}
	// Known fields on the wrong record kind must not be silently ignored.
	allowed := record{Sequence: r.Sequence, StoreID: r.StoreID, PreviousDigest: r.PreviousDigest, Kind: r.Kind, Timestamp: r.Timestamp}
	switch r.Kind {
	case "seed":
		allowed.Request = r.Request
		allowed.RequestDigest = r.RequestDigest
		allowed.Target = r.Target
	case "selected":
		allowed.Request = r.Request
		allowed.RequestDigest = r.RequestDigest
		allowed.Target = r.Target
		allowed.FromRevision = r.FromRevision
	case "session-opened":
		allowed.Request = r.Request
		allowed.SessionID = r.SessionID
	case "intent":
		allowed.Intent = r.Intent
		allowed.RequestDigest = r.RequestDigest
	case "apply-observed", "apply-failed":
		allowed.Receipt = r.Receipt
		allowed.SessionID = r.SessionID
	case "resolved":
		allowed.Request = r.Request
		allowed.RequestDigest = r.RequestDigest
		allowed.Choice = r.Choice
		allowed.PendingID = r.PendingID
	case "session-closed":
		allowed.SessionID = r.SessionID
	}
	if !reflect.DeepEqual(allowed, r) {
		return ErrCorruptJournal
	}
	s.inspection.Position = Position{StoreID: r.StoreID, Sequence: r.Sequence, Digest: digest}
	entry := Entry{Position: s.inspection.Position, Timestamp: r.Timestamp, Kind: r.Kind, SessionID: r.SessionID, RelatedOperationID: r.PendingID}
	if r.Request != nil {
		entry.OperationID = r.Request.ID
		entry.Approval = r.Request.Approval
	}
	if r.Intent != nil {
		entry.OperationID = r.Intent.Request.ID
		entry.Approval = r.Intent.Request.Approval
		entry.SessionID = r.Intent.SessionID
		entry.TargetDigest = r.Intent.TargetDigest
		entry.Plan = detached(&r.Intent.Plan)
		entry.Planning = detached(r.Intent.Planning)
	}
	if r.Target != nil {
		entry.TargetDigest, _ = Digest(*r.Target)
	}
	if r.Receipt != nil {
		entry.OperationID = r.Receipt.OperationID
		entry.Receipt = detached(r.Receipt)
		entry.TargetDigest = r.Receipt.ManifestDigest
	}
	s.entries = append(s.entries, entry)
	return nil
}
