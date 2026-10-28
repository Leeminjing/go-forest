package topology

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

// prepareRecord validates both newly appended and replayed records. It never infers
// a missing terminal record from an intent, desired hash or process death.
func (current *journalState) prepareRecord(r record, digest string) (preparedRecord, error) {
	next := *current
	s := &next // read old immutable values; writes below target only this delta
	change := preparedRecord{}

	if _, e := time.Parse(time.RFC3339Nano, r.Timestamp); e != nil {
		return preparedRecord{}, ErrCorruptJournal
	}
	pos := s.inspection.Position
	if r.Sequence != pos.Sequence+1 || r.PreviousDigest != pos.Digest || !validDigest(digest) {
		return preparedRecord{}, ErrCorruptJournal
	}
	if r.Sequence == 1 {
		if r.Kind != "genesis" || len(r.StoreID) != 32 {
			return preparedRecord{}, ErrCorruptJournal
		}
	} else if r.StoreID != pos.StoreID || r.Kind == "genesis" {
		return preparedRecord{}, ErrCorruptJournal
	}
	switch r.Kind {
	case "genesis":
		if r.Request != nil || r.Target != nil || r.Intent != nil || r.Receipt != nil || r.SessionID != "" {
			return preparedRecord{}, ErrCorruptJournal
		}
	case "seed":
		if len(s.history) != 0 || r.Request == nil || !validRequest(*r.Request) || r.Target == nil {
			return preparedRecord{}, ErrCorruptJournal
		}
		n, e := Normalize(*r.Target)
		if e != nil {
			return preparedRecord{}, e
		}
		h, _ := requestHash("seed", *r.Request, n, 0)
		if h != r.RequestDigest || r.Request.Expected != pos {
			return preparedRecord{}, ErrCorruptJournal
		}
		d, _ := Digest(n)
		revision := Revision{ID: r.Sequence, Source: "desired-only", Manifest: n, Digest: d}
		change.revision = &revision
		s.inspection.Desired = &revision
		receipt := Receipt{OperationID: r.Request.ID, Mode: "seed", Outcome: "selected", Revision: r.Sequence, ManifestDigest: d, Instances: []InstanceObservation{}}
		change.requests = append(change.requests, requestWrite{key: r.Request.ID, value: requestResult{digest: h, receipt: receipt, request: *r.Request}})
		s.inspection.LastResult = detached(&receipt)
	case "session-opened":
		if r.Request == nil || !validRequest(*r.Request) || r.Request.Expected != pos || s.inspection.Desired == nil || s.inspection.Pending != nil || len(r.SessionID) != 32 || s.sessions[r.SessionID] {
			return preparedRecord{}, ErrCorruptJournal
		}
		change.session = r.SessionID
		s.inspection.SessionID = r.SessionID
		s.sessionActive = true
		s.sessionBase = pos
		s.hasAttempt = false
		s.generation = 0
		s.runtimeDigest, _ = Digest(Empty())
	case "selected":
		if r.Request == nil || !validRequest(*r.Request) || r.Request.Expected != pos || r.Target == nil || s.inspection.Pending != nil {
			return preparedRecord{}, ErrCorruptJournal
		}
		if _, ok := s.requests[r.Request.ID]; ok {
			return preparedRecord{}, ErrRequestConflict
		}
		n, e := Normalize(*r.Target)
		if e != nil {
			return preparedRecord{}, e
		}
		d, _ := Digest(n)
		found := false
		for _, v := range s.history {
			if v.ID == r.FromRevision && v.Digest == d {
				found = true
			}
		}
		if !found {
			return preparedRecord{}, ErrCorruptJournal
		}
		h, _ := requestHash("select-historical", *r.Request, n, r.FromRevision)
		if h != r.RequestDigest {
			return preparedRecord{}, ErrCorruptJournal
		}
		v := Revision{ID: r.Sequence, Source: "host-selected", Manifest: n, Digest: d, FromRevision: r.FromRevision}
		change.revision = &v
		s.inspection.Desired = &v
		s.sessionActive = false
		result := Receipt{OperationID: r.Request.ID, Mode: "select-historical", Outcome: "selected", Revision: r.Sequence, ManifestDigest: d, Instances: []InstanceObservation{}}
		change.requests = append(change.requests, requestWrite{key: r.Request.ID, value: requestResult{digest: h, receipt: result, request: *r.Request}})
		s.inspection.LastResult = detached(&result)
	case "intent":
		if r.Intent == nil || !s.sessionActive || s.inspection.Pending != nil {
			return preparedRecord{}, ErrCorruptJournal
		}
		in := r.Intent
		if in.SessionID != s.inspection.SessionID || !validRequest(in.Request) || in.BaseGeneration != s.generation || in.BaseDigest != s.runtimeDigest {
			return preparedRecord{}, ErrCorruptJournal
		}
		if in.Request.Expected != pos && !(in.Mode == "restore" && !s.hasAttempt && in.Request.Expected == s.sessionBase) {
			return preparedRecord{}, ErrCorruptJournal
		}
		if _, ok := s.requests[in.Request.ID]; ok {
			return preparedRecord{}, ErrRequestConflict
		}
		n, e := Normalize(in.Target)
		if e != nil {
			return preparedRecord{}, e
		}
		d, _ := Digest(n)
		if d != in.TargetDigest {
			return preparedRecord{}, ErrCorruptJournal
		}
		h, hashErr := intentHash(*in)
		if hashErr != nil {
			return preparedRecord{}, hashErr
		}
		if h != r.RequestDigest {
			return preparedRecord{}, ErrCorruptJournal
		}
		before := Empty()
		switch in.Mode {
		case "restore":
			if s.generation != 0 || s.inspection.Desired == nil || d != s.inspection.Desired.Digest || in.SourceRevision != s.inspection.Desired.ID {
				return preparedRecord{}, ErrCorruptJournal
			}
		case "reconfigure", "rollback":
			if s.inspection.LastObserved == nil || s.inspection.LastObserved.SessionID != in.SessionID || s.inspection.Desired == nil {
				return preparedRecord{}, ErrCorruptJournal
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
					return preparedRecord{}, ErrCorruptJournal
				}
			} else if in.SourceRevision != 0 {
				return preparedRecord{}, ErrCorruptJournal
			}
		default:
			return preparedRecord{}, ErrCorruptJournal
		}
		p := makePlanV1(before, n, in.BaseGeneration, in.Mode != "restore")
		if in.Mode != "restore" && in.BaseDigest == d {
			p = Plan{BaseGeneration: in.BaseGeneration, Operations: []forest.Operation{}}
		}
		if in.Planning != nil {
			if in.Planning.Version != 2 {
				return preparedRecord{}, ErrUnsupportedPlanVersion
			}
			calculated, evidence, e := planV2(before, n, in.BaseGeneration, in.Planning.Facts)
			if e != nil || !reflect.DeepEqual(evidence, in.Planning) {
				return preparedRecord{}, ErrCorruptJournal
			}
			for _, fact := range in.Planning.Facts {
				key := factKey(fact)
				if old, ok := s.facts[key]; ok && !reflect.DeepEqual(old, fact) {
					return preparedRecord{}, ErrCorruptJournal
				}
				// planV2 already requires strictly increasing, unique fact
				// keys. Only previously unknown immutable facts need storage.
				if _, known := s.facts[key]; !known {
					change.facts = append(change.facts, factWrite{key: key, value: detached(fact)})
				}
			}
			p = calculated
		}
		if !reflect.DeepEqual(p, in.Plan) {
			return preparedRecord{}, ErrCorruptJournal
		}
		s.inspection.Pending = detached(in)
		s.pendingSequence = r.Sequence
		s.pendingHash = h
		s.hasAttempt = true
	case "apply-observed", "apply-failed":
		in := s.inspection.Pending
		if in == nil || r.Receipt == nil || r.SessionID != in.SessionID {
			return preparedRecord{}, ErrCorruptJournal
		}
		result := r.Receipt
		if result.OperationID != in.Request.ID || result.SessionID != in.SessionID || result.Mode != in.Mode || result.HistoricalOnly {
			return preparedRecord{}, ErrCorruptJournal
		}
		if r.Kind == "apply-observed" {
			expected := in.BaseGeneration
			if len(in.Plan.Operations) > 0 {
				if expected == ^uint64(0) {
					return preparedRecord{}, ErrCorruptJournal
				}
				expected++
			}
			if result.Generation != expected || result.ManifestDigest != in.TargetDigest || result.Outcome != "observed" || result.FailureKind != "" || len(result.FailureCodes) != 0 || result.FailureStage != "" || result.FailureTree != "" || result.Conflict != nil || len(result.Instances) != len(in.Target.Trees) {
				return preparedRecord{}, ErrCorruptJournal
			}
			ids := map[string]bool{}
			for i, instance := range result.Instances {
				if instance.TreeID != string(in.Target.Trees[i].ID) || !validID(instance.InstanceID) || ids[instance.InstanceID] {
					return preparedRecord{}, ErrCorruptJournal
				}
				ids[instance.InstanceID] = true
			}
			if in.Planning != nil && in.Mode != "restore" {
				if e := checkRetainedInstances(in.Planning.Impact, s.inspection.LastObserved, result); e != nil {
					return preparedRecord{}, e
				}
			}
			noop := in.Mode != "restore" && in.BaseDigest == in.TargetDigest
			if result.Noop != noop {
				return preparedRecord{}, ErrCorruptJournal
			}
			revision := s.inspection.Desired.ID
			if in.Mode != "restore" && !noop {
				revision = s.pendingSequence
				v := Revision{ID: revision, Source: "apply-observed", Manifest: detached(in.Target), Digest: in.TargetDigest, FromRevision: in.SourceRevision}
				change.revision = &v
				s.inspection.Desired = &v
			}
			if result.Revision != revision {
				return preparedRecord{}, ErrCorruptJournal
			}
			s.generation = expected
			s.runtimeDigest = in.TargetDigest
			s.inspection.LastObserved = detached(result)
		} else {
			if result.Outcome != "failed" || result.Generation != in.BaseGeneration || result.ManifestDigest != in.BaseDigest || result.Revision != s.inspection.Desired.ID || !validFailureCodes(result.FailureCodes) || result.FailureKind != result.FailureCodes[0] || !validID(result.FailureStage) || len(result.Instances) != 0 || result.Noop {
				return preparedRecord{}, ErrCorruptJournal
			}
		}
		change.requests = append(change.requests, requestWrite{key: result.OperationID, value: requestResult{digest: s.pendingHash, receipt: detached(*result), request: in.Request}})
		s.inspection.LastResult = detached(result)
		s.inspection.Pending = nil
		s.pendingSequence = 0
		s.pendingHash = ""
	case "resolved":
		in := s.inspection.Pending
		if in == nil || r.Request == nil || !validRequest(*r.Request) || r.Request.Expected != pos || r.PendingID != in.Request.ID || (r.Choice != "previous" && r.Choice != "target") {
			return preparedRecord{}, ErrCorruptJournal
		}
		if _, ok := s.requests[r.Request.ID]; ok || r.Request.ID == in.Request.ID {
			return preparedRecord{}, ErrRequestConflict
		}
		h, _ := requestHash("resolve/"+r.Choice+"/"+r.PendingID, *r.Request, Empty(), 0)
		if h != r.RequestDigest {
			return preparedRecord{}, ErrCorruptJournal
		}
		if r.Choice == "target" {
			v := Revision{ID: r.Sequence, Source: "host-selected", Manifest: detached(in.Target), Digest: in.TargetDigest, FromRevision: s.pendingSequence}
			change.revision = &v
			s.inspection.Desired = &v
		}
		result := Receipt{OperationID: r.Request.ID, Mode: "resolve", Outcome: "selected", Revision: s.inspection.Desired.ID, ManifestDigest: s.inspection.Desired.Digest, Instances: []InstanceObservation{}}
		change.requests = append(change.requests, requestWrite{key: r.Request.ID, value: requestResult{digest: h, receipt: result, request: *r.Request}})
		s.inspection.LastResult = detached(&result)
		change.requests = append(change.requests, requestWrite{key: in.Request.ID, value: requestResult{digest: s.pendingHash, receipt: Receipt{OperationID: in.Request.ID, SessionID: in.SessionID, Mode: in.Mode, Outcome: "reconciled", Revision: s.inspection.Desired.ID, ManifestDigest: s.inspection.Desired.Digest, Instances: []InstanceObservation{}}, request: in.Request}})
		s.inspection.Pending = nil
		s.pendingSequence = 0
		s.pendingHash = ""
		s.sessionActive = false
	case "session-closed":
		if !s.sessionActive || r.SessionID != s.inspection.SessionID {
			return preparedRecord{}, ErrCorruptJournal
		}
		s.sessionActive = false
	default:
		return preparedRecord{}, fmt.Errorf("%w: record kind %q", ErrUnsupportedFormat, r.Kind)
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
		return preparedRecord{}, ErrCorruptJournal
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
	change.entry = entry
	change.next = next
	return change, nil
}
