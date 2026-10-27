package testkit

import (
	"fmt"
	"sync"
)

type Ownership string

const (
	Owned    Ownership = "owned"
	Borrowed Ownership = "borrowed"
)

// Scope.Generation is local to Forest. Call is the fixture's actual invocation
// label, not an invented identifier for Forest's private invocation structure.
type Scope struct {
	Forest, Call string
	Generation   uint64
}
type Execution struct {
	Scope                                  Scope
	Task, Step, Attempt, Process, Instance string
}
type Effect struct {
	Namespace, Operation, InputDigest, Receipt, ContentDigest string
	OriginProcess, OriginVersion                              string
	OriginGeneration                                          uint64
}

// Resource describes a sample of the original owned/borrowed handle. A failed
// probe must be returned separately, not encoded as Usable=false.
type Resource struct {
	ID         string
	Ownership  Ownership
	Usable     bool
	CloseCount uint64
}
type Event struct {
	Stage     string
	Execution *Execution
	Effect    *Effect
	Resources []Resource
}
type Entry struct {
	Sequence uint64
	Event
}

// Trace is detached, with completeness information retained for every checker.
type Trace struct {
	entries []Entry
	err     error
}

func (t Trace) Entries() []Entry { return cloneEntries(t.entries) }

type Journal struct {
	mu       sync.Mutex
	capacity int
	entries  []Entry
	err      error
}

func NewJournal(capacity int) (*Journal, error) {
	if capacity < 1 || capacity > 65536 {
		return nil, fmt.Errorf("capacity %d: %w", capacity, ErrInvalidEvidence)
	}
	return &Journal{capacity: capacity}, nil
}
func (j *Journal) Record(event Event) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil {
		return 0, j.err
	}
	if err := validEvent(event); err != nil {
		j.err = err
		return 0, err
	}
	if len(j.entries) == j.capacity {
		j.err = ErrEvidenceOverflow
		return 0, j.err
	}
	n := uint64(len(j.entries) + 1)
	j.entries = append(j.entries, Entry{Sequence: n, Event: cloneEvent(event)})
	return n, nil
}
func (j *Journal) Snapshot() (Trace, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return Trace{entries: cloneEntries(j.entries), err: j.err}, j.err
}
func cloneEvent(e Event) Event {
	if e.Execution != nil {
		x := *e.Execution
		e.Execution = &x
	}
	if e.Effect != nil {
		x := *e.Effect
		e.Effect = &x
	}
	e.Resources = append([]Resource(nil), e.Resources...)
	return e
}
func cloneEntries(in []Entry) []Entry {
	out := make([]Entry, len(in))
	for i, e := range in {
		out[i] = Entry{Sequence: e.Sequence, Event: cloneEvent(e.Event)}
	}
	return out
}
func validEvent(e Event) error {
	fields := []string{e.Stage}
	if e.Stage == "" || len(e.Resources) > 128 {
		return ErrInvalidEvidence
	}
	if x := e.Execution; x != nil {
		fields = append(fields, x.Scope.Forest, x.Scope.Call, x.Task, x.Step, x.Attempt, x.Process, x.Instance)
	}
	if x := e.Effect; x != nil {
		fields = append(fields, x.Namespace, x.Operation, x.InputDigest, x.Receipt, x.ContentDigest, x.OriginProcess, x.OriginVersion)
	}
	for _, r := range e.Resources {
		fields = append(fields, r.ID)
		if r.ID == "" || (r.Ownership != Owned && r.Ownership != Borrowed) {
			return ErrInvalidEvidence
		}
	}
	for _, s := range fields {
		if len(s) > 4096 {
			return ErrInvalidEvidence
		}
	}
	return nil
}
