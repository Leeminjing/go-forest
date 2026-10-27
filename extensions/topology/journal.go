package topology

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Leeminjing/go-forest/forest"
)

const maxJournalBytes int64 = 256 << 20
const maxRecords = 10000
const frameMagic = "FRTOP001"

type Position struct {
	StoreID  string `json:"store_id"`
	Sequence uint64 `json:"sequence"`
	Digest   string `json:"digest"`
}
type Options struct{ ExpectedAnchor *Position }
type Request struct {
	ID       string   `json:"id"`
	Expected Position `json:"expected"`
	Approval string   `json:"approval"`
}
type InstanceObservation struct {
	TreeID     string `json:"tree_id"`
	InstanceID string `json:"instance_id"`
}
type Receipt struct {
	OperationID    string                     `json:"operation_id"`
	SessionID      string                     `json:"session_id"`
	Mode           string                     `json:"mode"`
	Outcome        string                     `json:"outcome"`
	Revision       uint64                     `json:"revision"`
	Generation     uint64                     `json:"generation"`
	ManifestDigest string                     `json:"manifest_digest"`
	Instances      []InstanceObservation      `json:"instances"`
	Noop           bool                       `json:"noop"`
	HistoricalOnly bool                       `json:"historical_only"`
	FailureKind    string                     `json:"failure_kind"`
	FailureCodes   []string                   `json:"failure_codes"`
	FailureStage   string                     `json:"failure_stage"`
	FailureTree    string                     `json:"failure_tree"`
	Conflict       *forest.GenerationConflict `json:"conflict"`
}
type Revision struct {
	ID           uint64   `json:"id"`
	Source       string   `json:"source"`
	Manifest     Manifest `json:"manifest"`
	Digest       string   `json:"digest"`
	FromRevision uint64   `json:"from_revision"`
}
type Intent struct {
	Request        Request  `json:"request"`
	SessionID      string   `json:"session_id"`
	Mode           string   `json:"mode"`
	Target         Manifest `json:"target"`
	TargetDigest   string   `json:"target_digest"`
	SourceRevision uint64   `json:"source_revision"`
	BaseGeneration uint64   `json:"base_generation"`
	BaseDigest     string   `json:"base_digest"`
	Plan           Plan     `json:"plan"`
}
type Inspection struct {
	Position     Position  `json:"position"`
	Desired      *Revision `json:"desired"`
	Pending      *Intent   `json:"pending"`
	LastObserved *Receipt  `json:"last_observed"`
	LastResult   *Receipt  `json:"last_result"`
	Incomplete   bool      `json:"incomplete"`
	SessionID    string    `json:"session_id"`
}
type record struct {
	Sequence       uint64    `json:"sequence"`
	StoreID        string    `json:"store_id"`
	PreviousDigest string    `json:"previous_digest"`
	Kind           string    `json:"kind"`
	Timestamp      string    `json:"timestamp"`
	Request        *Request  `json:"request"`
	RequestDigest  string    `json:"request_digest"`
	Target         *Manifest `json:"target"`
	SessionID      string    `json:"session_id"`
	Intent         *Intent   `json:"intent"`
	Receipt        *Receipt  `json:"receipt"`
	Choice         string    `json:"choice"`
	PendingID      string    `json:"pending_id"`
	FromRevision   uint64    `json:"from_revision"`
}
type requestResult struct {
	digest  string
	receipt Receipt
}
type journalState struct {
	inspection      Inspection
	history         []Revision
	requests        map[string]requestResult
	pendingSequence uint64
	pendingHash     string
	runtimeDigest   string
	generation      uint64
	sessionActive   bool
	sessions        map[string]bool
	sessionBase     Position
	hasAttempt      bool
	entries         []Entry
}

// Entry is immutable audit evidence. A receipt describes an observation in the
// recorded SessionID, never proof that its physical objects are alive now.
type Entry struct {
	Position                                                                            Position
	Timestamp, Kind, SessionID, OperationID, Approval, RelatedOperationID, TargetDigest string
	Receipt                                                                             *Receipt
}

func initialState() journalState {
	return journalState{history: []Revision{}, requests: map[string]requestResult{}, sessions: map[string]bool{}}
}
func detached[T any](v T) T {
	data, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(data, &out)
	return out
}
func (s journalState) clone() journalState {
	n := s
	n.inspection = detached(s.inspection)
	n.history = detached(s.history)
	n.entries = detached(s.entries)
	n.requests = make(map[string]requestResult, len(s.requests))
	n.sessions = make(map[string]bool, len(s.sessions))
	for id, v := range s.sessions {
		n.sessions[id] = v
	}
	for id, r := range s.requests {
		r.receipt = detached(r.receipt)
		n.requests[id] = r
	}
	return n
}

type journalFile interface {
	ReadAt([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
	Seek(int64, int) (int64, error)
}
type Store struct {
	mu                      sync.Mutex
	file                    journalFile
	lock                    *os.File
	unlock                  func() error
	state                   journalState
	end                     int64
	closed, poisoned, owned bool
	liveSession             string
}

func randomID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return hex.EncodeToString(b[:]), nil
}
func validRequest(r Request) bool {
	return validID(r.ID) && r.Approval != "" && len(r.Approval) <= 4096 && utf8.ValidString(r.Approval) && !strings.ContainsRune(r.Approval, 0)
}
func requestHash(mode string, r Request, target Manifest, source uint64) (string, error) {
	data, e := json.Marshal(struct {
		Mode    string
		Request Request
		Target  Manifest
		Source  uint64
	}{mode, r, target, source})
	if e != nil {
		return "", e
	}
	return hash("forest-topology/request/v1", data), nil
}

// Open acquires a nonblocking OS lock. The directory must already exist on a
// trusted local filesystem. An invalid journal is never repaired or truncated.
func Open(directory string, options Options) (*Store, error) {
	dir, e := filepath.Abs(directory)
	if e != nil {
		return nil, e
	}
	if e = checkDirectory(dir); e != nil {
		return nil, e
	}
	lp := filepath.Join(dir, "lock")
	if e = checkFile(lp, true); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(lp, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	unlock, e := lockFile(lock)
	if e != nil {
		lock.Close()
		return nil, e
	}
	fail := func(err error) (*Store, error) { unlock(); lock.Close(); return nil, err }
	jp := filepath.Join(dir, "journal")
	if options.ExpectedAnchor != nil {
		if _, err := os.Lstat(jp); errors.Is(err, os.ErrNotExist) {
			return fail(ErrHistoryConflict)
		} else if err != nil {
			return fail(err)
		}
	}
	if e = checkFile(jp, true); e != nil {
		return fail(e)
	}
	file, e := os.OpenFile(jp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := e == nil
	if errors.Is(e, os.ErrExist) {
		file, e = os.OpenFile(jp, os.O_RDWR, 0600)
	}
	if e != nil {
		return fail(e)
	}
	s := &Store{file: file, lock: lock, unlock: unlock, state: initialState()}
	if created {
		id, err := randomID()
		if err == nil {
			err = s.appendLocked(context.Background(), record{Kind: "genesis", StoreID: id}, 0)
		}
		if err == nil {
			err = syncDirectory(dir)
		}
		if err != nil {
			file.Close()
			return fail(err)
		}
	} else if e = s.scan(options.ExpectedAnchor); e != nil {
		file.Close()
		return fail(e)
	}
	if options.ExpectedAnchor != nil && created {
		file.Close()
		return fail(ErrHistoryConflict)
	}
	if _, e = file.Seek(s.end, io.SeekStart); e != nil {
		file.Close()
		return fail(e)
	}
	return s, nil
}
func checkDirectory(dir string) error {
	for p := dir; ; p = filepath.Dir(p) {
		info, e := os.Lstat(p)
		if e != nil {
			return e
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || isReparse(info) {
			return ErrUnsupportedPlatform
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	return nil
}
func checkFile(path string, missing bool) error {
	info, e := os.Lstat(path)
	if missing && errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() || isReparse(info) {
		return ErrUnsupportedPlatform
	}
	return nil
}
func frame(r record) ([]byte, string, error) {
	payload, e := json.Marshal(r)
	if e != nil || len(payload) > maxFrameBytes {
		return nil, "", ErrInvalidManifest
	}
	data := make([]byte, 16+len(payload))
	copy(data, frameMagic)
	binary.BigEndian.PutUint64(data[8:16], uint64(len(payload)))
	copy(data[16:], payload)
	sum := hash("forest-topology/record/v1", data)
	raw, _ := hex.DecodeString(sum)
	return append(data, raw...), sum, nil
}
func (s *Store) scan(anchor *Position) error {
	info, e := s.file.Stat()
	if e != nil {
		return e
	}
	size := info.Size()
	if size == 0 || size > maxJournalBytes {
		return &Error{Stage: "scan-size", Cause: ErrCorruptJournal}
	}
	found := anchor == nil
	for off := int64(0); off < size; {
		bad := func(cause error) error {
			return &Error{Stage: "scan", Sequence: s.state.inspection.Position.Sequence + 1, Offset: off, Cause: errors.Join(ErrCorruptJournal, cause), Prefix: ptrInspection(s.state.inspection)}
		}
		if s.state.inspection.Position.Sequence >= maxRecords || size-off < 48 {
			return bad(io.ErrUnexpectedEOF)
		}
		var header [16]byte
		if _, e = s.file.ReadAt(header[:], off); e != nil {
			return bad(e)
		}
		length := binary.BigEndian.Uint64(header[8:])
		if string(header[:8]) != frameMagic || length > maxFrameBytes || length > uint64(size-off-48) {
			return bad(ErrUnsupportedFormat)
		}
		data := make([]byte, 16+int(length))
		copy(data, header[:])
		if _, e = s.file.ReadAt(data[16:], off+16); e != nil {
			return bad(e)
		}
		var sum [32]byte
		if _, e = s.file.ReadAt(sum[:], off+int64(len(data))); e != nil {
			return bad(e)
		}
		digest := hash("forest-topology/record/v1", data)
		if hex.EncodeToString(sum[:]) != digest {
			return bad(fmt.Errorf("frame checksum"))
		}
		var r record
		if e = strict(data[16:], &r, maxFrameBytes); e != nil {
			return bad(e)
		}
		next := s.state.clone()
		if e = next.reduce(r, digest); e != nil {
			return bad(e)
		}
		s.state = next
		if anchor != nil && r.Sequence == anchor.Sequence {
			if s.state.inspection.Position != *anchor {
				return bad(ErrHistoryConflict)
			}
			found = true
		}
		off += int64(len(data) + 32)
		s.end = off
	}
	if !found {
		return ErrHistoryConflict
	}
	return nil
}
func ptrInspection(v Inspection) *Inspection { n := detached(v); n.Incomplete = true; return &n }
func (s *Store) appendLocked(ctx context.Context, r record, reserve int64) error {
	if s.closed || s.poisoned {
		return ErrPersistenceUnknown
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	pos := s.state.inspection.Position
	r.Sequence = pos.Sequence + 1
	r.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	r.PreviousDigest = pos.Digest
	if r.Kind != "genesis" {
		r.StoreID = pos.StoreID
	}
	data, digest, e := frame(r)
	if e != nil {
		return e
	}
	if r.Sequence > maxRecords || s.end+int64(len(data))+reserve > maxJournalBytes || (reserve > 0 && r.Sequence >= maxRecords) {
		return fmt.Errorf("%w: journal capacity", ErrInvalidManifest)
	}
	next := s.state.clone()
	if e = next.reduce(r, digest); e != nil {
		return e
	}
	for len(data) > 0 {
		n, err := s.file.Write(data)
		if n < 0 || n > len(data) {
			err = io.ErrShortWrite
			n = 0
		}
		if err != nil || n == 0 {
			s.poisoned = true
			return &Error{Stage: "write", Sequence: r.Sequence, Offset: s.end, Cause: errors.Join(ErrPersistenceUnknown, err, io.ErrShortWrite)}
		}
		data = data[n:]
	}
	if e = s.file.Sync(); e != nil {
		s.poisoned = true
		return &Error{Stage: "sync", Sequence: r.Sequence, Offset: s.end, Cause: errors.Join(ErrPersistenceUnknown, e)}
	}
	// No cancellation check can undo a completed Sync. Report its actual result.
	s.state = next
	info, e := s.file.Stat()
	if e != nil {
		s.poisoned = true
		return errors.Join(ErrPersistenceUnknown, e)
	}
	s.end = info.Size()
	return nil
}
func (s *Store) checkLocked(expected Position) error {
	if s.closed || s.poisoned {
		return ErrPersistenceUnknown
	}
	if s.state.inspection.Position != expected {
		return ErrHistoryConflict
	}
	return nil
}
func (s *Store) Inspect() Inspection {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := detached(s.state.inspection)
	v.Incomplete = s.poisoned
	if v.LastObserved != nil {
		v.LastObserved.HistoricalOnly = v.LastObserved.SessionID != s.liveSession
	}
	if v.LastResult != nil {
		v.LastResult.HistoricalOnly = v.LastResult.SessionID != s.liveSession
	}
	return v
}
func (s *Store) History() []Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return detached(s.state.history)
}

// Journal returns detached audit entries, including failed attempts and the
// host's explicit reconciliation note. It never returns an unverified tail.
func (s *Store) Journal() []Entry { s.mu.Lock(); defer s.mu.Unlock(); return detached(s.state.entries) }
func (s *Store) duplicate(id, digest string) (Receipt, bool, error) {
	r, ok := s.state.requests[id]
	if ok && r.digest != digest {
		return Receipt{}, true, ErrRequestConflict
	}
	if ok {
		return detached(r.receipt), true, nil
	}
	if p := s.state.inspection.Pending; p != nil && p.Request.ID == id {
		if s.state.pendingHash != digest {
			return Receipt{}, true, ErrRequestConflict
		}
		return Receipt{}, true, ErrReconciliationRequired
	}
	return Receipt{}, false, nil
}
func (s *Store) Seed(ctx context.Context, request Request, m Manifest) (Receipt, error) {
	n, e := Normalize(m)
	if e != nil {
		return Receipt{}, e
	}
	if !validRequest(request) {
		return Receipt{}, ErrInvalidManifest
	}
	h, e := requestHash("seed", request, n, 0)
	if e != nil {
		return Receipt{}, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned {
		return Receipt{}, ErrPersistenceUnknown
	}
	if r, ok, e := s.duplicate(request.ID, h); ok {
		return r, e
	}
	if s.owned {
		return Receipt{}, ErrStoreBusy
	}
	if e = s.checkLocked(request.Expected); e != nil {
		return Receipt{}, e
	}
	if len(s.state.history) != 0 || s.state.inspection.Pending != nil {
		return Receipt{}, ErrHistoryConflict
	}
	e = s.appendLocked(ctx, record{Kind: "seed", Request: &request, RequestDigest: h, Target: &n}, 0)
	if e != nil {
		return Receipt{}, e
	}
	return detached(s.state.requests[request.ID].receipt), nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owned {
		return ErrStoreBusy
	}
	return s.closeLocked()
}

// Resolve records a host choice, never an assertion that the missing Apply
// succeeded. No Controller may own the Store during reconciliation.
func (s *Store) Resolve(ctx context.Context, request Request, pendingID, choice string) (Receipt, error) {
	if !validRequest(request) || !validID(pendingID) || (choice != "previous" && choice != "target") {
		return Receipt{}, ErrInvalidManifest
	}
	h, _ := requestHash("resolve/"+choice+"/"+pendingID, request, Empty(), 0)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned {
		return Receipt{}, ErrPersistenceUnknown
	}
	if result, ok, e := s.duplicate(request.ID, h); ok {
		return result, e
	}
	if s.owned {
		return Receipt{}, ErrStoreBusy
	}
	if e := s.checkLocked(request.Expected); e != nil {
		return Receipt{}, e
	}
	if s.state.inspection.Pending == nil || s.state.inspection.Pending.Request.ID != pendingID {
		return Receipt{}, ErrHistoryConflict
	}
	if e := s.appendLocked(ctx, record{Kind: "resolved", Request: &request, RequestDigest: h, Choice: choice, PendingID: pendingID}, 0); e != nil {
		return Receipt{}, e
	}
	return detached(s.state.requests[request.ID].receipt), nil
}

// SelectHistorical chooses a desired structure only while no Controller owns
// the Store. It does not assert that the old structure's effects were undone.
// The host must join the old process/execution before authorizing recreation.
func (s *Store) SelectHistorical(ctx context.Context, request Request, revision uint64) (Receipt, error) {
	if !validRequest(request) {
		return Receipt{}, ErrInvalidManifest
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned {
		return Receipt{}, ErrPersistenceUnknown
	}
	var target *Manifest
	for _, r := range s.state.history {
		if r.ID == revision {
			copy := detached(r.Manifest)
			target = &copy
			break
		}
	}
	if target == nil {
		return Receipt{}, ErrHistoryConflict
	}
	h, _ := requestHash("select-historical", request, *target, revision)
	if result, ok, e := s.duplicate(request.ID, h); ok {
		return result, e
	}
	if s.owned {
		return Receipt{}, ErrStoreBusy
	}
	if e := s.checkLocked(request.Expected); e != nil {
		return Receipt{}, e
	}
	if s.state.inspection.Pending != nil {
		return Receipt{}, ErrReconciliationRequired
	}
	if e := s.appendLocked(ctx, record{Kind: "selected", Request: &request, RequestDigest: h, Target: target, FromRevision: revision}, 0); e != nil {
		return Receipt{}, e
	}
	return detached(s.state.requests[request.ID].receipt), nil
}
func (s *Store) closeLocked() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.file.Close(), s.unlock(), s.lock.Close())
}
