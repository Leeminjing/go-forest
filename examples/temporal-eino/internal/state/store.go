// Package state owns the example's single-process persistence domain.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
)

type Store struct {
	mu                        sync.Mutex
	directory, owner, profile string
	active                    map[string]bool
	native                    *ef.FileStore
	owned                     bool
}
type claim struct{ Owner, Profile string }

// HandoverAfterExit does not detect death: the trusted caller has completed Wait.
func HandoverAfterExit(directory, oldOwner string) error {
	if oldOwner == "" {
		return protocol.ErrOwner
	}
	p := filepath.Join(directory, "owner.json")
	b, e := os.ReadFile(p)
	if e != nil {
		return e
	}
	var c claim
	if json.Unmarshal(b, &c) != nil || c.Owner != oldOwner || c.Profile != protocol.Profile {
		return protocol.ErrOwner
	}
	return os.Remove(p)
}
func Audit(directory string, in protocol.Input) (protocol.Evidence, error) {
	if !protocol.Valid(in) {
		return protocol.Evidence{}, protocol.ErrConflict
	}
	return (&Store{directory: directory, profile: protocol.Profile}).Inspect(context.Background(), in)
}

type envelope struct {
	Digest  string
	Payload json.RawMessage
}

func publish(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 1<<20 {
		return protocol.ErrPartial
	}
	w, e := json.Marshal(envelope{Digest: protocol.BytesHash(b), Payload: b})
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(w); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Link(f.Name(), path)
}
func read(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	if len(b) > 2<<20 {
		return protocol.ErrCorrupt
	}
	var w envelope
	if json.Unmarshal(b, &w) != nil || w.Digest != protocol.BytesHash(w.Payload) {
		return protocol.ErrCorrupt
	}
	if json.Unmarshal(w.Payload, v) != nil {
		return protocol.ErrCorrupt
	}
	return nil
}
func Definition(directory, owner, profile string, capture func(*Store)) forest.Definition {
	return forest.Definition{TypeID: "te.state", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Store]("state", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		s := &Store{directory: directory, owner: owner, profile: profile, active: map[string]bool{}}
		capture(s)
		return forest.Hooks{OnStart: func(context.Context) error { return s.open() }, OnStop: func(context.Context) error { return s.close() }}, forest.Provide(e, "state", s)
	}}
}
func (s *Store) open() error {
	if e := os.MkdirAll(s.directory, 0700); e != nil {
		return e
	}
	p := filepath.Join(s.directory, "owner.json")
	f, e := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(e, os.ErrExist) {
		return protocol.ErrOwner
	}
	if e != nil {
		return e
	}
	s.owned = true
	b, _ := json.Marshal(claim{s.owner, protocol.Profile})
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		_ = os.Remove(p)
		s.owned = false
		return e
	}
	s.native, e = ef.NewFileStore(filepath.Join(s.directory, "native"), ef.Profile{Engine: "eino/0.9.22", Namespace: "temporal-eino", Schema: s.profile})
	return e
}
func (s *Store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.owned {
		return nil
	}
	var c claim
	b, e := os.ReadFile(filepath.Join(s.directory, "owner.json"))
	if e != nil {
		return e
	}
	if json.Unmarshal(b, &c) != nil || c.Owner != s.owner {
		return protocol.ErrOwner
	}
	if len(s.active) != 0 {
		return protocol.ErrBusy
	}
	e = os.Remove(filepath.Join(s.directory, "owner.json"))
	if e == nil {
		s.owned = false
	}
	return e
}
func (s *Store) Claimed() bool                                           { s.mu.Lock(); defer s.mu.Unlock(); return s.owned }
func (s *Store) Get(ctx context.Context, k string) ([]byte, bool, error) { return s.native.Get(ctx, k) }
func (s *Store) Set(ctx context.Context, k string, b []byte) error       { return s.native.Set(ctx, k, b) }
func (s *Store) CheckpointDigest(ctx context.Context, k string) (string, error) {
	b, ok, e := s.Get(ctx, k)
	if e != nil {
		return "", e
	}
	if !ok {
		return "", protocol.ErrMissing
	}
	return protocol.BytesHash(b), nil
}
func (s *Store) recordPath(id string) string {
	return filepath.Join(s.directory, "steps", protocol.Hash(id))
}

type journal struct {
	Profile string
	Records []protocol.Record
}

func (s *Store) load(id string) (journal, int, error) {
	j := journal{Profile: s.profile}
	entries, e := os.ReadDir(s.recordPath(id))
	if errors.Is(e, os.ErrNotExist) {
		return j, 0, nil
	}
	if e != nil {
		return j, 0, e
	}
	var names []string
	for _, x := range entries {
		if strings.HasPrefix(x.Name(), ".tmp-") {
			continue
		}
		n, e := strconv.Atoi(strings.TrimSuffix(x.Name(), ".json"))
		if e != nil || x.IsDir() || n < 1 || x.Name() != fmt.Sprintf("%06d.json", n) {
			return j, 0, protocol.ErrCorrupt
		}
		names = append(names, x.Name())
	}
	if len(names) > 256 {
		return j, 0, protocol.ErrCorrupt
	}
	if len(names) == 0 {
		return j, 0, nil
	}
	sort.Strings(names)
	e = read(filepath.Join(s.recordPath(id), names[len(names)-1]), &j)
	if e != nil {
		return j, 0, e
	}
	if j.Profile != s.profile {
		return j, 0, ef.ErrProfile
	}
	return j, len(names), nil
}
func (s *Store) save(id string, j journal, n int) error {
	return publish(filepath.Join(s.recordPath(id), fmt.Sprintf("%06d.json", n+1)), j)
}
func (s *Store) Begin(ctx context.Context, r protocol.Request) (*protocol.Result, func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := r.Input.ExecutionID
	if s.active[id] {
		return nil, nil, protocol.ErrBusy
	}
	j, n, e := s.load(id)
	if e != nil {
		return nil, nil, e
	}
	if r.Step < 0 || r.Step >= 8 {
		return nil, nil, protocol.ErrConflict
	}
	if r.Step < len(j.Records) {
		old := j.Records[r.Step]
		if protocol.Hash(old.Request) != protocol.Hash(r) {
			return nil, nil, protocol.ErrConflict
		}
		if old.State == "paused" || old.State == "completed" || old.State == "stopped" {
			out := old.Result
			out.Reused = true
			return &out, func() {}, nil
		}
		if old.State == "running" {
			// No active in-process owner remains, and startup holds the exclusive
			// directory claim. Persist uncertainty rather than resetting execution.
			j.Records[r.Step].State = "unknown"
			j.Records[r.Step].Result.State = "unknown"
			if e = s.save(id, j, n); e != nil {
				return nil, nil, errors.Join(protocol.ErrUnknown, e)
			}
		}
		return nil, nil, protocol.ErrUnknown
	}
	if r.Step != len(j.Records) {
		return nil, nil, protocol.ErrConflict
	}
	if r.Step == 0 {
		if r.Action != "run" {
			return nil, nil, protocol.ErrConflict
		}
		// A checkpoint without a receipt is not permission to overwrite native state.
		if _, exists, e := s.Get(ctx, protocol.Checkpoint(r.Input)); e != nil {
			return nil, nil, e
		} else if exists {
			j.Records = append(j.Records, protocol.Record{Request: r, Owner: s.owner, State: "unknown", Result: protocol.Result{State: "unknown"}})
			if e = s.save(id, j, n); e != nil {
				return nil, nil, errors.Join(protocol.ErrUnknown, e)
			}
			return nil, nil, protocol.ErrUnknown
		}
	} else {
		last := j.Records[r.Step-1]
		p := last.Result.Pause
		d := r.Decision
		if last.State != "paused" || r.Action != "resume" || last.Request.Input != r.Input || last.Request.WorkflowID != r.WorkflowID || last.Request.RunID != r.RunID || d.ExecutionID != id || d.Token != p.Token || len(p.Targets) != 1 || d.Target != p.Targets[0] || (d.Value != "continue" && d.Value != "stop") {
			return nil, nil, protocol.ErrConflict
		}
		actual, e := s.CheckpointDigest(ctx, p.Checkpoint)
		if e != nil {
			return nil, nil, e
		}
		if actual != p.Digest {
			return nil, nil, protocol.ErrUnknown
		}
	}
	j.Records = append(j.Records, protocol.Record{Request: r, Owner: s.owner, State: "running"})
	if e = s.save(id, j, n); e != nil {
		return nil, nil, e
	}
	s.active[id] = true
	return nil, func() { s.mu.Lock(); delete(s.active, id); s.mu.Unlock() }, nil
}
func (s *Store) Seal(ctx context.Context, r protocol.Request, out protocol.Result) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, n, e := s.load(r.Input.ExecutionID)
	if e != nil {
		return e
	}
	if r.Step >= len(j.Records) || j.Records[r.Step].State != "running" {
		return protocol.ErrConflict
	}
	j.Records[r.Step].State = out.State
	j.Records[r.Step].Result = out
	return s.save(r.Input.ExecutionID, j, n)
}
func (s *Store) artifact(id string) string {
	return filepath.Join(s.directory, "artifacts", protocol.Hash(id)+".json")
}
func (s *Store) Apply(ctx context.Context, in protocol.Input, receipt protocol.Receipt) (protocol.Receipt, error) {
	if e := ctx.Err(); e != nil {
		return receipt, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var old protocol.Receipt
	e := read(s.artifact(in.OperationID), &old)
	if e == nil {
		if old.OperationID != in.OperationID || old.Digest != receipt.Digest {
			return old, protocol.ErrConflict
		}
		return old, nil
	}
	if !errors.Is(e, os.ErrNotExist) {
		return old, e
	}
	if e = publish(s.artifact(in.OperationID), receipt); e != nil {
		return old, e
	}
	return receipt, nil
}
func (s *Store) Inspect(ctx context.Context, in protocol.Input) (out protocol.Evidence, e error) {
	if e = ctx.Err(); e != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e = read(s.artifact(in.OperationID), &out.Receipt)
	if errors.Is(e, os.ErrNotExist) {
		e = nil
	} else if e == nil {
		if out.Receipt.OperationID != in.OperationID || out.Receipt.Digest != protocol.Hash(struct{ Schema, Text string }{"artifact/v1", in.Text}) {
			return out, protocol.ErrConflict
		}
		out.Artifacts = 1
	}
	if e != nil {
		return
	}
	j, _, e := s.load(in.ExecutionID)
	out.Records = j.Records
	out.Owner = s.owner
	return out, e
}
