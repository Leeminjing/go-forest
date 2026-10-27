package topology

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

type faultFile struct {
	journalFile
	kind, stage, lastKind string
	fired                 bool
}

type shortFile struct{ journalFile }

func (f shortFile) Write(data []byte) (int, error) {
	if len(data) > 13 {
		data = data[:13]
	}
	return f.journalFile.Write(data)
}

func TestPartialProgressWritesFormCompleteDurableRecords(t *testing.T) {
	s, e := Open(t.TempDir(), Options{})
	if e != nil {
		t.Fatal(e)
	}
	directory := filepathDir(t, s)
	s.file = shortFile{s.file}
	_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "short writes must be completed"}, Empty())
	if e != nil {
		t.Fatal(e)
	}
	position := s.Inspect().Position
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(directory, Options{ExpectedAnchor: &position})
	if e != nil {
		t.Fatal(e)
	}
	if len(s.History()) != 1 {
		t.Fatal("short writes lost a selected structure")
	}
	s.Close()
}

func (f *faultFile) Write(p []byte) (int, error) {
	if len(p) >= 16 && string(p[:8]) == frameMagic {
		var r record
		_ = json.Unmarshal(p[16:len(p)-32], &r)
		f.lastKind = r.Kind
	}
	if !f.fired && f.lastKind == f.kind && f.stage == "write" {
		f.fired = true
		return 0, io.ErrClosedPipe
	}
	if !f.fired && f.lastKind == f.kind && f.stage == "partial" {
		f.fired = true
		n, e := f.journalFile.Write(p[:8])
		return n, errors.Join(e, io.ErrClosedPipe)
	}
	return f.journalFile.Write(p)
}
func (f *faultFile) Sync() error {
	if !f.fired && f.lastKind == f.kind && f.stage == "sync" {
		f.fired = true
		return io.ErrClosedPipe
	}
	return f.journalFile.Sync()
}

func TestPersistenceFailuresNeverBlindlyReapply(t *testing.T) {
	for _, scenario := range []struct {
		kind, stage      string
		builds           int32
		pending, corrupt bool
	}{{"intent", "write", 0, false, false}, {"intent", "partial", 0, false, true}, {"intent", "sync", 0, true, false}, {"apply-observed", "write", 1, true, false}, {"apply-observed", "sync", 1, false, false}} {
		t.Run(scenario.kind+"/"+scenario.stage, func(t *testing.T) {
			ctx := context.Background()
			var builds atomic.Int32
			catalog, m := numberCatalog(t, &builds)
			dir := t.TempDir()
			s, e := Open(dir, Options{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Seed(ctx, Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
			if e != nil {
				t.Fatal(e)
			}
			f := forest.New(forest.Options{})
			c, e := NewController(f, s, catalog)
			if e != nil {
				t.Fatal(e)
			}
			s.file = &faultFile{journalFile: s.file, kind: scenario.kind, stage: scenario.stage}
			request := Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"}
			_, e = c.Restore(ctx, request)
			if !errors.Is(e, ErrPersistenceUnknown) || builds.Load() != scenario.builds {
				t.Fatalf("failure: %v builds=%d", e, builds.Load())
			}
			if e = c.OpenAdmission(ctx); e == nil {
				t.Fatal("unknown outcome admitted business")
			}
			_, e = c.Restore(ctx, request)
			if !errors.Is(e, ErrPersistenceUnknown) || builds.Load() != scenario.builds {
				t.Fatalf("blind replay %v", e)
			}
			if e = c.Close(ctx); e != nil {
				t.Fatal(e)
			}
			s, e = Open(dir, Options{})
			if scenario.corrupt {
				if !errors.Is(e, ErrCorruptJournal) {
					t.Fatalf("partial frame: %v", e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if (s.Inspect().Pending != nil) != scenario.pending {
				t.Fatalf("scan: %+v", s.Inspect())
			}
			if scenario.pending {
				if _, e = NewController(forest.New(forest.Options{}), s, catalog); !errors.Is(e, ErrReconciliationRequired) {
					t.Fatalf("automatic recovery: %v", e)
				}
				_, e = s.Resolve(ctx, Request{ID: "host-review", Expected: s.Inspect().Position, Approval: "old execution exited; resources checked"}, "boot", "target")
				if e != nil {
					t.Fatal(e)
				}
				if s.Inspect().Desired.Source != "host-selected" || s.Inspect().Pending != nil {
					t.Fatal("resolution fabricated success")
				}
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
