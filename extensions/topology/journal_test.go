package topology

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalSeedCASLockAndDetachedHistory(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir, Options{})
	if e != nil {
		t.Fatal(e)
	}
	if other, e := Open(dir, Options{}); !errors.Is(e, ErrStoreBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("second owner: %v", e)
	}
	head := s.Inspect().Position
	r, e := s.Seed(context.Background(), Request{ID: "seed", Expected: head, Approval: "offline desired topology"}, Empty())
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.Seed(context.Background(), Request{ID: "seed", Expected: head, Approval: "offline desired topology"}, Empty())
	if e != nil || again.Revision != r.Revision {
		t.Fatalf("idempotent seed: %+v %v", again, e)
	}
	if _, e = s.Seed(context.Background(), Request{ID: "seed", Expected: head, Approval: "different"}, Empty()); !errors.Is(e, ErrRequestConflict) {
		t.Fatalf("request: %v", e)
	}
	anchor := s.Inspect().Position
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir, Options{ExpectedAnchor: &anchor})
	if e != nil {
		t.Fatal(e)
	}
	h := s.History()
	if len(h) != 1 || h[0].Source != "desired-only" {
		t.Fatalf("history: %+v", h)
	}
	h[0].Source = "mutated"
	if s.History()[0].Source != "desired-only" {
		t.Fatal("history alias")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(dir, "journal"), os.O_WRONLY|os.O_APPEND, 0)
	if e != nil {
		t.Fatal(e)
	}
	f.Write([]byte{1, 2, 3})
	f.Close()
	if s, e = Open(dir, Options{}); !errors.Is(e, ErrCorruptJournal) || s != nil {
		t.Fatalf("tail accepted: %v", e)
	}
	var detail *Error
	if !errors.As(e, &detail) || detail.Offset <= 0 {
		t.Fatalf("missing failed offset: %v", e)
	}
}
