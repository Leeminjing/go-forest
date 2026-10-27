package topology

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func rewriteRecords(t *testing.T, directory string, mutate func([]record) []record) {
	t.Helper()
	path := filepath.Join(directory, "journal")
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var records []record
	for len(data) > 0 {
		n := int(binary.BigEndian.Uint64(data[8:16]))
		var r record
		if e = json.Unmarshal(data[16:16+n], &r); e != nil {
			t.Fatal(e)
		}
		records = append(records, r)
		data = data[16+n+32:]
	}
	records = mutate(records)
	var output []byte
	var previous string
	for _, r := range records {
		r.PreviousDigest = previous
		frame, digest, e := frame(r)
		if e != nil {
			t.Fatal(e)
		}
		output = append(output, frame...)
		previous = digest
	}
	if e = os.WriteFile(path, output, 0600); e != nil {
		t.Fatal(e)
	}
}

func TestCorruptOrOutdatedJournalFailsClosed(t *testing.T) {
	for _, kind := range []string{"truncated", "bit-flip", "duplicate", "missing-middle", "unknown-kind", "unknown-version", "oversized-frame", "old-backup"} {
		t.Run(kind, func(t *testing.T) {
			var builds atomic.Int32
			catalog, m := numberCatalog(t, &builds)
			directory := t.TempDir()
			s, e := Open(directory, Options{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
			if e != nil {
				t.Fatal(e)
			}
			initial, e := os.ReadFile(filepath.Join(directory, "journal"))
			if e != nil {
				t.Fatal(e)
			}
			f := forest.New(forest.Options{})
			c, e := NewController(f, s, catalog)
			if e != nil {
				t.Fatal(e)
			}
			_, e = c.Restore(context.Background(), Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"})
			if e != nil {
				t.Fatal(e)
			}
			if e = c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
			s, e = Open(directory, Options{})
			if e != nil {
				t.Fatal(e)
			}
			anchor := s.Inspect().Position
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(directory, "journal")
			data, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "truncated":
				e = os.Truncate(path, int64(len(data)-1))
			case "bit-flip":
				data[20] ^= 1
				e = os.WriteFile(path, data, 0600)
			case "duplicate":
				rewriteRecords(t, directory, func(rs []record) []record { return append(rs, rs[len(rs)-1]) })
			case "missing-middle":
				rewriteRecords(t, directory, func(rs []record) []record { return append(rs[:3], rs[4:]...) })
			case "unknown-kind":
				rewriteRecords(t, directory, func(rs []record) []record { rs[1].Kind = "future-record"; return rs })
			case "unknown-version":
				data[7] = '2'
				e = os.WriteFile(path, data, 0600)
			case "oversized-frame":
				binary.BigEndian.PutUint64(data[8:16], ^uint64(0))
				e = os.WriteFile(path, data, 0600)
			case "old-backup":
				e = os.WriteFile(path, initial, 0600)
			}
			if e != nil {
				t.Fatal(e)
			}
			damaged, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			options := Options{}
			if kind == "old-backup" {
				options.ExpectedAnchor = &anchor
			}
			opened, e := Open(directory, options)
			if e == nil {
				opened.Close()
				t.Fatal("bad journal accepted")
			}
			if kind == "old-backup" {
				if !errors.Is(e, ErrHistoryConflict) {
					t.Fatal(e)
				}
			} else if !errors.Is(e, ErrCorruptJournal) {
				t.Fatal(e)
			}
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(after, damaged) {
				t.Fatal("Open rewrote corrupt evidence")
			}
		})
	}
}

func TestInvalidRecordCannotContaminateVerifiedPrefix(t *testing.T) {
	directory := t.TempDir()
	s, e := Open(directory, Options{})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, Empty())
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	rewriteRecords(t, directory, func(records []record) []record { records[1].SessionID = "unexpected-field-on-seed"; return records })
	_, e = Open(directory, Options{})
	var detail *Error
	if !errors.Is(e, ErrCorruptJournal) || !errors.As(e, &detail) || detail.Prefix == nil {
		t.Fatalf("expected corruption prefix: %v", e)
	}
	if detail.Prefix.Desired != nil || detail.Prefix.Position.Sequence != 1 {
		t.Fatalf("unverified record entered prefix: %+v", detail.Prefix)
	}
}

func TestExpectedAnchorDoesNotInitializeAnUnknownDirectory(t *testing.T) {
	directory := t.TempDir()
	_, e := Open(directory, Options{ExpectedAnchor: &Position{StoreID: "expected", Sequence: 2, Digest: "expected"}})
	if !errors.Is(e, ErrHistoryConflict) {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(directory, "journal")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("anchor failure initialized a different store")
	}
}
