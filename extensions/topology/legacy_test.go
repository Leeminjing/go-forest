package topology

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestReleasedV1JournalRetriesAndMixedVersionHistory(t *testing.T) {
	var fixture struct {
		Seed        Receipt
		Boot        Request
		BootReceipt Receipt
		Attempts    []struct {
			Request  Request
			Proposal Proposal
			Receipt  Receipt
		}
		Position Position
	}
	raw, e := os.ReadFile("testdata/legacy-v0.1/history/evidence.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &fixture); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	original, e := os.ReadFile("testdata/legacy-v0.1/history/journal")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "journal"), original, 0600); e != nil {
		t.Fatal(e)
	}
	s, e := Open(dir, Options{ExpectedAnchor: &fixture.Position})
	if e != nil {
		t.Fatal(e)
	}
	var builds atomic.Int32
	catalog, _ := numberCatalog(t, &builds)
	first := catalog.entries[forest.DefinitionRef{TypeID: "number", Version: "1"}]
	for _, version := range []string{"2", "bad"} {
		r := first
		r.Definition.Version = version
		catalog.entries[r.Definition.Ref()] = r
	}
	c, e := NewController(forest.New(forest.Options{}), s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	boot, e := c.Restore(context.Background(), fixture.Boot)
	if e != nil || !boot.HistoricalOnly || boot.Generation != fixture.BootReceipt.Generation {
		t.Fatal("legacy restore retry", boot, e)
	}
	for _, a := range fixture.Attempts {
		var r Receipt
		if a.Proposal.Mode == "rollback" {
			r, e = c.Rollback(context.Background(), a.Request, a.Proposal)
		} else {
			r, e = c.Reconfigure(context.Background(), a.Request, a.Proposal)
		}
		if a.Receipt.Outcome == "failed" {
			if !errors.Is(e, forest.ErrPrepareFailed) {
				t.Fatal("legacy failure", e)
			}
		} else if e != nil {
			t.Fatal(e)
		}
		r.HistoricalOnly = false
		if !reflect.DeepEqual(r, a.Receipt) {
			t.Fatalf("legacy receipt changed: %+v != %+v", r, a.Receipt)
		}
		changed := detached(a.Proposal)
		changed.Plan.Operations = append(changed.Plan.Operations, forest.Remove("number"))
		if _, e = c.commit(context.Background(), a.Request, changed, a.Proposal.Mode); !errors.Is(e, ErrRequestConflict) {
			t.Fatal("changed old payload accepted", e)
		}
	}
	if builds.Load() != 0 || s.Inspect().Position != fixture.Position {
		t.Fatal("old request retried actual work")
	}
	if _, e = c.Restore(context.Background(), Request{ID: "new-boot", Expected: s.Inspect().Position, Approval: "old process joined; current cold restore"}); e != nil {
		t.Fatal(e)
	}
	target := s.Inspect().Desired.Manifest
	target.Trees[0].Definition.Version = "2"
	p := commitTarget(t, c, target)
	if p.Planning == nil || p.Planning.Version != 2 {
		t.Fatal("new proposal not versioned")
	}
	if e = c.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "journal"))
	if !bytes.HasPrefix(data, original) {
		t.Fatal("rewrote v1 bytes")
	}
	s, e = Open(dir, Options{})
	if e != nil {
		t.Fatal("mixed replay", e)
	}
	defer s.Close()
	if s.Inspect().Desired.Manifest.Trees[0].Definition.Version != "2" {
		t.Fatal("wrong mixed history target")
	}
}

func TestReleasedV1PendingIntentStillRequiresHostResolution(t *testing.T) {
	dir := t.TempDir()
	data, e := os.ReadFile("testdata/legacy-v0.1/pending/journal")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "journal"), data, 0600); e != nil {
		t.Fatal(e)
	}
	s, e := Open(dir, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	pending := s.Inspect().Pending
	if pending == nil || pending.Planning != nil || pending.Mode != "reconfigure" || pending.BaseGeneration != 1 {
		t.Fatal("legacy pending misinterpreted", pending)
	}
	var builds atomic.Int32
	catalog, _ := numberCatalog(t, &builds)
	if _, e = NewController(forest.New(forest.Options{}), s, catalog); !errors.Is(e, ErrReconciliationRequired) {
		t.Fatal(e)
	}
	_, e = s.Resolve(context.Background(), Request{ID: "resolve", Expected: s.Inspect().Position, Approval: "actual old producer killed and Wait completed; previous chosen"}, pending.Request.ID, "previous")
	if e != nil || builds.Load() != 0 || !strings.Contains(s.Journal()[len(s.Journal())-1].Approval, "Wait") {
		t.Fatal(e)
	}
}
