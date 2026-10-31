package topology

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func copyProposal(t *testing.T, p Proposal) Proposal {
	t.Helper()
	raw, e := json.Marshal(p)
	if e != nil {
		t.Fatal(e)
	}
	var out Proposal
	if e = json.Unmarshal(raw, &out); e != nil {
		t.Fatal(e)
	}
	return out
}

func TestEveryReturnedProposalClaimIsRevalidated(t *testing.T) {
	catalog, m := orderedFixture(t, forest.Runtime)
	c, s, f, _ := managed(t, catalog, m)
	if e := c.OpenAdmission(context.Background()); e != nil {
		t.Fatal(e)
	}
	target := m
	target.Trees = append([]Tree(nil), m.Trees...)
	target.Trees[0].Definition.Version = "2"
	p, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	position, initialSnapshot := s.Inspect().Position, f.Snapshot()
	cases := []struct {
		name   string
		mutate func(*Proposal)
		want   error
	}{
		{"target-digest", func(p *Proposal) { p.TargetDigest = strings.Repeat("b", 64) }, ErrHistoryConflict},
		{"before-digest", func(p *Proposal) { p.BeforeDigest = strings.Repeat("b", 64) }, ErrHistoryConflict},
		{"current-generation", func(p *Proposal) { p.Generation++ }, ErrHistoryConflict},
		{"source-revision", func(p *Proposal) { p.SourceRevision = 1 }, ErrInvalidManifest},
		{"mode", func(p *Proposal) { p.Mode = "rollback" }, ErrInvalidManifest},
		{"position", func(p *Proposal) { p.Expected.Sequence++ }, ErrHistoryConflict},
		{"target-contract", func(p *Proposal) { p.Target.Trees[0].ContractKey = "untrusted" }, ErrDefinitionMismatch},
		{"plan", func(p *Proposal) { p.Plan.Operations[0].Definition.Version = "1" }, ErrHistoryConflict},
		{"facts", func(p *Proposal) { p.Planning.Facts[0].HotSwappable = !p.Planning.Facts[0].HotSwappable }, ErrHistoryConflict},
		{"impact", func(p *Proposal) { p.Planning.Impact.RetainedTrees = nil }, ErrHistoryConflict},
		{"nil-empty-difference", func(p *Proposal) { p.Diff.AddedTrees = nil }, ErrHistoryConflict},
		{"binding-order", func(p *Proposal) {
			p.Target.Bindings[0], p.Target.Bindings[1] = p.Target.Bindings[1], p.Target.Bindings[0]
		}, ErrHistoryConflict},
		{"plan-version", func(p *Proposal) { p.Planning.Version = 99 }, ErrUnsupportedPlanVersion},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			bad := copyProposal(t, p)
			test.mutate(&bad)
			_, e := c.Reconfigure(context.Background(), Request{ID: "tamper-" + test.name, Expected: bad.Expected, Approval: "untrusted claim"}, bad)
			if !errors.Is(e, test.want) {
				t.Fatalf("wrong rejection class: %v want %v", e, test.want)
			}
			if s.Inspect().Position != position || !reflect.DeepEqual(f.Snapshot().Trees, initialSnapshot.Trees) || f.Snapshot().Generation != initialSnapshot.Generation {
				t.Fatal("rejected claim changed confirmed state")
			}
			if e := c.Invoke(context.Background(), func(call context.Context) error {
				g, e := f.Generation(call)
				if e != nil {
					return e
				}
				if g != initialSnapshot.Generation {
					return errors.New("rejected proposal changed call generation")
				}
				return nil
			}); e != nil {
				t.Fatal("rejected proposal closed the old forest", e)
			}
		})
	}
	// Mutating the caller's original target after Preview cannot alter its result.
	target.Trees[0].ID = "caller"
	if p.Target.Trees[0].ID == "caller" {
		t.Fatal("Preview retained original input")
	}
	if _, e = c.Reconfigure(context.Background(), Request{ID: "valid-after-tampering", Expected: p.Expected, Approval: "original complete proposal"}, p); e != nil {
		t.Fatal(e)
	}
}

func TestStaleProposalAndHistoricalRetryKeepDifferentSemantics(t *testing.T) {
	catalog, m := orderedFixture(t, forest.Runtime)
	c, s, f, _ := managed(t, catalog, m)
	target := m
	target.Trees = append([]Tree(nil), m.Trees...)
	target.Trees[0].Definition.Version = "2"
	p, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	request := Request{ID: "first-update", Expected: p.Expected, Approval: "one change"}
	receipt, e := c.Reconfigure(context.Background(), request, p)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Reconfigure(context.Background(), Request{ID: "different-request", Expected: p.Expected, Approval: "stale but same logical target"}, p); !errors.Is(e, ErrHistoryConflict) {
		t.Fatal("stale proposal accepted", e)
	}
	back, e := c.Preview(m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Reconfigure(context.Background(), Request{ID: "back", Expected: back.Expected, Approval: "change current state"}, back); e != nil {
		t.Fatal(e)
	}
	position, generation := s.Inspect().Position, f.Snapshot().Generation
	again, e := c.Reconfigure(context.Background(), request, p)
	if e != nil {
		t.Fatal(e)
	}
	if !again.HistoricalOnly {
		t.Fatal("old receipt asserted current runtime")
	}
	again.HistoricalOnly = false
	if !reflect.DeepEqual(again, receipt) || s.Inspect().Position != position || f.Snapshot().Generation != generation {
		t.Fatal("historical retry was revalidated as a new operation or changed history")
	}
}
