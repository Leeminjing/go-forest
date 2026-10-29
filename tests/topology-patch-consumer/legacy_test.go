package consumer

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

	"github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
)

func TestReleasedLegacyRequestsKeepOriginalReceipts(t *testing.T) {
	ctx := context.Background()
	var fixture struct {
		Boot        topology.Request
		BootReceipt topology.Receipt
		Attempts    []struct {
			Request  topology.Request
			Proposal topology.Proposal
			Receipt  topology.Receipt
		}
		Position topology.Position
	}
	raw, err := os.ReadFile("testdata/legacy-history/evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile("testdata/legacy-history/journal")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.WriteFile(filepath.Join(dir, "journal"), original, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := topology.Open(dir, topology.Options{ExpectedAnchor: &fixture.Position})
	if err != nil {
		t.Fatal(err)
	}
	var builds atomic.Int32
	registrations := []topology.Registration{}
	for _, version := range []string{"1", "2", "bad"} {
		version := version
		d := forest.Definition{TypeID: "number", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[number]("number", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			builds.Add(1)
			return forest.Hooks{}, forest.Provide(env, "number", value(7))
		}}
		registrations = append(registrations, topology.Registration{Definition: d, ContractKey: "number/1", ImplementationDigest: strings.Repeat("a", 64)})
	}
	catalog, err := topology.NewCatalog(registrations)
	if err != nil {
		t.Fatal(err)
	}
	c, err := topology.NewController(forest.New(forest.Options{}), s, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	check := func(got, want topology.Receipt) {
		t.Helper()
		if !got.HistoricalOnly {
			t.Fatal("old observation claimed live resources")
		}
		got.HistoricalOnly = false
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("original receipt changed: %+v != %+v", got, want)
		}
	}
	boot, err := c.Restore(ctx, fixture.Boot)
	if err != nil {
		t.Fatal(err)
	}
	check(boot, fixture.BootReceipt)
	for _, attempt := range fixture.Attempts {
		var r topology.Receipt
		if attempt.Proposal.Mode == "rollback" {
			r, err = c.Rollback(ctx, attempt.Request, attempt.Proposal)
		} else {
			r, err = c.Reconfigure(ctx, attempt.Request, attempt.Proposal)
		}
		if attempt.Receipt.Outcome == "failed" {
			if !errors.Is(err, forest.ErrPrepareFailed) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		check(r, attempt.Receipt)
		changed := attempt.Proposal
		changed.Plan.Operations = append(append([]forest.Operation(nil), changed.Plan.Operations...), forest.Remove("number"))
		if changed.Mode == "rollback" {
			_, err = c.Rollback(ctx, attempt.Request, changed)
		} else {
			_, err = c.Reconfigure(ctx, attempt.Request, changed)
		}
		if !errors.Is(err, topology.ErrRequestConflict) {
			t.Fatalf("changed original request not rejected: %v", err)
		}
	}
	if builds.Load() != 0 || s.Inspect().Position != fixture.Position {
		t.Fatal("original retry executed work or appended history")
	}
	actual, err := os.ReadFile(filepath.Join(dir, "journal"))
	if err != nil || !bytes.Equal(actual, original) {
		t.Fatal("legacy bytes changed", err)
	}
	t.Logf("original boot and %d request receipts preserved; constructors=%d; sequence=%d", len(fixture.Attempts), builds.Load(), s.Inspect().Position.Sequence)
}
