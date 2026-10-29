package topology

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestConfirmedFactsDoNotBorrowMutableProposalPorts(t *testing.T) {
	catalog, initial := orderedFixture(t, forest.Runtime)
	c, store, _, _ := managed(t, catalog, initial)
	target := detached(initial)
	target.Trees[0].Definition.Version = "2"
	proposal := commitTarget(t, c, target)
	changed := false
	for i := range proposal.Planning.Facts {
		if len(proposal.Planning.Facts[i].Ports) > 0 {
			proposal.Planning.Facts[i].Ports[0].Phase = forest.Construction
			changed = true
		}
	}
	if !changed {
		t.Fatal("fixture did not contain a real port fact")
	}
	next, e := c.Preview(initial)
	if e != nil {
		t.Fatal(e)
	}
	before := store.Inspect().Position
	if _, e = c.Reconfigure(context.Background(), Request{ID: "after-caller-mutation", Expected: next.Expected, Approval: "original consumer facts are unchanged"}, next); e != nil {
		t.Fatalf("caller mutation poisoned future valid history at %d: %v", before.Sequence, e)
	}
}

func TestJournalQueriesRemainDeeplyDetached(t *testing.T) {
	catalog, m := orderedFixture(t, forest.Runtime)
	_, s, _, _ := managed(t, catalog, m)
	expectedInspection, expectedHistory, expectedJournal := s.Inspect(), s.History(), s.Journal()
	inspection, history, journal := s.Inspect(), s.History(), s.Journal()
	inspection.Position.Digest = "caller"
	inspection.Desired.Manifest.Trees[0].ID = "caller"
	inspection.LastObserved.Instances[0].InstanceID = "caller"
	history[0].Manifest.Bindings[0].Provider = "caller"
	for i := range journal {
		if journal[i].Receipt != nil && len(journal[i].Receipt.Instances) > 0 {
			journal[i].Receipt.Instances[0].InstanceID = "caller"
		}
		if journal[i].Plan != nil && len(journal[i].Plan.Operations) > 0 {
			journal[i].Plan.Operations[0].Tree = "caller"
		}
		if journal[i].Planning != nil {
			for j := range journal[i].Planning.Facts {
				if len(journal[i].Planning.Facts[j].Ports) > 0 {
					journal[i].Planning.Facts[j].Ports[0].ID = "caller"
				}
			}
		}
	}
	if !reflect.DeepEqual(expectedInspection, s.Inspect()) || !reflect.DeepEqual(expectedHistory, s.History()) || !reflect.DeepEqual(expectedJournal, s.Journal()) {
		t.Fatal("public return values changed confirmed journal state")
	}
}

func TestFailedSyncKeepsConfirmedStateUntilReopen(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir, Options{})
	if e != nil {
		t.Fatal(e)
	}
	seed, e := s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, Empty())
	if e != nil {
		t.Fatal(e)
	}
	before, history, journal := s.Inspect(), s.History(), s.Journal()
	s.file = &faultFile{journalFile: s.file, kind: "selected", stage: "sync"}
	_, e = s.SelectHistorical(context.Background(), Request{ID: "uncertain", Expected: before.Position, Approval: "complete write with failed Sync"}, seed.Revision)
	if !errors.Is(e, ErrPersistenceUnknown) {
		t.Fatal(e)
	}
	before.Incomplete = true
	if !reflect.DeepEqual(before, s.Inspect()) || !reflect.DeepEqual(history, s.History()) || !reflect.DeepEqual(journal, s.Journal()) {
		t.Fatal("unconfirmed record changed public state")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	// A full frame can be visible despite failed Sync. Recovery validates bytes;
	// it must not invent absence from the prior uncertainty report.
	reopened, e := Open(dir, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if reopened.Inspect().Position.Sequence != before.Position.Sequence+1 || len(reopened.History()) != len(history)+1 {
		t.Fatal("valid complete frame was discarded")
	}
}

func TestLateRecordValidationPreservesExactPrefix(t *testing.T) {
	for _, kind := range []string{"intent", "apply-observed"} {
		t.Run(kind, func(t *testing.T) {
			catalog, m := orderedFixture(t, forest.Runtime)
			c, s, _, _ := managed(t, catalog, m)
			target := detached(m)
			target.Trees[0].Definition.Version = "2"
			commitTarget(t, c, target)
			dir := filepathDir(t, s)
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
			var prefix []record
			// Use the final matching event so the prefix also contains real prior facts,
			// history, receipts and runtime-generation evidence.
			rewriteRecords(t, dir, func(records []record) []record {
				index := -1
				for i := range records {
					if records[i].Kind == kind {
						index = i
					}
				}
				if index < 0 {
					t.Fatal("missing native fixture record")
				}
				prefix = append([]record(nil), records[:index]...)
				if kind == "intent" {
					records[index].Choice = "wrong-kind-known-field"
				} else {
					records[index].Receipt.Revision++
				}
				return records[:index+1]
			})
			prefixDir := t.TempDir()
			var data []byte
			for _, r := range prefix {
				frame, _, e := frame(r)
				if e != nil {
					t.Fatal(e)
				}
				data = append(data, frame...)
			}
			if e := os.WriteFile(filepath.Join(prefixDir, "journal"), data, 0600); e != nil {
				t.Fatal(e)
			}
			good, e := Open(prefixDir, Options{})
			if e != nil {
				t.Fatal(e)
			}
			want := good.Inspect()
			want.Incomplete = true
			// Error.Prefix is the raw verified diagnostic, while Inspect derives
			// HistoricalOnly for a Store without a live Controller session.
			if want.LastObserved != nil {
				want.LastObserved.HistoricalOnly = false
			}
			if want.LastResult != nil {
				want.LastResult.HistoricalOnly = false
			}
			if e = good.Close(); e != nil {
				t.Fatal(e)
			}
			bad, e := Open(dir, Options{})
			if bad != nil {
				bad.Close()
				t.Fatal("invalid store exposed")
			}
			var detail *Error
			if !errors.Is(e, ErrCorruptJournal) || !errors.As(e, &detail) || detail.Prefix == nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(*detail.Prefix, want) {
				t.Fatalf("late %s contaminated verified prefix", kind)
			}
			detail.Prefix.Desired.Manifest.Trees[0].ID = "caller"
			_, again := Open(dir, Options{})
			var repeated *Error
			if !errors.As(again, &repeated) || repeated.Prefix == nil || !reflect.DeepEqual(*repeated.Prefix, want) {
				t.Fatal("caller mutation changed a subsequent verified error prefix", again)
			}
		})
	}
}

func TestFailedReceiptAndCachedRetryRemainDeeplyDetached(t *testing.T) {
	// A constructor can return a typed native failure. Exercise real Apply and
	// durable failed-receipt recording, rather than manufacturing stored state.
	definition := forest.Definition{TypeID: "fails", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return nil, &forest.GenerationConflict{Expected: 12, Actual: 13}
	}}
	catalog, e := NewCatalog([]Registration{{Definition: definition, ContractKey: "fails", ImplementationDigest: strings.Repeat("a", 64)}})
	if e != nil {
		t.Fatal(e)
	}
	c, store, _, _ := managed(t, catalog, Empty())
	target := Empty()
	target.Trees = append(target.Trees, Tree{ID: "failure", Definition: definition.Ref(), ContractKey: "fails", ImplementationDigest: strings.Repeat("a", 64)})
	proposal, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	request := Request{ID: "failing-constructor", Expected: proposal.Expected, Approval: "record native failure"}
	result, e := c.Reconfigure(context.Background(), request, proposal)
	if !errors.Is(e, forest.ErrGenerationConflict) || result.Conflict == nil || len(result.FailureCodes) == 0 {
		t.Fatalf("missing actual failed receipt: %+v %v", result, e)
	}
	want := detached(result)
	inspection, history, journal := store.Inspect(), store.History(), store.Journal()
	mutate := func(r *Receipt) { r.Conflict.Expected = 999; r.FailureCodes[0] = "caller" }
	mutate(&result)
	for i := 0; i < 2; i++ {
		retry, e := c.Reconfigure(context.Background(), request, proposal)
		// HistoricalOnly is a documented projection of a failed cached receipt.
		want.HistoricalOnly = true
		if !errors.Is(e, forest.ErrGenerationConflict) || !reflect.DeepEqual(retry, want) {
			t.Fatalf("cached receipt changed: %+v %v", retry, e)
		}
		mutate(&retry)
	}
	query := store.Inspect()
	mutate(query.LastResult)
	entries := store.Journal()
	for i := range entries {
		if entries[i].Receipt != nil && entries[i].Receipt.Conflict != nil {
			mutate(entries[i].Receipt)
		}
	}
	if !reflect.DeepEqual(inspection, store.Inspect()) || !reflect.DeepEqual(history, store.History()) || !reflect.DeepEqual(journal, store.Journal()) {
		t.Fatal("returned error receipt or cached retry mutated confirmed state")
	}
}

type syncEffectFile struct {
	journalFile
	cancel   context.CancelFunc
	failStat bool
	synced   bool
}

func (f *syncEffectFile) Sync() error {
	if e := f.journalFile.Sync(); e != nil {
		return e
	}
	f.synced = true
	if f.cancel != nil {
		f.cancel()
	}
	return nil
}
func (f *syncEffectFile) Stat() (os.FileInfo, error) {
	if f.synced && f.failStat {
		return nil, io.ErrClosedPipe
	}
	return f.journalFile.Stat()
}

func TestCompletedSyncPublishesDespiteCancellationOrLaterStatFailure(t *testing.T) {
	for _, statFail := range []bool{false, true} {
		t.Run(fmt.Sprint("stat-", statFail), func(t *testing.T) {
			dir := t.TempDir()
			s, e := Open(dir, Options{})
			if e != nil {
				t.Fatal(e)
			}
			seed, e := s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, Empty())
			if e != nil {
				t.Fatal(e)
			}
			before := s.Inspect().Position
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.file = &syncEffectFile{journalFile: s.file, cancel: cancel, failStat: statFail}
			result, e := s.SelectHistorical(ctx, Request{ID: "after-sync", Expected: before, Approval: "completed synchronization is not undone"}, seed.Revision)
			if ctx.Err() != context.Canceled {
				t.Fatal("real Sync did not trigger cancellation")
			}
			if statFail {
				if !errors.Is(e, ErrPersistenceUnknown) {
					t.Fatal(e)
				}
			} else if e != nil || result.Revision != before.Sequence+1 {
				t.Fatal(result, e)
			}
			after := s.Inspect()
			if after.Position.Sequence != before.Sequence+1 || after.Incomplete != statFail || len(s.History()) != 2 {
				t.Fatal("synced state was discarded or falsely confirmed", after)
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			reopened, e := Open(dir, Options{})
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			if reopened.Inspect().Position != after.Position || len(reopened.History()) != 2 {
				t.Fatal("reopen lost completed durable selection")
			}
		})
	}
}
