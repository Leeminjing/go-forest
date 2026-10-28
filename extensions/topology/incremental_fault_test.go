package topology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestIncrementalKillWaitRetainsPendingVersionedIntent(t *testing.T) {
	for _, stage := range []string{"intent-synced", "build", "start", "ready", "apply-returned", "observation-synced"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			resources := filepath.Join(dir, "resources")
			if e := os.Mkdir(resources, 0700); e != nil {
				t.Fatal(e)
			}
			_, m, e := fileCatalog(resources, nil)
			if e != nil {
				t.Fatal(e)
			}
			stable := m.Trees[0]
			stable.ID = "stable"
			m.Trees = append(m.Trees, stable)
			s, e := Open(dir, Options{})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "real process input"}, m); e != nil {
				t.Fatal(e)
			}
			s.Close()
			killed := superviseScenario(t, dir, stage, "incremental-crash", true)
			s, e = Open(dir, Options{})
			if e != nil {
				t.Fatal(e)
			}
			if stage == "observation-synced" {
				if s.Inspect().Pending != nil || s.Inspect().LastObserved.Generation != 2 {
					t.Fatal("lost committed incremental observation")
				}
			} else {
				pending := s.Inspect().Pending
				if pending == nil || pending.Planning == nil || pending.Planning.Version != 2 || pending.Mode != "reconfigure" || !reflect.DeepEqual(pending.Planning.Impact.RetainedTrees, []forest.TreeID{"stable"}) {
					t.Fatal("wrong pending scope", pending)
				}
				catalog, _, _ := fileCatalog(resources, nil)
				if _, e = NewController(forest.New(forest.Options{}), s, catalog); !errors.Is(e, ErrReconciliationRequired) {
					t.Fatal(e)
				}
				if _, e = s.Resolve(context.Background(), Request{ID: "resolve", Expected: s.Inspect().Position, Approval: "old worker Kill+Wait complete; resource review; select target"}, pending.Request.ID, "target"); e != nil {
					t.Fatal(e)
				}
			}
			s.Close()
			restored := supervise(t, dir, "", false)
			if restored.SessionID == killed.SessionID || restored.Generation != 1 || !strings.HasPrefix(restored.Marker, "v2:") {
				t.Fatal("cold recovery resurrected execution", restored)
			}
			t.Logf("incremental %s: Kill+Wait PID=%d, fresh PID=%d native Generation=%d", stage, killed.PID, restored.PID, restored.Generation)
		})
	}
}

func TestIncrementalPersistenceFailureDoesNotReapplyOrRebuildStableTree(t *testing.T) {
	for _, kind := range []string{"intent", "apply-observed"} {
		for _, stage := range []string{"write", "partial", "sync"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				catalog, m := orderedFixture(t, forest.Runtime)
				c, s, f, _ := managed(t, catalog, m)
				old := f.Snapshot()
				target := detached(m)
				target.Trees[0].Definition.Version = "2"
				p, e := c.Preview(target)
				if e != nil {
					t.Fatal(e)
				}
				request := Request{ID: "fault", Expected: p.Expected, Approval: "record failure test"}
				fault := &faultFile{journalFile: s.file, kind: kind, stage: stage}
				s.file = fault
				if _, e = c.Reconfigure(context.Background(), request, p); !errors.Is(e, ErrPersistenceUnknown) {
					t.Fatal(e)
				}
				if f.Snapshot().Trees["b"].InstanceID != old.Trees["b"].InstanceID || f.Snapshot().Trees["reader"].InstanceID != old.Trees["reader"].InstanceID {
					t.Fatal("failure reconstructed stable instances")
				}
				after := f.Snapshot()
				if kind == "intent" && after.Generation != 1 {
					t.Fatal("executed without durable intent")
				}
				if kind == "apply-observed" && after.Generation != 2 {
					t.Fatal("fixture did not reach native commit")
				}
				if _, e = c.Reconfigure(context.Background(), request, p); !errors.Is(e, ErrPersistenceUnknown) {
					t.Fatal(e)
				}
				if f.Snapshot().Generation != after.Generation || f.Snapshot().Trees["a"].InstanceID != after.Trees["a"].InstanceID {
					t.Fatal("blind reapply")
				}
			})
		}
	}
}

func TestVersionedPlanningEvidenceCannotBeReinterpreted(t *testing.T) {
	for _, mutation := range []string{"version", "facts", "impact", "plan", "erase"} {
		t.Run(mutation, func(t *testing.T) {
			catalog, m := orderedFixture(t, forest.Runtime)
			c, s, _, _ := managed(t, catalog, m)
			dir := filepathDir(t, s)
			target := detached(m)
			target.Trees[0].Definition.Version = "2"
			commitTarget(t, c, target)
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
			rewriteRecords(t, dir, func(records []record) []record {
				for i := range records {
					r := &records[i]
					if r.Intent == nil || r.Intent.Mode != "reconfigure" {
						continue
					}
					in := r.Intent
					switch mutation {
					case "version":
						in.Planning.Version = 999
					case "facts":
						in.Planning.Facts = nil
					case "impact":
						in.Planning.Impact.RetainedTrees = nil
					case "plan":
						in.Plan.Operations = append(in.Plan.Operations, forest.Remove("b"))
					case "erase":
						in.Planning = nil
					}
					// Recompute the outer record hash and supported request digest too: this
					// tests semantic replay, not just a mismatched frame checksum.
					if h, e := intentHash(*in); e == nil {
						r.RequestDigest = h
					}
				}
				return records
			})
			if opened, e := Open(dir, Options{}); e == nil {
				opened.Close()
				t.Fatal(fmt.Sprint("accepted invalid ", mutation))
			}
		})
	}
}
