package ecosystem

import (
	"context"
	"errors"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
	tk "github.com/Leeminjing/go-forest/testkit"
)

func TestConformanceMessagesPinReplaceAndRebindWithSameVersionCounterexample(t *testing.T) {
	for _, mode := range []string{"replace", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			f := newConf(t)
			gate := tk.NewBarrier()
			defer gate.Release()
			f.hook = func(c context.Context, stage string, call confCall, _ confReceipt) error {
				if stage == "handler.between" && call.Message.ID == f.o.Namespace+":old" {
					return gate.Hold(c)
				}
				return nil
			}
			f.submit(t, "http", "old")
			oldSeq := f.relay(t, "old")
			before := f.r.Forest.Snapshot()
			oldGeneration := before.Generation
			f.enable(t)
			if err := tk.Await(f.ctx, gate.Arrived()); err != nil {
				t.Fatal(err)
			}
			if err := f.bundle.switchVersion(mode); err != nil {
				t.Fatal(err)
			}
			newer := f.generation()
			for _, id := range []string{"app/proof/consumer", "app/proof/db", "app/proof/store", "app/agent", "app/checkpoint"} {
				if before.Trees[forest.TreeID(id)].InstanceID != f.r.Forest.Snapshot().Trees[forest.TreeID(id)].InstanceID {
					t.Fatal("stable resource rebuilt", id)
				}
			}
			f.submit(t, "rpc", "new")
			newSeq := f.relay(t, "new")
			fresh := f.await(t, newSeq)
			f.submit(t, "http", "same-version")
			sameSeq := f.relay(t, "same-version")
			same := f.await(t, sameSeq)
			if fresh.Receipt.Version != "v2" || same.Receipt.Version != "v2" {
				t.Fatal(fresh, same)
			}
			for _, pair := range []struct {
				seq uint64
				out confObservation
			}{{newSeq, fresh}, {sameSeq, same}} {
				if err := tk.CheckPinned(f.pinned(t, pair.seq, pair.out, newer)); err != nil {
					t.Fatal(err)
				}
			}
			bad := f.pinned(t, newSeq, fresh, newer)
			bad.Trace = same.Trace
			if err := tk.CheckPinned(bad); !errors.Is(err, tk.ErrViolation) {
				t.Fatal("same-version message swap accepted", err)
			}
			missing := f.pinned(t, newSeq, fresh, newer)
			missing.Required = append(missing.Required, "unobserved.stage")
			if err := tk.CheckPinned(missing); !errors.Is(err, tk.ErrViolation) {
				t.Fatal("missing stage accepted", err)
			}
			empty := f.pinned(t, newSeq, fresh, newer)
			empty.Trace = tk.Trace{}
			if err := tk.CheckPinned(empty); !errors.Is(err, tk.ErrViolation) {
				t.Fatal("empty trace accepted", err)
			}
			if p, err := f.bundle.handlers["v1"].file.sample(); err != nil || !p.Usable || p.CloseCount != 0 {
				t.Fatal("old resource prematurely closed", p, err)
			}
			gate.Release()
			old := f.await(t, oldSeq)
			if old.Receipt.Version != "v1" {
				t.Fatal(old)
			}
			if err := tk.CheckPinned(f.pinned(t, oldSeq, old, oldGeneration)); err != nil {
				t.Fatal(err)
			}
			f.acked(t, sameSeq)
			if err := f.r.Forest.Wait(f.ctx); err != nil {
				t.Fatal(err)
			}
			if p, err := f.bundle.handlers["v1"].file.sample(); err != nil || p.Usable || p.CloseCount != 1 {
				t.Fatal(p, err)
			}
		})
	}
}
