package ecosystem

import (
	"errors"
	"testing"

	tk "github.com/Leeminjing/go-forest/testkit"
)

func TestConformanceEntryAndMessageAreIndependentScopesAcrossRouteChange(t *testing.T) {
	for _, transport := range []string{"http", "rpc"} {
		t.Run(transport, func(t *testing.T) {
			f := newConf(t)
			f.parentTrace = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
			before := f.generation()
			task := f.submit(t, transport, "boundary")
			if task.Version != "v1" {
				t.Fatal(task)
			}
			f.entriesMu.Lock()
			entry := f.entries["boundary"][0]
			entryTrace := f.entryTraces["boundary"][0]
			f.entriesMu.Unlock()
			if err := f.bundle.switchVersion("replace"); err != nil {
				t.Fatal(err)
			}
			seq := f.relay(t, "boundary")
			f.enable(t)
			out := f.await(t, seq)
			if entry.Scope.Generation != before || out.Execution.Scope.Generation == before || entry.Scope.Call == out.Execution.Scope.Call || out.Receipt.Version != "v2" {
				t.Fatal(entry, out)
			}
			if entryTrace == "" || entryTrace != out.TraceID {
				t.Fatal("trace correlation lost", entryTrace, out.TraceID)
			}
			if err := tk.CheckPinned(f.pinned(t, seq, out, f.generation())); err != nil {
				t.Fatal(err)
			}
			j, _ := tk.NewJournal(128)
			_, err := j.Record(tk.Event{Stage: "entry", Execution: &entry})
			if err != nil {
				t.Fatal(err)
			}
			for _, point := range out.Trace.Entries() {
				if _, err = j.Record(point.Event); err != nil {
					t.Fatal(err)
				}
			}
			mixed, err := j.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err = tk.CheckPinned(tk.PinnedEvidence{Label: "incorrect cross-message scope", Scope: entry.Scope, Required: []string{"entry", "handler.enter"}, Trace: mixed}); !errors.Is(err, tk.ErrViolation) {
				t.Fatal("cross-message scope accepted", err)
			}
			f.acked(t, seq)
		})
	}
}
