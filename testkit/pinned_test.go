package testkit_test

import (
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"testing"
)

func trace(t *testing.T, events ...tk.Event) tk.Trace {
	t.Helper()
	j, e := tk.NewJournal(32)
	if e != nil {
		t.Fatal(e)
	}
	for _, event := range events {
		if _, e = j.Record(event); e != nil {
			t.Fatal(e)
		}
	}
	out, e := j.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestPinnedRejectsMixedMissingAndForeignInvocationEvidence(t *testing.T) {
	scope := tk.Scope{Forest: "forest-a", Call: "attempt-1", Generation: 0}
	point := func(stage string, s tk.Scope) tk.Event {
		return tk.Event{Stage: stage, Execution: &tk.Execution{Scope: s}}
	}
	good := tk.PinnedEvidence{Label: "old", Scope: scope, Required: []string{"first", "second"}, Trace: trace(t, point("first", scope), point("second", scope))}
	if e := tk.CheckPinned(good); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"mixed", "foreign", "missing", "empty", "call", "requirements"} {
		t.Run(kind, func(t *testing.T) {
			bad := good
			other := scope
			switch kind {
			case "mixed":
				other.Generation = 1
			case "foreign":
				other.Forest = "forest-b"
			case "call":
				other.Call = "attempt-2"
			case "missing":
				bad.Required = []string{"missing"}
			case "empty":
				bad.Trace = tk.Trace{}
			case "requirements":
				bad.Required = nil
			}
			if kind == "mixed" || kind == "foreign" || kind == "call" {
				bad.Trace = trace(t, point("first", scope), point("second", other))
			}
			if e := tk.CheckPinned(bad); !errors.Is(e, tk.ErrViolation) {
				t.Fatal("incorrect proof accepted", e)
			}
		})
	}
}
