package statemachine

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func TestModelGeneratorAndCodecAreDeterministic(t *testing.T) {
	a, b := m.Generate(Catalog(), 81276, 128, 16), m.Generate(Catalog(), 81276, 128, 16)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different input")
	}
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var replay m.Trace
	if err = json.Unmarshal(raw, &replay); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, replay) {
		t.Fatal("trace codec lost intent or concrete inputs")
	}
	requireNative(t, replay)
	path := filepath.Join(t.TempDir(), "standalone.json")
	saveTrace(t, path, replay)
	loaded := loadTraces(t, path)
	if len(loaded) != 1 || !reflect.DeepEqual(loaded[0], a) {
		t.Fatal("CLI trace loading changed concrete input")
	}
	wrapper, err := json.Marshal(workerInput{Traces: []m.Trace{replay}})
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := decodeTraces(wrapper)
	if err != nil || !reflect.DeepEqual(wrapped, loaded) {
		t.Fatal(wrapped, err)
	}
	for _, bad := range [][]byte{[]byte(`{`), []byte(`{"Format":999}`)} {
		if _, err := decodeTraces(bad); err == nil {
			t.Fatal("invalid replay accepted", string(bad))
		}
	}
}
func TestModelReducerRejectsImpossibleSchedules(t *testing.T) {
	good := normalizeTrace(pinnedTrace(81276, 1, 1, false))
	var withoutCalls, withoutChange m.Trace
	withoutCalls = m.Clone(good)
	withoutCalls.Actions = nil
	withoutChange = m.Clone(good)
	withoutChange.Actions = nil
	for _, a := range good.Actions {
		if a.Kind != "start-call" {
			withoutCalls.Actions = append(withoutCalls.Actions, a)
		}
		if a.Kind != "apply" {
			withoutChange.Actions = append(withoutChange.Actions, a)
		}
	}
	if validateTrace(withoutCalls) == nil {
		t.Fatal("orphaned barrier admitted")
	}
	if validateTrace(withoutChange) == nil {
		t.Fatal("Wait failure could be fabricated without retired work")
	}
	invalid := prepTrace("parallel", false)
	for i := range invalid.Actions {
		if a := &invalid.Actions[i]; a.Plan != nil {
			a.Plan.Ops = a.Plan.Ops[:2]
			break
		}
	}
	if validateTrace(invalid) == nil {
		t.Fatal("unreachable constructor reference admitted")
	}
	invalid = stopTrace("startup", "hold")
	invalid.Steps[0].Plan.Ops = invalid.Steps[0].Plan.Ops[:3]
	if validateTrace(invalid) == nil {
		t.Fatal("missing lifecycle setup admitted")
	}
	invalid = m.Clone(good)
	invalid.Format++
	if validateTrace(invalid) == nil {
		t.Fatal("unknown trace version admitted")
	}
}

func TestModelUnorderedWriterCannotAssumeOneWinner(t *testing.T) {
	trace := m.Trace{Format: m.FormatVersion, Catalog: Catalog()}
	a := m.Plan{Ops: []m.Op{root("a", "runtime-many")}}
	b := m.Plan{Ops: []m.Op{root("b", "runtime-many")}}
	trace.Actions = []m.Action{{Kind: "race", Plans: []m.Plan{a, b}}, {Kind: "start-call", Actor: "new-call", Tree: "a", Port: "in"}, {Kind: "await-call", Actor: "new-call"}, {Kind: "release-call", Actor: "new-call"}, {Kind: "join-call", Actor: "new-call"}}
	// Use actual bound routes so the first simulated winner admits the caller.
	trace.Actions[0].Plans[0].Ops = append(trace.Actions[0].Plans[0].Ops, root("p", "provider-v1"), link("l", "a", "in", "p", false))
	if validateTrace(trace) == nil {
		t.Fatal("one allowed winner has no A, but preflight accepted dependent caller")
	}
	if _, err := m.Rebase(trace); err == nil {
		t.Fatal("reducer chose an unordered winner to materialize dependent commands")
	}
	prepared := m.Clone(trace)
	prepared.Actions[0].Kind = "prepared-race"
	if err := validateTrace(prepared); err != nil {
		t.Fatal("known first-winner continuation rejected", err)
	}
	requireNative(t, prepared)
}

func TestModelUnorderedWriterWaitRequiresPriorCallsJoined(t *testing.T) {
	trace := knownTrace(ruleCase{name: "branch-dependent-wait", steps: []ruleStep{yes(root("p", "provider-v1"), root("c", "runtime-required"), link("l", "c", "in", "p", false))}})
	replace := m.Plan{Base: 1, Ops: []m.Op{replacement("p", "provider-next", false)}}
	add := m.Plan{Base: 1, Ops: []m.Op{root("other", "provider-v1")}}
	trace.Actions = []m.Action{{Kind: "start-call", Actor: "old", Tree: "c", Port: "in"}, {Kind: "await-call", Actor: "old"}, {Kind: "race", Plans: []m.Plan{replace, add}}, {Kind: "wait"}, {Kind: "release-call", Actor: "old"}, {Kind: "join-call", Actor: "old"}}
	if validateTrace(trace) == nil {
		t.Fatal("Wait may block a valid Replace winner behind its own release")
	}
	if _, err := m.Rebase(trace); err == nil {
		t.Fatal("branch-dependent Wait admitted by reducer")
	}
	trace.Actions = append(trace.Actions[:3], m.Action{Kind: "release-call", Actor: "old"}, m.Action{Kind: "join-call", Actor: "old"}, m.Action{Kind: "wait"})
	if err := validateTrace(trace); err != nil {
		t.Fatal("post-join Wait rejected", err)
	}
	requireNative(t, trace)
}
