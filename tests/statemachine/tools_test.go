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
