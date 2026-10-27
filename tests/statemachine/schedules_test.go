package statemachine

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func pinnedTrace(seed int64, count, generations int, closeHeld bool) m.Trace {
	trace := knownTrace(ruleCase{name: "pinned", steps: []ruleStep{yes(root("p", "provider-v1"), root("c", "runtime-required"), link("l", "c", "in", "p", false))}})
	trace.Seed = seed
	var all []string
	for g := 0; g < generations; g++ {
		for n := 0; n < count; n++ {
			id := fmt.Sprintf("g%d-call%d", g, n)
			all = append(all, id)
			trace.Actions = append(trace.Actions, m.Action{Kind: "start-call", Actor: id, Tree: "c", Port: "in"})
		}
		for n := 0; n < count; n++ {
			trace.Actions = append(trace.Actions, m.Action{Kind: "await-call", Actor: fmt.Sprintf("g%d-call%d", g, n)})
		}
		if g == 0 {
			trace.Actions = append(trace.Actions, m.Action{Kind: "wait-current"})
		}
		def := "provider-next"
		if g%2 == 1 {
			def = "provider-v1"
		}
		p := m.Plan{Base: uint64(g + 1), Ops: []m.Op{replacement("p", def, false)}}
		trace.Actions = append(trace.Actions, m.Action{Kind: "apply", Base: m.BaseIntent{Kind: "current"}, Plan: &p}, m.Action{Kind: "probe-held", Actor: fmt.Sprintf("g%d-call0", g)}, m.Action{Kind: "check"})
	}
	trace.Actions = append(trace.Actions, m.Action{Kind: "wait-pending"}, m.Action{Kind: "cancel", Actor: "g0-call0"}, m.Action{Kind: "await-cancel", Actor: "g0-call0"}, m.Action{Kind: "probe-held", Actor: "g0-call0"})
	if closeHeld {
		p := m.Plan{Base: uint64(generations + 1), Ops: []m.Op{{Kind: "remove", Tree: "c"}, {Kind: "remove", Tree: "p"}}}
		trace.Actions = append(trace.Actions, m.Action{Kind: "apply", Base: m.BaseIntent{Kind: "current"}, Plan: &p}, m.Action{Kind: "close-held"}, m.Action{Kind: "probe-held", Actor: "g0-call0"})
	}
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	for _, id := range all {
		trace.Actions = append(trace.Actions, m.Action{Kind: "release-call", Actor: id}, m.Action{Kind: "join-call", Actor: id})
	}
	trace.Actions = append(trace.Actions, m.Action{Kind: "wait"})
	if !closeHeld {
		trace.Actions = append(trace.Actions, m.Action{Kind: "check"})
	}
	for i := range trace.Actions {
		trace.Actions[i].ID = fmt.Sprintf("action-%04d", i)
	}
	return trace
}
func TestModelPinnedSchedules(t *testing.T) {
	counts := []int{1, 8, 32}
	generations := 4
	if os.Getenv("FOREST_MODEL_PROFILE") == "extended" {
		counts = []int{64, 128}
		generations = 8
	}
	for i, count := range counts {
		trace := pinnedTrace(81276+int64(i), count, generations, false)
		requireNative(t, trace)
	}
	requireNative(t, pinnedTrace(91, 1, 1, true))
}
func TestModelWriterSchedules(t *testing.T) {
	var traces []m.Trace
	for _, kind := range []string{"race", "prepared-race"} {
		for seed := int64(1); seed <= 4; seed++ {
			trace := m.Trace{Name: kind, Format: m.FormatVersion, Generator: m.GeneratorVersion, Seed: seed, Catalog: Catalog()}
			a, b := m.Plan{Ops: []m.Op{root("a", "provider-v1")}}, m.Plan{Ops: []m.Op{root("b", "provider-v1")}}
			trace.Actions = []m.Action{{ID: "writers", Kind: kind, Plans: []m.Plan{a, b}}, {ID: "final", Kind: "check"}}
			traces = append(traces, trace)
		}
	}
	requireNative(t, traces...)
}
