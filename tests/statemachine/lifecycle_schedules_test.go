package statemachine

import (
	"fmt"
	"testing"

	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func prepTrace(stage string, canceled bool) m.Trace {
	trace := knownTrace(ruleCase{name: "preparation-" + stage, steps: []ruleStep{yes(root("old", "provider-v1"))}})
	def := "fail-" + stage
	if canceled {
		def = "provider-v1"
	}
	p := m.Plan{Base: 1, Ops: []m.Op{root("candidate", def)}}
	if stage == "parallel" {
		p.Ops = []m.Op{root("provider", "provider-v1"), root("candidate", "parallel-build"), link("l", "candidate", "in", "provider", false)}
	}
	trace.Actions = []m.Action{{Kind: "start-apply", Actor: "candidate-change", Tree: "candidate", Target: stage, Plan: &p, Base: m.BaseIntent{Kind: "current"}}, {Kind: "await-prepare", Actor: "candidate-change"}}
	if stage == "parallel" {
		trace.Actions = append(trace.Actions, m.Action{Kind: "await-prepare-cancel", Actor: "candidate-change"})
	}
	trace.Actions = append(trace.Actions, m.Action{Kind: "probe-prepared", Actor: "candidate-change"}, m.Action{Kind: "check"})
	if canceled {
		trace.Actions = append(trace.Actions, m.Action{Kind: "cancel-apply", Actor: "candidate-change"})
	}
	trace.Actions = append(trace.Actions, m.Action{Kind: "release-prepare", Actor: "candidate-change"}, m.Action{Kind: "join-apply", Actor: "candidate-change"}, m.Action{Kind: "check"})
	for i := range trace.Actions {
		trace.Actions[i].ID = fmt.Sprintf("preparation-%d", i)
	}
	return trace
}
func stopTrace(phase, mode string) m.Trace {
	trace := knownTrace(ruleCase{name: "stop-" + phase + "-" + mode, steps: []ruleStep{yes(root("owner", "provider-v1"), nested("owner", "provider", "provider-v1"), root("consumer", phase+"-required"), link("resource", "consumer", "in", "provider", false))}})
	if mode == "timeout" {
		trace.DrainMillis = 30
	}
	p := m.Plan{Base: 1, Ops: []m.Op{{Kind: "remove", Tree: "consumer"}, {Kind: "remove-subtree", Tree: "owner"}}}
	trace.Actions = []m.Action{{Kind: "arm-stop", Actor: "stop", Tree: "consumer", Target: mode}, {Kind: "guard-stop", Actor: "stop", Tree: "provider"}, {Kind: "guard-stop", Actor: "stop", Tree: "owner"}, {Kind: "apply", Base: m.BaseIntent{Kind: "current"}, Plan: &p}, {Kind: "await-stop", Actor: "stop"}}
	for _, id := range []string{"consumer", "provider", "owner"} {
		trace.Actions = append(trace.Actions, m.Action{Kind: "probe-file", Tree: id, Target: "open"})
	}
	if mode == "error" {
		trace.Actions = append(trace.Actions, m.Action{Kind: "wait-stop-error", Actor: "stop"})
		return trace
	}
	if mode == "timeout" {
		trace.Actions = append(trace.Actions, m.Action{Kind: "await-stop-timeout", Actor: "stop"}, m.Action{Kind: "wait-stop-timeout", Actor: "stop"}, m.Action{Kind: "probe-file", Tree: "provider", Target: "open"}, m.Action{Kind: "probe-file", Tree: "owner", Target: "open"})
	}
	trace.Actions = append(trace.Actions, m.Action{Kind: "release-stop", Actor: "stop"})
	for _, id := range []string{"consumer", "provider", "owner"} {
		trace.Actions = append(trace.Actions, m.Action{Kind: "await-file-closed", Tree: id})
	}
	trace.Actions = append(trace.Actions, m.Action{Kind: "wait"}, m.Action{Kind: "check"})
	for i := range trace.Actions {
		trace.Actions[i].ID = fmt.Sprintf("stop-%d", i)
	}
	return trace
}
func reverseTrace() m.Trace {
	trace := knownTrace(ruleCase{name: "historical-ownership-reversal", steps: []ruleStep{yes(root("a", "provider-v1"), nested("a", "b", "provider-v1"), root("c", "runtime-required"), link("l", "c", "in", "b", false))}})
	move := m.Plan{Base: 1, Ops: []m.Op{{Kind: "unnest", Tree: "b"}, {Kind: "move", Tree: "a", Parent: "b"}}}
	remove := m.Plan{Base: 2, Ops: []m.Op{{Kind: "remove", Tree: "c"}, {Kind: "remove-subtree", Tree: "b"}}}
	trace.Actions = []m.Action{{Kind: "start-call", Actor: "old", Tree: "c", Port: "in"}, {Kind: "await-call", Actor: "old"}, {Kind: "apply", Base: m.BaseIntent{Kind: "current"}, Plan: &move}, {Kind: "start-call", Actor: "newer", Tree: "c", Port: "in"}, {Kind: "await-call", Actor: "newer"}, {Kind: "apply", Base: m.BaseIntent{Kind: "current"}, Plan: &remove}, {Kind: "probe-held", Actor: "old"}, {Kind: "probe-held", Actor: "newer"}, {Kind: "release-call", Actor: "old"}, {Kind: "join-call", Actor: "old"}, {Kind: "release-call", Actor: "newer"}, {Kind: "join-call", Actor: "newer"}, {Kind: "await-file-closed", Tree: "a"}, {Kind: "await-file-closed", Tree: "b"}, {Kind: "await-file-closed", Tree: "c"}, {Kind: "wait"}}
	return trace
}
func TestModelPreparationAndPhysicalLifecycle(t *testing.T) {
	var traces []m.Trace
	for _, stage := range []string{"build", "start", "readiness", "parallel"} {
		traces = append(traces, prepTrace(stage, false))
	}
	traces = append(traces, prepTrace("readiness", true), reverseTrace())
	for _, phase := range []string{"construction", "startup"} {
		for _, mode := range []string{"hold", "timeout", "error"} {
			traces = append(traces, stopTrace(phase, mode))
		}
	}
	for _, trace := range traces {
		t.Run(trace.Name, func(t *testing.T) {
			if err := validateTrace(trace); err != nil {
				t.Fatal(err)
			}
			requireNative(t, trace)
		})
	}
}
