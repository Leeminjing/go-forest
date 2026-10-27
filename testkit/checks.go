package testkit

import "fmt"

type PinnedEvidence struct {
	Label    string
	Scope    Scope
	Required []string
	Trace    Trace
}

func violation(rule, label, stage, field string, want, got any) error {
	return &Failure{Rule: rule, Label: label, Stage: stage, Field: field, Expected: fmt.Sprint(want), Actual: fmt.Sprint(got), Err: ErrViolation}
}
func complete(rule, label string, t Trace) error {
	if t.err != nil {
		return &Failure{Rule: rule, Label: label, Field: "trace", Err: t.err}
	}
	if len(t.entries) == 0 {
		return violation(rule, label, "", "trace", "nonempty", 0)
	}
	return nil
}
func CheckPinned(e PinnedEvidence) error {
	const rule = "pinned"
	if err := complete(rule, e.Label, e.Trace); err != nil {
		return err
	}
	if e.Scope.Forest == "" || e.Scope.Call == "" || len(e.Required) == 0 {
		return violation(rule, e.Label, "", "scope/required", "explicit nonempty values", e.Scope)
	}
	required := map[string]bool{}
	for _, s := range e.Required {
		if s == "" {
			return violation(rule, e.Label, "", "required", "nonempty stage", s)
		}
		if _, ok := required[s]; ok {
			return violation(rule, e.Label, s, "required", "unique", s)
		}
		required[s] = false
	}
	for _, p := range e.Trace.entries {
		if p.Execution == nil || p.Execution.Scope != e.Scope {
			return violation(rule, e.Label, p.Stage, "scope", e.Scope, p.Execution)
		}
		if _, ok := required[p.Stage]; ok {
			required[p.Stage] = true
		}
	}
	for _, s := range e.Required {
		if !required[s] {
			return violation(rule, e.Label, s, "stage", "observed", "missing")
		}
	}
	return nil
}
