package testkit_test

import (
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"testing"
)

func admission(t *testing.T) tk.AdmissionEvidence {
	return tk.AdmissionEvidence{Label: "candidate", Before: tk.Topology{Forest: "f", Generation: 3, Fingerprint: "original-route"}, After: tk.Topology{Forest: "f", Generation: 3, Fingerprint: "original-route"}, Trace: trace(t,
		tk.Event{Stage: "candidate.begin"},
		tk.Event{Stage: "candidate.prepared", Resources: []tk.Resource{{ID: "file", Ownership: tk.Owned, Usable: true}}},
		tk.Event{Stage: "original.served"},
		tk.Event{Stage: "candidate.end", Resources: []tk.Resource{{ID: "file", Ownership: tk.Owned, CloseCount: 1}}},
		tk.Event{Stage: "commit"}, tk.Event{Stage: "enable"}, tk.Event{Stage: "business.enter"})}
}
func TestAdmissionRequiresFullRollbackAndRealPositiveControl(t *testing.T) {
	good := admission(t)
	if e := tk.CheckAdmission(good); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"route", "generation", "steal", "no-business", "unreleased", "no-old-call"} {
		t.Run(kind, func(t *testing.T) {
			bad := good
			switch kind {
			case "route":
				bad.After.Fingerprint = "changed"
			case "generation":
				bad.After.Generation++
			default:
				events := []tk.Event{}
				for _, e := range good.Trace.Entries() {
					if kind == "no-business" && e.Stage == "business.enter" {
						continue
					}
					if kind == "no-old-call" && e.Stage == "original.served" {
						continue
					}
					if kind == "unreleased" && e.Stage == "candidate.end" {
						e.Resources[0].Usable = true
					}
					events = append(events, e.Event)
					if kind == "steal" && e.Stage == "candidate.prepared" {
						events = append(events, tk.Event{Stage: "business.enter"})
					}
				}
				bad.Trace = trace(t, events...)
			}
			if e := tk.CheckAdmission(bad); !errors.Is(e, tk.ErrViolation) {
				t.Fatal(kind, e)
			}
		})
	}
}
