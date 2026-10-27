package testkit_test

import (
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"testing"
)

func TestIdentitySeparatesNewDeliveryFromOriginalEffect(t *testing.T) {
	first := tk.Execution{Scope: tk.Scope{Forest: "a", Call: "call-a", Generation: 1}, Task: "workflow/run", Step: "process", Attempt: "1", Process: "process-a", Instance: "app-a"}
	next := first
	next.Scope = tk.Scope{Forest: "b", Call: "call-b", Generation: 1}
	next.Process = "process-b"
	next.Instance = "app-b"
	next.Attempt = "2"
	effect := tk.Effect{Namespace: "sql", Operation: "operation", InputDigest: "input", Receipt: "receipt", ContentDigest: "result", OriginProcess: "process-a", OriginVersion: "v1", OriginGeneration: 1}
	good := tk.IdentityEvidence{Label: "redelivery", Original: first, Delivery: next, OriginalEffect: effect, DeliveredEffect: effect, Changed: []tk.IdentityField{tk.Process, tk.Instance, tk.Attempt}}
	if e := tk.CheckIdentity(good); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"origin", "input", "missing", "unchanged", "cross-generation", "unknown", "no-requirements"} {
		t.Run(kind, func(t *testing.T) {
			bad := good
			switch kind {
			case "origin":
				bad.DeliveredEffect.OriginProcess = next.Process
			case "input":
				bad.DeliveredEffect.InputDigest = "different"
			case "missing":
				bad.Original.Instance = ""
			case "unchanged":
				bad.Delivery.Process = first.Process
			case "cross-generation":
				bad.Changed = []tk.IdentityField{tk.Generation}
			case "unknown":
				bad.Changed = []tk.IdentityField{"unknown"}
			case "no-requirements":
				bad.Changed = nil
			}
			if e := tk.CheckIdentity(bad); !errors.Is(e, tk.ErrViolation) {
				t.Fatal(kind, e)
			}
		})
	}
}
