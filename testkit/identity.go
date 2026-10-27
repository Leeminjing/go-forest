package testkit

type IdentityField string

const (
	Call       IdentityField = "call"
	Step       IdentityField = "step"
	Attempt    IdentityField = "attempt"
	Process    IdentityField = "process"
	Instance   IdentityField = "instance"
	Generation IdentityField = "generation"
)

type IdentityEvidence struct {
	Label                           string
	Original, Delivery              Execution
	OriginalEffect, DeliveredEffect Effect
	Changed                         []IdentityField
}

func fullExecution(x Execution) bool {
	return x.Scope.Forest != "" && x.Scope.Call != "" && x.Task != "" && x.Step != "" && x.Attempt != "" && x.Process != "" && x.Instance != ""
}
func fullEffect(x Effect) bool {
	return x.Namespace != "" && x.Operation != "" && x.InputDigest != "" && x.Receipt != "" && x.ContentDigest != "" && x.OriginProcess != "" && x.OriginVersion != ""
}
func CheckIdentity(e IdentityEvidence) error {
	const rule = "identity"
	if !fullExecution(e.Original) || !fullExecution(e.Delivery) || !fullEffect(e.OriginalEffect) || !fullEffect(e.DeliveredEffect) {
		return violation(rule, e.Label, "", "identity", "complete actual facts", "missing fields")
	}
	if e.Original.Task != e.Delivery.Task {
		return violation(rule, e.Label, "", "task", e.Original.Task, e.Delivery.Task)
	}
	if e.OriginalEffect != e.DeliveredEffect {
		return violation(rule, e.Label, "", "effect", e.OriginalEffect, e.DeliveredEffect)
	}
	if len(e.Changed) == 0 {
		return violation(rule, e.Label, "", "changed", "explicit requirements", "empty")
	}
	used := map[IdentityField]bool{}
	for _, field := range e.Changed {
		if used[field] {
			return violation(rule, e.Label, "", "changed", "unique requirement", field)
		}
		used[field] = true
		a, b := e.Original, e.Delivery
		var same bool
		switch field {
		case Call:
			same = a.Scope.Call == b.Scope.Call
		case Step:
			same = a.Step == b.Step
		case Attempt:
			same = a.Attempt == b.Attempt
		case Process:
			same = a.Process == b.Process
		case Instance:
			same = a.Instance == b.Instance
		case Generation:
			if a.Scope.Forest != b.Scope.Forest {
				return violation(rule, e.Label, "", "generation domain", a.Scope.Forest, b.Scope.Forest)
			}
			same = a.Scope.Generation == b.Scope.Generation
		default:
			return violation(rule, e.Label, "", "changed", "known dimension", field)
		}
		if same {
			return violation(rule, e.Label, "", string(field), "different", "unchanged")
		}
	}
	return nil
}
