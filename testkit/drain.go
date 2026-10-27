package testkit

type DrainEvidence struct {
	Label      string
	Execution  Execution
	Trace      Trace
	ProbeError error
}

// CheckDrain checks explicit actual-exit evidence, not whether a timeout or SDK
// Stop returned. The fixture must join native work before recording joined.
func CheckDrain(e DrainEvidence) error {
	const rule = "drain"
	if err := probeError(rule, e.Label, e.ProbeError); err != nil {
		return err
	}
	if !fullExecution(e.Execution) {
		return violation(rule, e.Label, "", "execution", "complete expected work identity", e.Execution)
	}
	s, err := stages(rule, e.Label, e.Trace, "entered", "notified", "held", "released", "exited", "joined")
	if err != nil {
		return err
	}
	for _, p := range e.Trace.entries {
		if p.Execution == nil || *p.Execution != e.Execution {
			return violation(rule, e.Label, p.Stage, "execution", e.Execution, p.Execution)
		}
	}
	if err = resources(rule, e.Label, s["entered"].Resources, s["held"].Resources, false); err != nil {
		return err
	}
	return resources(rule, e.Label, s["entered"].Resources, s["joined"].Resources, true)
}
