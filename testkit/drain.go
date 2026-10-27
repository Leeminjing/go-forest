package testkit

type DrainEvidence struct {
	Label      string
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
	s, err := stages(rule, e.Label, e.Trace, "entered", "notified", "held", "released", "exited", "joined")
	if err != nil {
		return err
	}
	if err = resources(rule, e.Label, s["entered"].Resources, s["held"].Resources, false); err != nil {
		return err
	}
	return resources(rule, e.Label, s["entered"].Resources, s["joined"].Resources, true)
}
