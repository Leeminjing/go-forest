package testkit

// Topology is a fixture's canonical routing projection, excluding live state,
// timestamps and event sequence. Fingerprint must cover the claimed routes.
type Topology struct {
	Forest      string
	Generation  uint64
	Fingerprint string
}
type AdmissionEvidence struct {
	Label         string
	Before, After Topology
	Trace         Trace
	ProbeError    error
}

func stages(rule, label string, t Trace, required ...string) (map[string]Entry, error) {
	if err := complete(rule, label, t); err != nil {
		return nil, err
	}
	result := map[string]Entry{}
	var previous uint64
	for _, stage := range required {
		for _, p := range t.entries {
			if p.Stage == stage {
				if _, ok := result[stage]; ok {
					return nil, violation(rule, label, stage, "marker", "unique", "repeated")
				}
				result[stage] = p
			}
		}
		p, ok := result[stage]
		if !ok || p.Sequence <= previous {
			return nil, violation(rule, label, stage, "marker", "present in required order", p.Sequence)
		}
		previous = p.Sequence
	}
	return result, nil
}
func probeError(rule, label string, err error) error {
	if err == nil {
		return nil
	}
	return &Failure{Rule: rule, Label: label, Field: "probe", Err: err}
}
func resources(rule, label string, before, after []Resource, released bool) error {
	if len(before) == 0 || len(before) != len(after) {
		return violation(rule, label, "", "resources", "same nonempty set", len(after))
	}
	prior := map[string]Resource{}
	for _, r := range before {
		if r.ID == "" || (r.Ownership != Owned && r.Ownership != Borrowed) {
			return violation(rule, label, "", "resource", "explicit ID/ownership", r)
		}
		if _, ok := prior[r.ID]; ok {
			return violation(rule, label, "", "resource", "unique ID", r.ID)
		}
		if !r.Usable {
			return violation(rule, label, "", "resource", "initially usable", r)
		}
		prior[r.ID] = r
	}
	for _, r := range after {
		b, ok := prior[r.ID]
		if !ok {
			return violation(rule, label, "", "resource", "matching unique ID", r.ID)
		}
		delete(prior, r.ID)
		want := b
		if released && b.Ownership == Owned {
			if b.CloseCount == ^uint64(0) {
				return violation(rule, label, "", "close count", "no rollover", b.CloseCount)
			}
			want.Usable = false
			want.CloseCount++
		}
		if r != want {
			return violation(rule, label, "", "resource", want, r)
		}
	}
	return nil
}
func CheckAdmission(e AdmissionEvidence) error {
	const rule = "admission"
	if err := probeError(rule, e.Label, e.ProbeError); err != nil {
		return err
	}
	if e.Before.Forest == "" || e.Before.Fingerprint == "" || e.Before != e.After {
		return violation(rule, e.Label, "", "topology", e.Before, e.After)
	}
	s, err := stages(rule, e.Label, e.Trace, "candidate.begin", "candidate.prepared", "original.served", "candidate.end", "commit", "enable")
	if err != nil {
		return err
	}
	if err = resources(rule, e.Label, s["candidate.prepared"].Resources, s["candidate.end"].Resources, true); err != nil {
		return err
	}
	count := 0
	for _, p := range e.Trace.entries {
		if p.Stage == "business.enter" {
			count++
			if p.Sequence <= s["enable"].Sequence {
				return violation(rule, e.Label, p.Stage, "admission", "after enable", p.Sequence)
			}
		}
	}
	if count == 0 {
		return violation(rule, e.Label, "business.enter", "positive control", "actual execution", "missing")
	}
	return nil
}
