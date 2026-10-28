package topology

// A prepared record borrows immutable existing values and owns only the
// additions for one record. It is private to a Store's exclusive operation.
// No map write or append to an existing backing array occurs until publication.
type preparedRecord struct {
	next     journalState
	revision *Revision
	requests []requestWrite
	facts    []factWrite
	session  string
	entry    Entry
}
type requestWrite struct {
	key   string
	value requestResult
}
type factWrite struct {
	key   string
	value DefinitionFacts
}

// publish has no I/O, semantic checks, or recoverable errors. The append path
// calls it after successful Sync; scan calls it after full record validation.
// Native map/slice growth is amortized storage work, not full-history cloning.
func (p preparedRecord) publish(s *journalState) {
	*s = p.next
	if p.revision != nil {
		s.history = append(s.history, *p.revision)
	}
	for _, write := range p.requests {
		s.requests[write.key] = write.value
	}
	for _, write := range p.facts {
		s.facts[write.key] = write.value
	}
	if p.session != "" {
		s.sessions[p.session] = true
	}
	s.entries = append(s.entries, p.entry)
}
