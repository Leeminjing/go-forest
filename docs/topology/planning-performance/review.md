# Independent review

Fixed baseline: `a9f75600afaeba5aee0b407bcd08bcb85eb2fdeb`.
Measured production candidate: `f8078b9156bbd1d962d433f0af17d05a0c067fe6`.
Two independent read-only reviews examined specification conformance and
implementation quality, followed by review of the measurement corrections.

## Standards

No confirmed production defect or constraint violation remained. Canonical
state and indexes have operation-local ownership; public objects remain
detached and no global mutable route/Manifest cache was introduced.

Review identified two measurement issues: allocation profiles could omit the
tail before GC profile refresh, and summary pairing did not check the actual
binary's compilation settings. Supplementary old/new profiles now flush two
GC cycles outside ordinary timing. Pairing now checks saved non-VCS build
settings and fixed dependency checksums; GOAMD64 and CGO_ENABLED changes are
covered by negative regressions. Original raw profiles and build information
remain available. The reviewer independently reran the four lightweight
provenance tests successfully and confirmed both issues closed.

## Spec

No confirmed production semantic defect or scope expansion remained. Complete
Proposal checking, duplicate ordering, Capture partial errors, old wire rules
and independent writable projection maps were preserved.

Review identified an evidence gap: timing/attribution programs discarded some
public results. A separate four-build audit now verifies all returned results,
preserving sequence and retained/new instance relationships. A follow-up
required hashing the actual frozen files and using the exact final attribution
sources rather than an earlier instrumented copy; both corrections were
implemented, independently checked and tested with valid-journal/source
substitution negatives. Nine cases agree across four builds. The reviewer
also verified all then-published artifact digests and 1,300 timing rows.

Final finding count: Standards 0 open; Spec 0 open. This is evidence within the
tested scope, not a proof of every possible workload or concurrent execution.

Both reviewers also examined the final CI's native Eino test synchronization
correction. They confirmed that waiting for observed Joining closes a real
producer/visitor scheduling gap, checks the call remains blocked, and preserves
all original post-switch, resource, generation and error assertions without a
runtime change or longer timeout. No additional finding remained.
