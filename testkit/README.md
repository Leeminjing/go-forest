# Adapter Conformance TestKit

`github.com/Leeminjing/go-forest/testkit` is an independent Go 1.23 module using
only the standard library. It checks evidence supplied by real adapter fixtures;
it does not run, wrap or certify an execution engine. There is no Core, Temporal,
Eino, Fx or database dependency.

```sh
go get github.com/Leeminjing/go-forest/testkit@v0.1.0
```

## Coordinate actual work

Put `barrier.Hold(callContext)` at a real fixture hook. In the controlling test,
`Await(ctx, barrier.Arrived())` proves that hook entered before replacing a route
or requesting cancellation. `Release` is idempotent. Always release in cleanup
**before** joining native work or closing its resources.

```go
barrier := testkit.NewBarrier()
defer barrier.Release()
// Inside the actual callback: return barrier.Hold(callContext)
// In the controller:
if err := testkit.Await(ctx, barrier.Arrived()); err != nil { return err }
// Publish the real route change and verify an independent call here.
barrier.Release()
```

A Barrier has one designated entrant. Duplicate Hold and Release before Hold
are errors; early Release never invents arrival. Await/Hold honor cancellation
already visible on entry. Later simultaneous cancellation and release have no
artificial ordering. For a deliberately noncooperative drain case, the fixture
may Hold with `context.WithoutCancel` after observing actual cancellation; pass
that context only to the barrier, not downstream business calls. There is no
forced goroutine stop or background timeout wrapper.

## Record detached evidence

Create a `Journal` with capacity 1–65536. `Record(Event)` returns a local sequence;
`Snapshot()` returns a detached `Trace`. `Trace.Entries()` returns another copy.
Execution/Effect pointers and resource slices are copied on both boundaries.
Events contain values, never native handles or callback closures. Stage is
required, identity strings are limited to 4096 bytes, resources to 128 per event.
Invalid writes and overflow poison the Journal: snapshots/checks keep returning
the cause, rather than accepting evidence after silently dropping entries.

Sequence orders observation in this recorder, not distributed execution.
Barriers and actual joins establish the causal relationships. Call is a
fixture label attached to a real invocation; it is not Forest's private Scope ID.
Read Generation from the original call context/trace, not the current snapshot.
Generation zero is valid, and is comparable only within one Forest domain.

## Four checks

| Check | Required evidence |
| --- | --- |
| `CheckPinned` | Explicit expected Scope, nonempty unique Required stages, nonempty complete Trace. Every point must have the same Forest/Call/Generation. Different Trees may have different instances. |
| `CheckAdmission` | Equal before/after Topology routing projections; original capability remains callable; candidate resources prepared then cleaned; zero actual business admission before explicit Enable; successful real positive control. |
| `CheckDrain` | Actual entry, notification, continued blocked work, release, native exit and join, with resource samples before/while held/after join. Owned resources close once; borrowed resources stay usable with unchanged close count. |
| `CheckIdentity` | Complete original/delivery execution and effect identities; same logical Task; explicit execution dimensions that must change; the original operation, input, receipt, content and provenance remain identical. |

Admission uses these unique ordered markers:
`candidate.begin`, `candidate.prepared` (resource sample), `original.served`,
`candidate.end` (sample after actual cleanup), `commit`, `enable`.
Actual callbacks record `business.enter`; all such events must be after `enable`
and at least one must exist. `enable` marks the invocation of native Enable;
the fixture must separately check that Enable succeeds. The rollback interval
must cover the complete failed candidate and cleanup, with work already queued
before preparation. One instantaneous zero counter does not prove sustained non-admission.
Post-commit Enable failure remains a separate native test.

Drain uses unique ordered markers: `entered` (resource sample), `notified`,
`held` (sample while work is still blocked), `released`, `exited`, `joined`
(sample after actual native join/resource retirement). DrainEvidence.Execution
must identify the expected real work; every event must retain that same complete
identity. Exit evidence from another work item sharing these resources fails.
Read callback identities from the native context again on exit, rather than
filling every event with an expected label. An SDK Stop return, reader
Close, timeout or status enum is not an exit witness. `ProbeError` preserves a
sampling failure; do not turn an unknown probe error into Usable=false.

Fixtures sample original resources with actual Check/Stat/Ping and successful
native close counts. Opening a second handle is not a probe of the original.
The tool never closes a resource or interprets a framework error. Assert native
Inflight/Stop/Join, output and ownership facts alongside the common check.

Topology.Fingerprint is a fixture's canonical routing projection: stable sorted
nodes/instances/definitions and binding endpoints/order. Exclude live State,
Error, Timestamp and EventSequence; failed attempts may generate events without
changing topology. Retain direct original snapshot/output assertions.

IdentityField supports Call, Step, Attempt, Process, Instance and Generation.
Generation changes require the same Forest domain. Use different fields for
cross-process recovery. Record actual native attempt data, not a guessed retry
count from possibly compacted history. Effect provenance describes the original
effect, independently of its current deliverer or route. A cached result may
legitimately retain the old engine identity while a new worker delivers it.

All checks return errors. `errors.Is` recognizes ErrViolation, ErrInvalidEvidence,
ErrEvidenceOverflow and barrier misuse. `errors.As` to `*Failure` exposes Rule,
Label, Stage, Field, Expected/Actual and the wrapped cause. Inspect errors in
the controlling test goroutine; do not call Fatal from native work.

## Native fixtures and limits

The two real users are [Fx × Temporal × GORM](../examples/cross-adapter) and
[Temporal × Eino](../examples/temporal-eino). Both use the same checks for pinned
versions, failed candidates/admission, physical drain and crash-recovery identity.
Their original SQL receipts, checkpoints, native retries, Signal/Resume,
OutcomeUnknown, Kill+Wait and replay assertions remain intact.

This module checks the supplied proof, including missing stages and known bad
counterexamples. It cannot establish facts omitted or fabricated by a fixture.
It supplies no universal Run/Resume interface, resource owner, native checkpoint
decoder, process supervisor, distributed generation or exactly-once promise.
Keep uncertain side effects and recovery decisions in the native fixture.

Run tool checks without services with `go vet ./...` and `go test -race ./... -count=3`. Real service/race/published acceptance remains in the two existing
scripts. See [validation](../docs/testkit/validation.md) for the separate gates.
