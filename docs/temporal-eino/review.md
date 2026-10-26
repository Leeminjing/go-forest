# Temporal × Eino independent review

Baseline `ecf7492`; initial complete implementation reviewed at `75cb090`.
Two independent read-only reviewers checked the approved local Spec/Design and
repository adapter conventions. Fixes are in `6d0f5c7`.

## Spec

Three P2 findings were fixed:

1. Query could report completed before required final Graph verification, or
   retain a stale phase on failure. Workflow now uses verifying, then completed/
   stopped only after actual verification; errors expose failed/canceled phases.
2. Signals received while Resume was running could remain unclassified. A native
   deterministic Workflow coroutine now processes dedup/conflict/stale decisions
   during outstanding Activities, queues early decisions for their pause and
   rejects new decisions after execution closes.
3. Persisted execution evidence omitted actual Eino physical identity. Results,
   native boundary traces and Graph history/query values now include qualified
   process/instance/generation identities. Cached delivery retains original source.

Regressions include a barrier-held Resume with duplicate/conflicting Signals,
blocked/failed final Graph verification, actual snapshot identity assertions and
process restoration/redelivery identity checks. Re-review confirmed all three
closed, no scope creep and **zero outstanding Spec findings**.

## Standards

Two P2 findings were fixed:

1. The same premature Workflow completion violated the adapter guide's explicit
   runtime-diagnostic boundary. The verifying/error phases resolve it.
2. An observation Hook received the mutable result after durable Seal; changing
   text or pause identity could alter delivery while retry returned original data.
   Meter now supplies a deep detached DTO copy, including nested slices/results.

TestObservationCannotMutateSealedPauseOrDeliveredNativeResult reproduced the
second issue before the fix and now passes. A nonblocking possible Duplicated
Code suggestion was also handled by centralizing the managed heartbeat helper.
Re-review confirmed **zero outstanding Standards findings**, with no new
substantive convention violations or smells.

Fixed published dependencies, native engine semantics, consumer-defined contracts
and unchanged Core/adapters were independently checked. Private Spec/Design and
scratch files remain ignored. Publication/platform/race/external-consumer runs
are separate execution gates, not inferred successful from code review.

Summary: Spec 3 findings closed; Standards 2 findings closed; zero outstanding
on either axis. No universal adapter or public TestKit was introduced.
