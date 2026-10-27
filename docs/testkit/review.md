# TestKit independent review

Baseline `5bb2b15`; initial complete implementation `5e6415f`.
Independent read-only Spec and Standards reviewers used the approved local
Spec/Design, adapter guide and existing native integration/publication patterns.
Private documents remain ignored. Execution gates are separate from review.

## Spec

Two P2 findings were reproduced and fixed in `26d6fc1`:

1. Drain checked ordered markers and shared resources without verifying work
   identity. A public counterexample spliced another task's exit into the held
   task's proof and incorrectly passed. DrainEvidence now requires a complete
   expected Execution; every event must retain it. Tests reject mixed/missing
   work. Real fixtures read native context identity on entry and again on exit.
2. Pinned fixture projections assigned an expected label to all returned points,
   allowing separate tasks on one engine/worker to collide. Actual callbacks now
   collect WorkflowID/RunID/ActivityID/Attempt and Forest.Generation. The expected
   task is independently taken from Start. Both fixtures run two real tasks at
   generation 2, accept their own traces and reject swapped complete traces.
   Original all-stage generation/output/stable-instance assertions remain.

The reviewer independently reran the public Drain/Pinned tests and re-reviewed
the native collection. Both findings are closed; **zero open Spec findings**.

## Standards

One P2 finding concerned failure cleanup. Initial admission fixtures reused the
expired test context for graph Close and used unbounded remote termination.
`26d6fc1` installs independent bounded cleanup, reports errors, accepts typed
NotFound for already finished owned workflows and releases before candidate Join.
The candidate completion channel no longer round-trips an error for cleanup.

Focused re-review found one remaining ownership subcase: a committed Runtime
can be returned with an inspection error. All four new fixtures now register
conditional bounded Close whenever Runtime is non-nil, before checking New's
error (`d801755`). The reviewer confirmed this subcase and the original finding
closed; **zero open Standards findings**, no additional smell advisories.

Summary: Spec 2 closed/0 open; Standards 1 closed/0 open. Public TestKit imports
only standard packages. Native fixtures keep their framework operations and
original real-service/process recovery assertions. No universal engine wrapper,
production runtime change, Core or stable Adapter patch was introduced.
