# Four reusable checks, two native fixtures

| Common rule | Fx × Temporal × GORM | Temporal × Eino | Semantics kept native |
| --- | --- | --- | --- |
| Pinned | Existing Activity → Fx → business → SQL trace; shared Barrier and CheckPinned | Existing ADK tool sequence and new invocation; same Barrier and CheckPinned | Actual output and stable native instance assertions remain |
| Admission | Queued real Workflow, held Prepared, old Ping, physical candidate cleanup, then disabled Worker + explicit Enable positive control | Same complete candidate interval, original owned handle Check and native model entry control | Enable failure after commit is not rollback; no generic engine activation API |
| Drain | Actual managed Fx child observes cancel and remains held; original file Check, native Worker stopped/inflight, join | Actual Eino stream producer observes cancel and remains held; original owned handle Check, native Worker stopped/inflight, join | Native Stop/reader Close and actual work exit stay distinct; borrowed file is not closed |
| Identity | Real before-commit native attempt witness, SQL receipt, original Workflow after Worker Kill+Wait | Actual Activity/step witness even on sealed redelivery; original artifact, checkpoints and new worker after Kill+Wait | Compacted retry history is not used to guess original attempts; unknown outcomes are not replayed |

Candidate rollback covers active topology/routing, not Definition Registry.
Native examples register definitions during construction, so a failed preparation
leaves registered references. The positive admission control uses a fresh Forest
with the same queued Workflow and profile. The original Forest is independently
proved unchanged and callable. No Core/Adapter modification is made to hide this.

Common evidence is an explicit fixture projection. Original snapshot, output,
native status, receipt/effect, invocation count, replay, checkpoint corruption and
process recovery assertions are retained. The TestKit does not interpret Temporal
retry errors, Eino Interrupt/Resume, Fx DI hooks or SQL Commit uncertainty, and
does not replace the original real-service acceptance with simulated execution.
