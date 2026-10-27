# Independent topology review

2026-10-27. Baseline `657a783`; initial implementation `cae76ce`. Approved
Spec/Design remain ignored and local. Two read-only reviewers independently
checked requirements and engineering correctness.

## Standards

One P2, closed: Restore derived retry identity from the latest desired topology.
After a later Reconfigure, an identical original Request incorrectly conflicted.
The ledger now retains the original explicit Request from its intent, including
replay and reconciliation. Restore checks that original identity first. Receipts
from earlier native Generations or other sessions remain historical and do not
admit an empty Forest.

The regression failed before the fix and passed afterwards. It covers changed
Desired, changed-payload rejection, historical selection and cross-session replay.
Focused re-review and the reviewer's independent external reproduction passed;
zero remaining correctness/ownership/durability findings or separate heuristic
smells were reported.

## Spec

One P2 acceptance gap, closed: separate history tests and process-kill tests did
not jointly prove historical rollback after an actual process restart.

`TestPersistedHistoryRestoresComparesAndRollsBackInANewProcess` now joins a writer
that creates two actual revisions, starts another process, cold-restores v2,
compares history and rolls back through native Apply to v1. Evidence includes
actual file-backed capability reads, different sessions/processes, restarted
Generations 1/2, an increasing third Revision and immutable previous history.
The original nine Kill+Wait windows remain. Focused independent re-review/test
passed and reported zero remaining Spec findings or scope creep.

## Disposition

Both findings belong to this new extension/its acceptance, not to Forest Core.
Core, stable Adapters, TestKit, Agent Runtime and Focus-Go remain unchanged.
Final-source platform/race/full-CI and fixed-version external-consumer results are
separate gates, recorded in [validation](validation.md).
