# Core state-machine acceptance

Baseline: `7ae1354`. Approved Spec/Design remain local and ignored. The tests use
an independent standard-library-only reference model. No confirmed Core defect
has been found in the runs recorded so far. Deliberate observation corruption
is labeled as a tool self-test, never as a Core defect.

| Gate | Public/native evidence |
| --- | --- |
| A01 independent oracle | Model-owned DTOs, child adjacency/Kahn constraints; hand-written golden/atomic/retention cases and independent review; dependency audit |
| A02 random plans | TestModelRandom: model-precomputed inputs, fixed seeds, actual accepted/rejected/operation counts and per-plan comparisons |
| A03 topology/boundaries | TestModelKnownRules and supervised corpus: Nest/Link/BindNest, exported ancestry, directional visibility, interface/semantic mismatch, required/One/Many and cycles |
| A04 generation/identity/order | Empty and nonempty no-op, failed/conflicting plans, Replace/Move/reused TreeID and binding ID; actual Many callback order and snapshot Order |
| A05 rollback/diagnostics | Active topology projection, actual old routes, failure events separate from generation; preparation rollback and detached snapshot mutation |
| A06 concurrency | TestModelPinnedSchedules and TestModelWriterSchedules: entered callbacks, multiple pinned versions, fresh calls, common-start/prepared writers and conflict details |
| A07 phase/ownership | Real phase accesses, lifecycle partial-order journal, retained provider/consumer constraints, native dependency/owner reads during Stop, ownership reversal |
| A08 cancellation/drain | Cancellation before release, whole pinned graph probes, current versus retired Wait, Close admission, real native Close and exact counts |
| A09 preparation failure | Build/Start/Readiness barriers, precommit old routing, canceled preparation, entered construction Ref child held beyond cancellation, rollback cleanup |
| A10 replay/reduction | Exact JSON codec/replay, stable action IDs and version intent, same-fingerprint native shrinking, three replays, invalid schedule/anchor/budget counterexamples |
| A11 platform/race | Windows default/extended; Go 1.23 Linux default/extended race3; child build/status/log evidence; additive CI job and artifacts |
| A12 review/scope | Independent Spec/Standards review, original CI gates retained, protected paths and private document/identity audit; final result recorded after completion |

The [test README](../../tests/statemachine/README.md) documents commands, finite
catalog/budgets, native witnesses and reducer limits. Default skipped worker/tool
roles do not count as native acceptance.

## Local results before independent review

- Windows Go 1.26.5: root vet and full default tests passed. Extended profile
  completed 96 seeds / 24,576 plans: 14,782 accepted, 9,794 rejected. All eleven
  operation kinds had actual successes and rejections. Extended call schedules
  passed up to eight groups of 128 actually entered calls.
- Native tool self-test reduced its marked error from four steps to two, then
  reproduced the same signature three times. Missing setup/version anchors,
  impossible barriers and exhausted budgets fail closed.
- Linux Go 1.23: root vet/default race3 and the extended two-package race3 gate
  passed. Extended statemachine completed in 358.199s on the local Windows-backed
  Docker filesystem, within the 480s bound. Final review corrections are checked
  separately and final CI re-executes the complete source.

Topology equality excludes live States, historical Instances and EventSequence;
they are captured independently as diagnostics. Failure may legitimately add
them. A candidate's old numerical Generation is not evidence of publication.

No Core API/ABI change or version tag is planned without a confirmed reproducer
requiring a minimal fix. Stable Adapters, TestKit, Agent Runtime and Focus-Go
remain unchanged. Persistence/recovery and new Adapter work are outside scope.
