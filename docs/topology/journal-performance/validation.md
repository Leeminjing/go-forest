# Journal performance acceptance

The production diff is confined to `extensions/topology/journal.go`,
`records.go` and new `transition.go`. Fifteen measured topology production/module
files match the final candidate source. Forest Core, stable Adapter code,
TestKit, Agent Runtime and Focus-Go were not changed for this optimization.
Core v0.3.1 publication is separately documented in
[the release record](../../core-performance/release.md).

## Behavior and evidence

| Approved behavior | Acceptance |
| --- | --- |
| Fixed Core delivery | Published v0.3.1, Windows/Linux outside-repository empty-cache download, no Core replacement; old tags unchanged. |
| Frozen measurements | Old source ea90a93, harness ad828d9, final runtime 6cff55a; identical checksummed journals, 81 pairs/1,620 ordinary observations, profiles and separate memory probes. |
| Late validation isolation | Real invalid intent field and observation revision are re-framed/checksummed; error Prefix equals the separately opened valid prefix. Facts, sessions, requests/history and position are not partly published. |
| Persistence uncertainty | Existing short/partial/full-write and Sync failure tests retained. New complete-write/failed-Sync test checks unchanged confirmed memory, poisoning, and actual complete-frame reopen. |
| Completed Sync | New wrapper performs real Sync then cancels; the successful record remains published. Post-Sync Stat failure reports ErrPersistenceUnknown with the confirmed record still present; reopen agrees. |
| Return ownership | New tests mutate completed proposal Fact.Ports, nested Inspect/History/Journal data, failed and cached receipt Conflict/FailureCodes, and returned error Prefix. Subsequent validation, repeated error queries and cached retries remain intact. Existing original-input and basic receipt tests are retained. |
| Legacy interpretation | Released v0.1 history/pending fixtures, original retries, v1/v2 mixed append, planning evidence/version tampering and original-prefix preservation tests remain green. |
| Full replay projection | Supplementary audit compares full Inspect/History/Journal and all original offline request receipts for 12 frozen journals plus both released-v0.1 fixtures: 14/14 identical; retries neither append nor change bytes/state. |
| Real crash windows | Original incremental and cold-restore process tests still Kill+Wait at intent, preparation, Apply-returned and observation boundaries; pending results require explicit host resolution. |
| Real resources | Windows and Linux source-mode Gin/PostgreSQL tests preserve actual entry/DB InstanceIDs, Gin engine/endpoint and sql.DB identity through business update and rollback. Old requests stay v1/gen1, new v2/gen2, rollback new v1/gen3. Final close releases the pool; committed DB receipt remains v2. |

All validation runs use the new source where claimed. Original fixed-version
consumer jobs test the published versions separately and are not presented as
proof of this unpublished topology runtime. No topology tag was moved or newly
published in this stage.

## Local runs

- Windows Go **1.23.12**: `go vet ./...`, `go test ./... -count=1 -v` in
  topology, including actual child-process termination/reopen.
- Linux Go **1.23.12**: `go vet ./...`, `go test -race ./... -count=3
  -timeout=240s -v`, including all existing child-process scenarios and new
  transition tests.
- Windows real PostgreSQL17 + Gin, source topology:
  `python scripts/test-topology-incremental.py --count 1`; actual native
  tests ran, not compile-only. Host Go **1.26.5**.
- Linux Go **1.25**, PostgreSQL17, source topology via private modfile:
  vet + real `go test -race ./... -count=3 -timeout=240s -v` in the
  incremental example. No repository manifest was rewritten.
- Supplementary full-projection audit uses Go **1.23.12** and Core v0.3.1,
  switching only the private topology replacement between frozen source trees.

Raw acceptance logs, module/binary provenance and hashes are in `acceptance/`.
Race and service runs were outside the primary and interleaved timing runs.
The task-owned PostgreSQL container/network were removed after acceptance;
unrelated user containers were preserved.

## CI and review

The implementation CI on `468a8c9`,
[40711371104](https://github.com/Leeminjing/go-forest/actions/runs/40711371104),
passed **68/68 jobs**. All prior jobs remain, including Core state-machine,
TestKit, Adapter/true-service/candidate-Core integration, topology legacy and
published consumer acceptance. The added journal smoke verifies the frozen
measurement fixture and current source; it imposes no unstable latency threshold.
Subsequent changes add acceptance tests, audits, repeated controls and records;
the measured production runtime remains unchanged.

Independent Spec and Standards read-only reviews found no confirmed runtime
defect. Two acceptance gaps (physical request/session/kind counts and completed-
Sync outcome tests) were filled. Two supplementary tool issues (input digest
preflight and hard-coded corpus count) were corrected; input inventory/digest
checks reject wrong or incomplete corpora before running the audit.

## Limits

The full baseline tail completed during editing in its isolated old-source
workspace, rather than all baseline commands completing before any edit. It
finished before candidate measurement. This sequencing deviation and the
primary latency regressions are recorded in [results](results.md), together
with interleaved controls; no raw result is discarded.

No power-loss simulator, log compaction, new storage format, less frequent Sync,
global transaction, effect rollback, exactly-once guarantee or total-replay
linearity is claimed. Complete history/query ownership and the host's
reconciliation/admission responsibilities remain.

Spec and Design remain local ignored files and are absent from Git history.
