# Cross-adapter acceptance

Baseline: `99b01d4`. The new independent example consumes fixed published Core
v0.3.0 and Fx/Temporal/GORM adapters v0.1.0. No adapter implementation is copied
or changed. Private Spec/Design remain local and ignored.

| Acceptance | Actual public-boundary evidence |
| --- | --- |
| A01 real end-to-end command and data | TestRealWorkflowCrossesFxAndDatabaseInOneGeneration; native CLI |
| A02 candidate failure and old forest continuity | TestFailedCandidateLeavesOldForestAliveAndCleansThreeAdapters |
| A03 disabled/partially enabled/full admission and enable failure | TestQueuedWorkflowWaitsForCommittedFxAndWorkerAdmission; TestWorkerEnableFailureDoesNotPretendToRollbackCommittedGraph; TestWrongEnableOrderReturnsDependencyUnavailableRatherThanHalfReadySuccess |
| A04 old/new scopes, Replace and Rebind | TestOldActivityKeepsVersionWhileNewCallsUseReboundOrReplacedBusiness |
| A05 native retry with new route | TestNativeRetryUsesNewGenerationAfterAnExplicitTransientFailure |
| A06 concurrent same-operation commands and conflict | TestConcurrentWorkflowsShareOneDurableEffectAndRejectInputConflicts |
| A07 concrete native retry classification after Fx | TestFxJoinedDomainFailuresBecomeConcreteNativeFailures; TestOutcomeUnknownAcrossFxStaysNonRetryableAndReceiptCanBeReconciled; TestPostCommitObserverFailureIsUnknownRatherThanARegularRetry |
| A08 cancellation before vs after commit | TestCancelBeforeCommitRollsBackButAfterCommitKeepsReceipt |
| A09 actual join, native Stop distinction and host waits | TestCoordinatedDrainKeepsActualActivityAndFxChildrenAliveUntilJoin; TestCloseCancelsLocalResultWaitAndDoesNotConfuseItWithAdapterLeak; TestActivityCannotSynchronouslyManageItsOwnPipeline |
| A10 actual killed worker and fresh process recovery | TestKillAfterCommitRecoversOriginalWorkflowWithoutRepeatingEffect |
| A11 native history replay and recovered worker graceful shutdown | TestReplayDoesNotInvokeBusinessAgain; real process recovery stopped report |
| A12 independent review, platform/race, external fixed consumer | release/review records and scripts/verify-cross-adapter-release.py |

Evidence is from actual Temporal Workflows/Activities, native Fx DI and real
PostgreSQL transactions. Channel barriers establish the version/commit/exit
ordering. Tests inspect real adapter counters and owned resource probes. They
never turn a cancelled Context or native Stop return into a fake physical join.

The separate receipt and effect counter are checked through the public Inspect
entry. Administrative pg_stat_activity probing confirms the killed process's
connections eventually disappear. Kill+Wait is not claimed to execute OnStop;
the recovered process must produce a successful normal stopped report.

The helper TestWorkerSubprocess is an executable role and intentionally skips in
the parent; it is actually run in separate child OS processes. During race runs
both child workers are instrumented, and the killed worker's log is also checked
for a race diagnostic (normal exit cannot flush its final report after Kill).

Implementation validation so far:

- Windows Go 1.26.5: real-service tests/vet and deterministic CLI passed.
- Linux Go 1.26: isolated real-service harness, three race rounds and real CLI passed.
- Explicit service mode fails if address/DSN is missing; default skips are not
  counted as real acceptance. No mock network replaces the integration gates.

Package-local coverage is not reported as whole-module coverage: callbacks run
in imported packages and the process helper; those need different instrumentation
to aggregate. The acceptance matrix, not a headline percentage, defines delivery.

Full standalone command from repository root:

```sh
python scripts/test-cross-adapter.py --race --count 3
```

Independent review and published-consumer results will be recorded before final
delivery. The example's SQL protocol does not establish cross-framework atomic
transactions, distributed generations, or exactly-once external effects.
