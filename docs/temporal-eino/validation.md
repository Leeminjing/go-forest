# Temporal × Eino acceptance

Baseline ecf7492. Uses fixed published Core/Temporal/Eino modules; private
Spec/Design remain local and ignored. The deterministic model uses the original
Eino engine and real Temporal Server, not an SDK mock execution environment.

| Gate | Actual test/public boundary |
| --- | --- |
| A01 native Graph and ADK cycle | TestRealWorkflowUsesNativeGraphAndAgentThenSignalResumesCheckpoint |
| A02 preparation, admission, post-publication cancellation | TestCandidateFailurePreservesBorrowedForestAndCleansOwnedResources; TestQueuedTasksWaitForPostCommitEnable; TestCallerCancellationAfterPublicationKeepsBorrowedForestManageable |
| A03 old/new generations and compatible Resume | TestOldNativeAgentKeepsGenerationAcrossReplaceAndRebind; TestEarlyConsumerErrorJoinsProducerAndRetainsOldStreamGeneration |
| A04 signal identity and native continuation | TestStaleConflictAndDuplicateSignalsDoNotAdvanceNativeExecution |
| A05 retry and sealed receipt redelivery | TestRetryBeforeNativeAdmissionUsesNewGeneration; TestSealedStepRetriesDoNotResumeStaleNativeCheckpoint |
| A06 actual cancellation, timeout, producer drain | TestWorkerStopAndStreamCloseDoNotReleaseLiveNativeProducer; TestServerCancellationReachesStreamButCannotReleaseNoncooperativeWork; TestTimedOutAttemptRetainsOwnershipUntilActualNativeExit |
| A07 original workflow and checkpoint restart | TestKilledWorkerRestoresNativeCheckpointAndOriginalWorkflow/paused |
| A08 completed step restart | TestKilledWorkerRestoresNativeCheckpointAndOriginalWorkflow/completed |
| A09 unsafe restart/state corruption | TestKilledWorkerRestoresNativeCheckpointAndOriginalWorkflow/unknown; TestCheckpointFaultsNeverRestartTheNativeAgent; TestOrphanNativeCheckpointCannotAuthorizeFreshRun |
| A10 isolated sessions, operation conflicts and directory ownership | TestIndependentNativeSessionsReconcileOneArtifactAndRejectChangedInput; TestStateDirectoryCannotBeManagedTwiceAndBorrowedForestRemainsUsable; process ownership gate |
| A11 native replay and host management | Native replay in initial and process cases; TestHostCloseCancelsLocalGetWithoutWaitingForDurableWorkflow; TestActivityCannotSynchronouslyManageItsOwnResources; TestNativeErrorClassificationSurvivesTheActivityBoundary |
| A12 platform/race/review/published consumer/comparison | Separate final release/review evidence; comparison.md |

Selected Windows real-service tests have passed during implementation. Full
Windows/Linux, three-round Linux race, independent review and external consumer
execution remain final gates until their results are recorded in release.md.
No skipped parent helper role is counted as a passed service scenario: the
helper executes in actual child OS processes, instrumented under -race.

Kill+Wait evidence never claims graceful OnStop in the killed process. The
recovered process must separately close real handles and release its owner claim.
Artifact content/source and native engine invocation counts distinguish restored
state from silently starting again with an idempotent tool.
