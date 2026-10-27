# TestKit acceptance matrix

Baseline: `5bb2b15`. Local Spec/Design remain ignored and never enter history.
The new module uses only the standard library. No existing production runtime,
Core or stable Adapter implementation is changed.

| Gate | Public test boundary / retained real proof |
| --- | --- |
| A01 barrier order/misuse/cancel | TestBarrierCoordinatesActualWorkAndRejectsReentry; TestBarrierDoesNotInventArrivalAfterCancellationOrEarlyRelease; TestCancellationUnblocksActualBarrierWaitWithoutInventingRelease |
| A02 concurrent detached complete evidence | TestJournalKeepsConcurrentDetachedEvidenceAndFailsClosedOnLoss; TestInvalidEvidenceCannotRecoverIntoAnApparentlyCompleteTrace |
| A03 pinned versions and counterexamples | TestPinnedRejectsMixedMissingAndForeignInvocationEvidence; both original Replace/Rebind real tests use actual callback identity, including rejecting swapped traces from two actual tasks at the same generation |
| A04 candidate interval + positive control | TestAdmissionRequiresFullRollbackAndRealPositiveControl; both TestConformanceCandidateRollbackAndPostCommitAdmission cases |
| A05 physical drain + ownership | TestDrainRequiresActualExitAndPreservesBorrowedResources includes mixed/missing work; both TestConformanceRealDrainRetainsOriginalHandleAndBorrowedResource cases |
| A06 execution/effect identity | TestIdentitySeparatesNewDeliveryFromOriginalEffect; native test-only attempt witnesses added to both existing crash-recovery tests |
| A07 Fx/Temporal/GORM | Existing mutation/process tests plus new conformance admission/drain fixtures, real native file and PostgreSQL proofs |
| A08 Temporal/Eino | Existing mutation/process tests plus new conformance admission/drain fixtures, original native checkpoints/Resume/Unknown proofs |
| A09 minimum version/platform/race | Go 1.23 Windows/Linux tests/vet and three Linux race rounds; recorded at final delivery |
| A10 full native regression | Both complete real-service suites, original instrumented OS process Kill+Wait and Linux race3, not default skipped cases |
| A11 published external consumers | verify-testkit-release.py and the two original real-service published consumer scripts |
| A12 review/preserved scope | Independent Spec/Standards review and unchanged Core/Adapters/Agent/Focus checks at final delivery |

Full native Windows tests/vet and Linux real-service race3 passed, alongside
minimum Go 1.23 Linux TestKit vet/race3 and all three fixed-version external
consumer scripts. Independent Spec/Standards re-review closed every finding.
See review.md and release.md for separate source, publication and final CI gates.
