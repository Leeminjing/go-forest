# Fx integration acceptance

Implementation baseline: Forest `f4d6c26`. Core, Agent and Focus remain unchanged.
The independent adapter pins Fx v1.24.0 and uses public Fx APIs only.

This file records implementation evidence; it does not contain the local-only
Spec or Design documents. Final release/CI verification is recorded in
[release.md](release.md).

| Acceptance | Automated evidence |
| --- | --- |
| A01 native DI, named/grouped/private/lazy components and annotated lifecycle | TestNativeModulesKeepPrivateNamedGroupedAndIndependentObjects; TestInvalidExportsAndConstructorPanicDoNotPublish |
| A02 both directions and consuming interfaces | TestDeclaredConsumerAndFxProxyKeepOneVersionAcrossRebindAndReplace; examples/fx/internal/application and internal/business |
| A03 independent App/resource/export instances | TestNativeModulesKeepPrivateNamedGroupedAndIndependentObjects |
| A04 old/new pinned versions, Rebind and Replace, no Fx reconstruction | TestDeclaredConsumerAndFxProxyKeepOneVersionAcrossRebindAndReplace; TestRealModuleHTTPBackgroundAndConsumerShareDynamicBusiness |
| A05 failed candidate never admits business; old forest remains usable | TestCandidateHTTPAndLoopNeverExecuteAndFailedPlanReleasesListener |
| A06 Apply then explicit Enable; no duplicate OnStart | TestNativeModulesKeepPrivateNamedGroupedAndIndependentObjects; TestRealHTTPAndLoopQuiesceBeforeNativeResourceStop |
| A07 failures/partial resources/rollback errors | TestFailedInitializationAndStartCleanAllOwnedResourcesOnce; TestNativeRollbackWaitsForPreparedIngressBeforeStoppingDependencies; TestNativeRollbackErrorsAreNotErasedByLaterNoopStop; TestCleanupErrorRemainsUnreleasedInsteadOfBeingRetried |
| A08 actual hook exit distinct from cancellation | TestCancelledStartJoinsRealHookBeforeRollback; TestStopDeadlineDoesNotProveHookOrResourceRelease |
| A09 joined children, cancellation isolation, late callbacks | TestChildTaskKeepsScopeAfterServiceReturnsAndRejectsLateWork; TestIndependentCancellationAndConcurrentQuiesceJoinAllCalls |
| A10 real HTTP/producer stop, timeout/repeated waits, final native cleanup | TestRealHTTPAndLoopQuiesceBeforeNativeResourceStop; TestClosingRuntimeWakesQueuedAndFutureBackgroundCalls |
| A11 management restrictions, parallel quiesce, required dependencies, default replacement rejection | TestManagementBoundariesAndNativeActuatorStayPrivate; TestConsumerRequiredLinkStillProtectsFxRemoval |
| A12 real offline composition, race and platform matrix | examples/fx integration test and installed demo; CI matrix for both independent modules |

Runtime error diagnostics are also covered by
TestLoopFailureIsCheckableWithoutPoisoningResourceCleanup: sentinel errors and
panics remain checkable through Quiesce, while final owned cleanup still succeeds.

Tests use real native Fx construction and lifecycle calls. HTTP tests listen on
real loopback sockets and prove failed preparation releases the port by binding
it again. Channels establish concurrency order. Timeouts exercise cancellation
and provide test deadlines, rather than guessing commit/read ordering by sleep.

An owned cleanup failure intentionally stays unproven and observable; no test
claims arbitrary uncooperative goroutines can be killed. Original Fx hook order
is preserved, but native outer timeout abandonment is not used by the adapter.

Local validation during implementation:

- Windows Go 1.26.5: adapter/example tests, vet, real demo; passed.
- Linux Go 1.23: both modules passed three race rounds (final verification below).
- Adapter package-local coverage measured 90.4% after review fixes. This is not
  a coverage claim for every repository/package or every possible Fx module.

Commands for a released checkout:

```sh
cd adapters/fx
go vet ./...
go test ./... -timeout 60s
go test -race ./... -count=3 -timeout 90s
cd ../../examples/fx
go vet ./...
go test ./... -timeout 60s
go test -race ./... -count=3 -timeout 90s
go run ./cmd/demo
```

Before publishing the adapter tag, example development uses only an ignored
temporary workspace override. Released validation must use downloaded fixed
modules with GOWORK=off, no replace, and sources outside this checkout.
