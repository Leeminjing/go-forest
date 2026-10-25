# Temporal acceptance — 2026-10-25

Baseline: Forest `5cdb6fe`; SDK `v1.50.0`; Forest `v0.3.0`; Agent `v0.2.0`;
GORM adapter `v0.1.0`. Approved Spec/Design remain local ignored documents.

The official checksum-verified CLI `v1.9.1` runs real Temporal Server `1.32.0`
with a persistent SQLite file. Business storage is real PostgreSQL 18.
These are isolated test services, not mocked workflow execution. The development
server does not establish production HA or disaster-recovery guarantees.

| Gate | Automated evidence |
| --- | --- |
| A1 Candidate admission | `TestCandidateNeverPollsAndNativeStopIsNotPhysicalJoin`: workflow queued before candidate preparation; readiness barrier and rollback leave activity count zero/generation zero. Successful Apply still leaves count zero; explicit Enable starts execution. |
| A2 Native client | Replacement test uses ClientAccess and native WorkflowRun/GetWorkflow. Example uses native Query/Signal; retry test uses CancelWorkflow and checks native cancellation result. Only detached IDs/results escape callbacks. |
| A3 Composition | Nine real trees: jobs/worker Nest binding, separate native client, GORM/PostgreSQL, Agent/model/checkpoint/events/approval. Activity and business consumers declare their interfaces. No Core branch or Agent state machine change. |
| A4 Replay/idle | `TestRealAgentDatabaseWorkflowAndReplay`: Signal wait, replace business tree, Forest.Wait completes while workflow remains waiting; native replay of server history leaves model invocation count at one. |
| A5 Replacement | `TestClientAndActivityKeepOldBindingAcrossReplacement`: paused old Activity reads `1/1`; new Activity reads `2/2`; old provider Stop stays zero until the old callback returns. |
| A6 Physical exit | Real native Stop returns while a noncooperative Activity remains blocked. Status reports NativeStopped and Inflight=1, Forest.Wait times out, then drains after release. `TestManagedProducerJoinsAfterFunctionReturn` also retains Scope for owned subwork and denies late Guard access. |
| A7 Retries/cancel | `TestRealRetryCancellationAndOverlappingAttempts`: retryable failure, native heartbeat cancellation, timeout retry overlapping the old task; attempt two sees generation 2/provider 2 while the still-live attempt retains generation 1/provider 1. |
| A8 Worker crash | `TestWorkerKillAfterCommitReconcilesSameWorkflow`: real child Worker is killed and waited for after commit/before response. A new child takes over the same queue and Agent directory, completes the original Workflow/Run, retains one planned step and one committed mutation. |
| A9 Server restart | `TestActualPersistentServerRestartKeepsHistory`: independent actual Server process is killed, query fails while disconnected, Server restarts on the same persistent file, original workflow continues by Signal and history replays. |
| A10 Business outcomes | Eight concurrent duplicate operations return Changes=1; changed input is rejected. Unknown post-commit outcome remains nonretryable and is reconciled by reading the receipt. A native Agent ModelRunning run returns its continuation identity without a second Generate. |
| A11 Errors/control | Missing namespace Enable fails without reverting committed generation. Repeated Enable does not duplicate the activity; stopped Worker cannot restart. Repeated Quiesce retains stopped state. Native duplicate registration/panic rolls back and resources drain. Removed/nested control and unsupported modes are rejected. |
| A12 Engineering | Windows full harness vet/test/CLI; Linux full harness vet + three race rounds including actual services and process tests. Fixed-version outside-checkout verification is recorded separately in release.md. |

Commands: `python scripts/test-temporal.py --count 1` (Windows) and
`python scripts/test-temporal.py --race --count 3` (Linux). Explicit integration
fails on missing service configuration; the child-process test entry is not a
standalone acceptance test. Default module tests skip real-service cases unless
configured; that default skip is not claimed as integration evidence.

Windows Go version: 1.26.5. Linux toolchain: 1.26.10. Initial Windows package
coverage: adapter 82.2%, example library 82.3%; these package-local measurements
exclude imported internal packages and the CLI. No whole-repository coverage
percentage is inferred.

Limits: Stop cannot force Go goroutine termination; native SDK Stop is not
application Join. Context deadlines can leave work draining. Registration and
execution callbacks obey an explicit resource contract; arbitrary opaque
background work is unsupported. Worker/Client replacement is disabled. Native
Local/Standalone Activities, Session/Nexus/plugin lifecycle and escaped async
completion are outside this managed surface. Native retries can overlap and
external effects require business reconciliation. Forest Generation is local,
not a durable workflow address or distributed transaction version.
