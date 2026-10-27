# Comparing execution-boundary evidence

Compare Fx × Temporal × GORM with Temporal × Eino, preserving their native
semantics. This is an evidence report, not an implemented public TestKit.

| Boundary from cross-adapter/conformance-notes.md | Temporal × Eino evidence | Reusable seam / native distinction |
| --- | --- | --- |
| Scope and derived cancellation propagate | Native ADK/Graph version tests, Server cancellation | Block two dependency resolutions, publish a route change, compare actual identities. Native lifetime ownership still needs each engine's own contract. |
| Commit is distinct from producer enable | Candidate/admission tests | Queue work before commit/Enable and observe engine calls. Eino has no generic Enable API; Temporal does. |
| Cancel, SDK Stop and actual exit differ | Native stream producer, consumer error and timeout tests | Hold noncooperative resource use after cancellation and compare live probes with native state; no force-kill goroutine abstraction. |
| Host waits can retain snapshots | Long Get close test | Cancel/join local host wait before instance retirement; durable Workflow waiting is not a local lifetime. |
| Error wrapping changes retry interpretation | Real native error classification and safe pre-engine retry | Test upstream retry decisions through its real engine, not only errors.Is. Eino Interrupt is a Paused value even when error=nil. |
| Execution identity differs from effect identity | Rebind/Replace, process restore, artifact contention | Keep native route/attempt/process distinct from original effect receipt. SQL atomic receipt/effect differs from an immutable artifact. |
| Persistence/replay differ from graceful drain | Three Kill+Wait windows, native history Replay, restored-worker Close | Sealed-step redelivery and actual native Resume have different witnesses; OS death does not imply OnStop. |

The second engine adds important cases: original checkpoint versus logical step
receipt, targeted Signal decisions, checkpoint profile matching, leftover native
checkpoints after completion, and exclusive directory handover after positive OS
exit. A checkpoint is not permission to rerun tools; a sealed receipt can make
redelivery safe without invoking the native engine at all.

Observed application defects during development included abandoned Running state
not being persisted as Unknown, a native checkpoint without a receipt permitting
fresh Query, and missing Graph heartbeat delaying Server cancellation. Regression
tests exercise each real boundary. No Core or published-adapter patch was needed.

Candidate helpers could package controlled barriers, identity comparison, physical
resource probes and installed-consumer provenance checks. Framework-specific
fixtures must still provide actual admission, cancellation and Join actions,
native interruption/error interpretation and side-effect reconciliation policy.
The current helpers remain local after this experiment; a later proposal can
select a minimal public TestKit surface from these witnesses and the completed
acceptance/review evidence. No unified Transaction, Checkpoint, Agent or Lifecycle
wrapper is added.
