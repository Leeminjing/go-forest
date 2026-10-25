# Eino adapter acceptance — 2026-10-25

Baseline: Forest `68e583d`; native Eino `v0.9.22`; Forest `v0.3.0`;
go-forest-agent `v0.2.0`. Specification and implementation design are approved
local documents, ignored by Git; this file records evidence only.

| Gate | Executed evidence |
| --- | --- |
| A1 Native engines | `TestActualEnginesCoexistAndNativeGraphBranches`: real compiled multi-node Graph/branch and Stream; real ChatModelAgent/Runner and Forest Agent, deterministic models/tools. |
| A2 Both directions | Example has 12 trees, 2 explicit Nest bindings, consumer-owned lookup and answer interfaces; ADK tools and Graph nodes use declared Forest Refs. Internal native nodes are not Forest trees. |
| A3 Coexist/rebind | 16 concurrent calls across both engines; `TestEngineRebindAndActiveNativeReplacement` atomically rebinds the consumer while an old task remains active. |
| A4 Replacement | Tool barrier retains the old provider/ADK version. `TestNativeOutputStreamPinsProviderAndEngineAcrossReplacement` pauses the old producer, replaces provider+Graph, verifies new `g2:p2/g2:p2`, old `g1:p1/g1:p1`, and old Stop counts zero before release, one afterwards. |
| A5 Physical completion | `TestNativeCancelDoesNotReleaseRunningResource`: real native Graph returns from immediate interrupt while guarded node remains blocked; Forest Wait reports drain timeout, Stop remains zero. `TestNativeADKCancelKeepsLeaseUntilRealJoin` separately proves CancelHandle.Wait does not release the execution ID. |
| A6 Streams/cancel | Full consume, early stop, callback failure, native cancel, Context cancel and external Lifetime cancellation tested. Native stream readers have one consuming callback and adapter-owned Close; owned producers finish before release. |
| A7 Native resume | Tool StatefulInterrupt and targeted ResumeParams; separate-process Kill+Wait then fresh native Runner/FileStore resume. Unknown checkpoint, busy identity, failed native save, corrupt latest revision, wrong engine/schema are errors. Native bytes are not converted into Forest Agent state. |
| A8 Rollback | Invalid native Graph build and unmanaged host rejected. `TestFailedChangesKeepBothEnginesAvailable` covers Start failure/cleanup, required binding removal and default unsafe replacement; generation stays unchanged and both engines still execute after each failure. |
| A9 Engineering | Windows Go 1.26.5 vet/test; Linux Go 1.23 vet plus three race rounds for both independent modules. CI adds Windows/Linux Go 1.23 module jobs. External fixed-version acceptance is recorded in release.md. |

Windows adapter statement coverage: 81.3%; example library: 78.4%.
The CLI/catalog packages were not instrumented by the example library's package
coverage command; these numbers are not whole-repository coverage.

No API keys, simulated execution engines, external databases or message services
are needed. Native Eino iterators still have unbounded internal queues; budgets
are not a memory sandbox. Lifetime safety is an explicit builder contract, not
automatic proof for arbitrary opaque agents. A noncooperative task can retain
resources indefinitely. Directory ownership is single-process; native side
effects are not promised exactly-once, and no power-loss durability is claimed.

Forest Core, go-forest-agent state machine, Focus-Go and Python Focus are unchanged.
