# Fx × Temporal × GORM

One Forest composes released `adapters/fx`, `adapters/temporal` and `adapters/gorm`
v0.1.0, with Core v0.3.0. This independent module requires Go 1.26. No Core,
adapter, Agent Runtime or Focus code is changed.

```text
Temporal Server → Activity Attempt / Forest Scope
  → declared Fx Access Port → native Fx service / Ref-backed constructor injection
  → versioned Business Tree → owned GORM pool → real PostgreSQL transaction
```

The ordinary `app` ownership root nests client, worker, Fx and business trees.
The fixed database resource is outside that subtree; the business tree declares
a Construction dependency on it. Siblings collaborate through explicit Links.
Fx constructors remain inside Fx, not individual Forest trees.

## Run with real services

From the repository root:

```sh
python scripts/test-cross-adapter.py
python scripts/test-cross-adapter.py --race --count 3
```

The script owns isolated real Temporal and PostgreSQL services, unique ports,
queues and test IDs, then cleans its own processes/containers. It uses the fixed,
checksum-verified Temporal CLI v1.9.1. It never substitutes an SDK mock server.

Against existing explicit services, set `FOREST_TEMPORAL_ADDRESS` and
`FOREST_POSTGRES_DSN`, then install/run the fixed module:

```sh
go run github.com/Leeminjing/go-forest/examples/cross-adapter/cmd/demo@v0.1.0 \
  -setup -queue isolated-cross-demo -id stable-operation -directory ./native-state
```

`-setup` explicitly migrates example tables; tree lifecycles do not migrate.
Use a fresh test ID for another independent demo. Workflow IDs are explicit;
Start does not silently randomize an empty ID or reuse a closed workflow.

Expected evidence:

```json
{"FirstVersion":"v1","LaterVersion":"v2","ReceiptVersion":"v1","Reused":true,"EffectCount":1,"Generation":2}
```

The second workflow uses v2 but returns v1's saved receipt. That is reconciliation,
not a mixed-generation call. Stage traces separately record Activity, Fx,
business and GORM callback generations. Process/physical identities qualify
local generations; Generation 1 after restart is not global version continuity.

## Boundaries

- `New` prepares and atomically commits the topology, leaving both business
  gates disabled. `Enable` opens Fx first, then native Worker polling. A later
  enable failure cannot roll back the committed graph.
- Activities use their declared Borrower Ref and Fx `Access.Use`, propagating
  its callback Context. `Bridge.Run` is for fresh external ingress and must not
  be used to re-enter this already scoped Activity.
- Fx keeps native Module/DI/Lifecycle. Its injected proxy stores a Business Ref,
  never a permanently borrowed raw business/database pointer.
- One PostgreSQL transaction serializes by OperationID, checks a versioned input
  digest, updates a separate effect counter and commits a complete receipt.
  Identical commands reconcile; different input under the same key fails.
- Native retries may execute an Activity many times. This protocol protects
  only the explicit SQL effect, not arbitrary network side effects.
- Domain errors cross Fx as ordinary errors. The final Activity boundary emits
  concrete native Temporal failures, preserving retry/non-retry/cancel semantics.
  Commit uncertainty and post-commit observation failures require reconciliation;
  cancellation never claims it reversed an already committed effect.
- `Close` seals/cancels host requests (including a long native Get), drains the
  Worker, then drains Fx, removes the subtree/database and awaits actual Stop.
  A timeout ends only that wait; retry Close with a fresh Context. Diagnostic
  host calls, active attempts, Fx borrows and Scope references are distinct facts.
- A native Fx-owned file handle and the owned SQL pool are real resource probes.
  Their lifetime follows actual work, not only SDK Stop or Context cancellation.
- Management must use a fresh host Context; activities cannot synchronously
  wait on their own Close/Switch. Framework instances/pools remain stable while
  compatible business providers are replaced or rebound.

`Options.Forest` permits an unrelated existing topology. It is borrowed: failed
preparation preserves that topology, and Close removes only `app`/`database`.
The example reserves those logical IDs and definition IDs for one installation
per forest; registration is not a reusable multi-application container API.
After publication, identity inspection uses a fresh bounded Context. If that
inspection fails, New returns a non-nil Runtime with the inspection error;
the host still owns Close and must use a fresh management Context for cleanup.
Hooks in Options are explicit acceptance fault/observation seams, not plugins.

## Process recovery

The real-process test kills Worker A after a confirmed database commit and before
Activity return. Worker B uses the same Server, queue, workflow registration,
OperationID and SQL protocol, but a fresh Forest/Fx/Worker identity. It returns
the saved receipt without increasing the effect count. The original WorkflowID
and RunID complete. A's native hooks are not reported as gracefully stopped;
B must separately demonstrate normal drain and resource release.

The CLI's `-worker -ready PATH -stop PATH` mode uses the same public RunWorker
supervision. `-commit-marker PATH` is an acceptance-only post-commit barrier;
do not enable it for normal work. It is not workflow persistence/checkpointing.

Default `go test ./...` explicitly skips real-service tests. Full acceptance sets
`FOREST_INTEGRATION=1`; missing real configuration then fails. The subprocess
entry-point skip in the parent is not an integration pass or missing test gate.

See [acceptance](../../docs/cross-adapter/validation.md),
[release](../../docs/cross-adapter/release.md), [review](../../docs/cross-adapter/review.md)
and [observed conformance rules](../../docs/cross-adapter/conformance-notes.md).
