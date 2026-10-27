# Temporal × Eino

An independent Go 1.26 example using published Core v0.3.0, Temporal Adapter
v0.1.0 and Eino Adapter v0.1.0. Native SDKs remain Temporal 1.50.0 and Eino 0.9.22.
No Core, existing adapter, Agent Runtime or Focus implementation is changed.

```text
Temporal Workflow → Activity / Forest Scope → native ADK Runner
  → consumer-owned Business Ref → immutable artifact
  → native tool interrupt → Workflow waits for Signal → native Resume
Temporal Graph Activity → native Graph Invoke/Stream → Business Ref
```

The deterministic model uses real Eino ChatModelAgent, tools, streams and native
checkpoints; no external model API or key is needed. Graph nodes remain inside
the original compiled Eino graph. Business versions can be replaced/rebound
without rebuilding Worker, Eino engines or the state resource.

## Run

Against an explicitly configured real Temporal development service:

```sh
export FOREST_TEMPORAL_ADDRESS=127.0.0.1:7233
go run ./cmd/demo -queue isolated-example -id stable-operation -directory ./state
```

Use unique queue, ID and state directory for another independent demonstration.
The demo starts the native workflow, waits for a persisted interrupt, replaces
business v1 with v2, sends a targeted continuation Signal, verifies the original
artifact, replays real history and closes actual local resources. JSON evidence:

```json
{"State":"completed","Text":"finished:v1:oak","EffectVersion":"v1","ResumeGeneration":2,"Artifacts":1,"Query":1,"Resume":1,"Apply":1,"Released":true}
```

The original effect belongs to v1. Resume uses a new Scope/generation and reads
that existing artifact; it does not call the completed apply_once tool again.

## Four separate facts

1. Temporal history records native scheduling, Signals, retries and results.
2. Eino FileStore preserves opaque original interrupt/checkpoint bytes and profile.
3. Application step receipts seal logical Run/Resume outcomes before delivery.
4. The immutable artifact records this example's controlled file effect.

An Activity retry uses the same logical step. Sealed outcomes are redelivered;
they never Query/Resume again, including an old native checkpoint remaining after
completion. A checkpoint without a step receipt, or an abandoned Running step,
requires OutcomeUnknown reconciliation. No fallback to a new agent execution.

Artifact publication uses synced temporary files and local atomic hard links.
Same operation/input reuses the existing artifact; changed input conflicts.
Only this explicit artifact effect is protected, not arbitrary side effects.
Native checkpoint, step receipt and Temporal acknowledgement are separate writes.
Their uncertain windows are tested instead of called an atomic transaction.

## Ownership and management

The state directory has one process manager, an exclusive local owner claim and
one in-process Eino Owner. A second process is refused. This is not a distributed
lease, automatic failover protocol or power-loss durability guarantee. Use local
trusted storage supporting hard links. Host code must cover actual resource work
with the published adapters' Guard/Go and Lifetime contracts.

The supervisor must Kill+Wait an abandoned owner before calling
`HandoverAfterExit(directory, oldOwnerToken)`. That API trusts the supervisor's
positive exit proof; it does not infer death from a PID check or timeout.
`Audit` reads immutable application evidence without mounting another FileStore.

`New` commits with Worker polling disabled. `Enable` opens polling explicitly;
enable failure does not roll back topology. If post-commit inspection fails,
New returns the non-nil manageable Runtime with its error; callers must Close it.
`Options.Forest` is borrowed. The example reserves app/state and te.* IDs for one
installation per forest and removes only its own topology on Close.

Close seals host requests and cancels local long Get calls, then Quiesces Worker,
awaits actual native work and removes app/state. SDK Stop, cancellation, reader
Close and real Join are distinct. A timeout ends the current wait; retry Close
using a fresh top-level Context. An activity cannot synchronously manage itself.
Native handles stay alive until their users really exit.

Hook/Prepared options are explicit acceptance observation/fault seams. Profile is
the host's expected execution schema; incompatible persisted state is rejected.
Input.AttemptMillis is a bounded, history-recorded native timeout override; zero
uses the 20-second default. Signals are transport-accepted first and validated by
Workflow state later; inspect Query.Decisions for acceptance/rejection.

The CLI also has supervised `-worker -ready PATH -stop PATH` mode. Its
`-barrier-mode paused|completed|unknown -barrier PATH` is fault injection, not
production persistence. `-audit` is read-only; `-handover-after-exit TOKEN` is a
trusted management action after the prior process was positively joined.

## Acceptance

Default tests explicitly skip real service cases. FOREST_INTEGRATION=1 requires
an actual Temporal address and fails if it is absent. See the
[acceptance matrix](../../docs/temporal-eino/validation.md) and
[comparison](../../docs/temporal-eino/comparison.md). Source process/race evidence,
review and published-consumer checks are separate delivery gates.

The v0.1.1 test suite uses the independent [TestKit](../../testkit/README.md)
for shared pinned-version, admission/rollback, actual-drain and execution/effect
identity checks. Production code, native state formats and Adapter versions are
unchanged. Original real-service, Kill+Wait, recovery and native assertions remain.
