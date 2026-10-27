# Core topology state machine

This package compares Forest's public API with an independent finite-domain
[reference model](../internal/topologymodel). The model imports only the Go 1.23
standard library. It does not call Core validation, reflection assignability or
private reference counters. Child-adjacency reachability and Kahn elimination
implement its graph constraints. The fixture catalog contains two distinct
consumer-owned Go interfaces, explicit capability boundaries, three dependency
phases, optional/required One/Many ports and lifecycle promises.

## Runs

```sh
go test ./tests/internal/topologymodel ./tests/statemachine -count=1
FOREST_MODEL_SEED=81276 go test ./tests/statemachine -run '^TestModelRandom$' -count=1 -v
FOREST_MODEL_PROFILE=extended go test -race ./tests/internal/topologymodel ./tests/statemachine -count=3 -timeout=480s -v
```

PowerShell: set `$env:FOREST_MODEL_SEED='81276'` or
`$env:FOREST_MODEL_PROFILE='extended'` before running the same go test.
Linux with a C compiler is used for race acceptance.

Default random coverage is 16 seeds × 128 plans. Extended coverage is 96 × 256,
with 16/32 maximum active nodes respectively. These are experimental bounds,
not Forest limits. A small mutable routing skeleton keeps successful
Link/BindNest/Rebind/Unlink operations in the experiment; fixed public-API cases
also exercise complete removal/reuse. Actual success/rejection and operation
coverage are logged, rather than inferred from intended input proportions.

Call batches span four historical generations at 1/8/32 calls per generation;
extended runs use eight at 64/128. Every call must reach its real capability
callback before mutations begin. Other schedules exercise common-start and
prepared writers, cancellation, actual join, post-Close admission, preparation
failure, ownership reversal and Construction/Startup resource retention.

Experiments run in the same compiled test executable's supervised worker role.
Race builds therefore instrument the children too; reports record actual
Go/OS/PID and race build flag. `GORACE` forces output to stderr and disables only
the artificial exit sleep; the parent waits and checks status and actual race
logs, including killed workers. No race checks are disabled.

## Evidence and replay

Artifacts default to `.scratch/core-state-machine/runs/experiment-*/` under the
root module. Set `FOREST_MODEL_ARTIFACT_DIR` to choose an owned evidence directory.
Each worker saves complete input **before** execution, result JSON and native
logs. Reports separate active topology from live instance/error/event diagnostics.
Factory/Ref/Stop callbacks supply actual generation, resource identity, data and
close evidence. Driver actor IDs are labels, not invented Core scope IDs.
Snapshot instance IDs are checked separately from native file keys.

Use an absolute saved path: go test executes the package in its own directory.

```sh
FOREST_MODEL_REPLAY=/absolute/experiment/input.json go test ./tests/statemachine -run '^TestModelReplay$' -count=1 -v
FOREST_MODEL_REDUCE=/absolute/experiment/input.json go test ./tests/statemachine -run '^TestModelReduce$' -count=1 -timeout=10m -v
```

Standalone trace JSON and the worker input envelope are accepted. Replay uses
materialized plans without regenerating from seed. Only reduction may expand
explicit Current/Stale/version-anchor intent using the independent model; it
never repairs versions from the actual Forest snapshot.

Reduction deletes plan blocks, individual steps/operations/actions, clears
KeepChildren and simplifies schema-preserving definition versions. It preserves
the failure rule, stable step, model constraints, phase/relation profile and
logical role. Impossible actor/barrier/resource schedules are rejected before
native replay. A completed final pass plus three same-failure replays earns
`minimized=true`: **1-minimal for those rules**, not globally shortest. Budget
exhaustion keeps the original and best trace with an incomplete result. Automatic
reduction has a short budget; the explicit Reduce entry supplies a longer run.

`TestModelNativeReplayAndReduction` deliberately swaps a detached observation to
verify the checker/reducer. Its trace has `TestFault=swap-resolution`: a tool
self-test, not a Core defect. Worker/Replay/Reduce entry tests skip unless
explicitly invoked; native replay/reducer/codec self-tests run automatically.

Cancellation, Wait timeout, native Close and OS process exit are different facts.
Failed Stop retains its dependencies and is tested in a separate worker; OS exit
is not counted as successful Forest Stop. The supervisor requests cancellation
first, then Kill+Wait if needed. Unconfirmed runtime races/deadlines preserve
input/logs and fail acceptance without being labeled a minimal Core defect.

This is bounded empirical verification, not proof of every Go implementation or
OS instruction interleaving. It adds no universal Adapter, persistence or
distributed execution API.
