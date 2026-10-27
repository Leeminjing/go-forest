# Adapter Conformance TestKit v0.1.0 delivery

Three independent module tags were published without rewriting earlier tags:

| Module | Version | Immutable source |
| --- | --- | --- |
| github.com/Leeminjing/go-forest/testkit | v0.1.0 | 50392aeb9b549950c798ff80e1031155603f47bb |
| github.com/Leeminjing/go-forest/examples/cross-adapter | v0.1.1 | 3cc22b93ad47d62c5c319d6dcd2e8884978b5d5d |
| github.com/Leeminjing/go-forest/examples/temporal-eino | v0.1.1 | 3cc22b93ad47d62c5c319d6dcd2e8884978b5d5d |

TestKit declares Go 1.23.0 and has no Core/framework dependency. The two Go 1.26
examples retain the previously published Core/Adapter versions and production
source/state formats. Their patch releases update tests, documentation and the
fixed TestKit dependency/checksums, not the native runtimes.

## Executed acceptance

- TestKit public positive/negative tests and vet passed. Linux Go 1.23.12 three
  race rounds passed. Windows local validation used Go 1.26.5; the minimum-version
  Windows/Linux matrix is also an explicit final CI gate.
- Both complete Windows real-service suites/vet passed using published TestKit
  with GOWORK=off: cross-adapter 35.100 seconds, temporal-eino 53.822 seconds.
  Default integration skips were not counted. Root regression tests/vet passed.
- Both complete Linux real-service suites/vet, original instrumented child
  processes, three race rounds and actual CLI passed before publication.
  Final local race runs took 194.216 seconds and 310.577 seconds respectively;
  durations describe these executions, not a benchmark.
- Local prepublication Linux used an ignored workspace resolving only TestKit
  locally. Existing Core/Adapters stayed published. Source cache verification
  there is distinct from the normal checksum-verified external acceptance below.
  Final CI reruns both real-service race3 suites without workspace overrides.
- Both examples use the same Barrier/Journal and all four public checks.
  Actual callbacks supply native task/activity/attempt and pinned Generation;
  same-generation tasks with swapped evidence fail. Drain rejects another work
  item's exit and missing identity, even for shared resources.
- Original SQL effects/receipts, Eino interruption/checkpoint/Resume/Unknown,
  native error/retry interpretation, cancellation, native joins, actual OS
  Kill+Wait, recovery, replay and physical resource assertions remain intact.
- Independent review closed 2 Spec and 1 Standards findings, including the
  committed-error cleanup subcase; zero outstanding. See [review](review.md).

The [A01–A12 matrix](validation.md) maps requirements to public/native tests and
[compatibility](compatibility.md) preserves the different framework meanings.
Candidate rollback leaves the Definition Registry intact; the original Forest's
routing and callability are checked separately from a fresh positive control.

## Fixed-version consumers

All three verifier scripts passed on Windows in temporary projects outside the
checkout, with GOWORK=off, normal checksum verification and no Replace/check-out
source directories. The standalone TestKit consumer checked its sole module
dependency, standard-library-only production imports, four mechanisms and a
negative identity case. It used no Core, Temporal, Eino, Fx or database package.

`verify-cross-adapter-release.py` checked six exact published modules, compiled
the consumer contract, ran vet/build and installed the v0.1.1 CLI. Against real
Temporal/PostgreSQL it asserted:

```json
{"FirstVersion":"v1","LaterVersion":"v2","ReceiptVersion":"v1","Reused":true,"EffectCount":1,"Generation":2}
```

`verify-temporal-eino-release.py` checked five exact published modules, compiled
the consumer contract, ran vet/build and installed the v0.1.1 CLI. Against real
Temporal it asserted:

```json
{"State":"completed","Text":"finished:v1:oak","EffectVersion":"v1","ResumeGeneration":2,"Artifacts":1,"Query":1,"Resume":1,"Apply":1,"Released":true}
```

The native CLIs complement the source process/race tests; installation alone is
not conformance evidence. Repeat the published gates with:

```sh
python scripts/verify-testkit-release.py
python scripts/test-cross-adapter.py --race --count 3 --published
python scripts/test-temporal-eino.py --race --count 3 --published
```

The expanded main CI contains 49 jobs: the original full regression set,
TestKit Windows/Linux minimum-version checks and its standalone published
consumer. The native service/race/published jobs remain mandatory. Its latest
main result is the final delivery gate; it is not inferred from local success.

## Preserved scope and ownership

Core, existing stable Adapters, root go.mod/go.sum and both examples' production
Go files have no changes from `5bb2b15`. Agent remains `4e01beb`; Focus-Go remains
`4bd07f3`. No new Adapter, product migration or universal execution wrapper.

Spec/Design and .scratch remain ignored and absent from Git history. Authors,
committers, annotated taggers and authenticated pushes are Leeminjing. Owned
local service harnesses are stopped/joined after acceptance; unrelated services
are not modified.

TestKit verifies explicit fixture evidence; it cannot certify omitted or invented
facts. It does not unify native retries, checkpoints, SQL transactions or process
ownership, and makes no cross-framework atomicity/exactly-once promise.
