# Temporal × Eino composition v0.1.0

Published independent module:
`github.com/Leeminjing/go-forest/examples/temporal-eino@v0.1.0`.
Immutable tag `examples/temporal-eino/v0.1.0` resolves to
`106b5957676fccb7f278d989a5ad4acdc42e2c9b`.
It requires Go 1.26 and uses unchanged published dependencies:

| Module | Version |
| --- | --- |
| Core | v0.3.0 |
| adapters/temporal | v0.1.0 |
| adapters/eino | v0.1.0 |

Native engines are Temporal SDK 1.50.0 and Eino 0.9.22. No replace directive,
workspace override, mock Temporal environment or model API is needed.

## Executed source acceptance

- Windows full real-service tests/vet and native CLI passed; final module test
  execution took 51.193 seconds. Root module regression tests/vet also passed.
- Linux full real-service tests/vet, three race rounds and native CLI passed;
  final module race execution took 258.474 seconds. Child worker processes use
  the race-instrumented executable and killed-process logs are checked separately.
  Elapsed times describe these runs, not a performance benchmark.
- Native ADK model/tool/interrupt/resume and compiled Graph Invoke/Stream execute
  inside Activity Scopes. Replace/Rebind keeps old calls on their original
  generation while later attempts and compatible Resume use a new generation.
- Native Signal classification, pre-engine retry, cancellation, timeout,
  noncooperative work, early reader closure, host waits and physical joins have
  separate tests. Cancellation and SDK Stop never stand in for actual exit.
- Three actual Kill+Wait windows cover sealed pause, sealed completion and an
  effect with an unsealed result. A new OS process resumes the original Workflow
  and original native checkpoint, redelivers a sealed result without engine
  entry, or stops with OutcomeUnknown respectively. A second process is refused
  while the directory owner is alive. No PID/timeout-based failover is inferred.
- Missing, corrupt, changed or incompatible checkpoint evidence never permits a
  fresh Query. History Replay does not execute tools again. Artifact receipts,
  qualified physical engine identities and invocation counters distinguish
  actual recovery from a silent fresh execution with an idempotent tool.
- Independent Spec and Standards re-review closed all findings: three Spec and
  two Standards findings, with zero outstanding. See [review](review.md).

The [A01–A12 matrix](validation.md) names the tests. Explicit integration mode
requires a real service; default skips are not counted as service acceptance.
Local Linux reused verified dependency caches; published acceptance below uses
normal Go module checksum verification rather than GOSUMDB=off.

## External fixed-version acceptance

On Windows, `python scripts/verify-temporal-eino-release.py` passed outside the
checkout with GOWORK=off. It downloaded the four exact modules, verified their
checksums, rejected every Replace and checkout source directory, compiled a
consumer-owned native Resume contract and ran vet/build. It installed the tagged
CLI and executed it against a real Temporal Server. Its asserted result was:

```json
{"State":"completed","Text":"finished:v1:oak","EffectVersion":"v1","ResumeGeneration":2,"Artifacts":1,"Query":1,"Resume":1,"Apply":1,"Released":true}
```

The original artifact belongs to v1; Resume enters generation 2 and reads the
existing effect without re-entering apply_once. This complements, rather than
replaces, the source process/race tests. To repeat all gates with an owned server:

```sh
python scripts/test-temporal-eino.py --race --count 3 --published
```

The expanded main CI has 46 jobs, including Windows/Linux module checks and the
real-service three-round race plus published-consumer command. Its latest main
result is the final CI delivery gate; local results do not imply CI success.

## Preserved boundaries

Core, root go.mod/go.sum and all existing adapters are unchanged from `ecf7492`.
Agent remains `4e01beb`; Focus-Go remains `4bd07f3`. No product migration, new
Adapter implementation or public TestKit was introduced. The evidence-based
[comparison](comparison.md) maps the seven earlier consistency boundaries and
records which native distinctions must remain explicit.

Spec/Design and scratch files remain ignored and absent from Git history. All
new commit authors, committers, tagger and push authentication are Leeminjing.
Temporal history, opaque Eino checkpoints, step receipts and controlled artifacts
remain separate records, without a distributed generation, atomic cross-engine
commit, distributed lease or universal exactly-once guarantee. Uncertain native
execution stops automatic replay and requires explicit reconciliation.
