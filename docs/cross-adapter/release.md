# Fx × Temporal × GORM composition v0.1.0

Published module:
`github.com/Leeminjing/go-forest/examples/cross-adapter@v0.1.0`.
Immutable tag `examples/cross-adapter/v0.1.0` resolves to
`9bc3d6e501b4db4778f46d201d376ebc77cfb2ad`.
The module requires Go 1.26 and uses these unchanged published dependencies:

| Module | Version |
| --- | --- |
| Core | v0.3.0 |
| adapters/fx | v0.1.0 |
| adapters/temporal | v0.1.0 |
| adapters/gorm | v0.1.0 |

No replace directive or workspace override participates in release acceptance.
Native versions are Fx 1.24.0, Temporal SDK 1.50.0, GORM 1.31.2 and PostgreSQL 18.

## Executed acceptance

- Windows Go 1.26.5: real-service module tests and vet, plus root regression
  tests/vet, passed. Explicit integration mode fails if configuration is absent.
- Linux Go 1.26: isolated real Temporal/PostgreSQL harness, three race rounds
  including instrumented child worker processes, and the actual CLI passed.
  Final module race execution took 133 seconds; elapsed time is not a benchmark.
- Real barriers prove old Activity/Fx/SQL scopes remain on the old generation;
  Replace and Rebind route new work to the new business instance.
- Native retry, concrete non-retryable errors, SQL contention/input conflict,
  cancellation before/after commit, host result waits and actual child joins
  have separate targeted tests.
- After actual Kill+Wait following commit, a new worker process completes the
  original WorkflowID/RunID using the original receipt with effect count one.
  Native history replay does not invoke business code. Graceful resource release
  is verified separately on the recovered worker.
- Independent Standards and Spec reviews each found the post-publication
  cancellation ownership defect, fixed in `2f17e64`. Re-review found zero
  outstanding findings on either axis. See [review](review.md).

The [A01–A12 matrix](validation.md) links these claims to named tests. Local
Linux reused its dependency cache; published verification below used normal
Go proxy and checksum verification and did not disable GOSUMDB.

## External fixed-version consumer

On Windows, `python scripts/verify-cross-adapter-release.py` ran in a temporary
directory outside this checkout with GOWORK=off. It fetched the five exact module
versions, verified checksums, rejected every Replace record and checkout source
directory, compiled a consumer-owned interface, ran vet and built the consumer.
It then installed the tagged CLI and ran it against real Temporal/PostgreSQL:

```json
{"FirstVersion":"v1","LaterVersion":"v2","ReceiptVersion":"v1","Reused":true,"EffectCount":1,"Generation":2}
```

The installed executable performed native Fx construction, Temporal activities,
business replacement, SQL reconciliation and native replay before normal Close.
This CLI evidence complements the source process/race tests; it does not replace
them. To repeat all gates with isolated services:

```sh
python scripts/test-cross-adapter.py --race --count 3 --published
```

The main GitHub Actions workflow now has 43 jobs. The cross-adapter job runs the
same real-service/race/published command, alongside all existing module,
published-consumer, browser and ecosystem regressions. Its latest main result is
the final delivery gate, not inferred from the separate local runs.

## Scope and preserved repositories

Core, root go.mod/go.sum and all existing adapters have no changes from
`99b01d4`. Agent remains `4e01beb`; Focus-Go remains `4bd07f3`. No product migration,
new Adapter implementation, Eino composition or public TestKit is included.

Private Spec/Design and temporary validation files remain ignored and absent
from Git history. The SQL protocol protects only this explicit database effect;
there is no distributed generation, cross-framework atomic transaction, arbitrary
goroutine termination or universal exactly-once promise. See the evidence-based
[conformance notes](conformance-notes.md) for possible future test seams.
