# Eino adapter delivery — 2026-10-25

Published independent modules:

| Module | Version | Commit |
| --- | --- | --- |
| `github.com/Leeminjing/go-forest/adapters/eino` | `v0.1.0` | `281da25` |
| `github.com/Leeminjing/go-forest/examples/eino` | `v0.1.0` | `5b37a32` |

The example uses the fixed adapter version with no replacement directive.
Dependencies remain Forest `v0.3.0`, Forest Agent `v0.2.0` and Eino `v0.9.22`.
Existing release tags were not changed. Core has no changes from `68e583d`.

`python scripts/verify-eino-release.py` passed on Windows after publication:
it created a consumer outside this repository with GOWORK=off, downloaded and
verified all four released Forest modules, rejected any module replacement or
checkout source path, compiled a consumer-owned native-query interface, then
installed the published CLI by version and ran it with a fresh checkpoint dir.

```json
{"eino":"eino:v1:oak/v1:oak","forest_agent":"forest:v1:oak/v1:oak","generation":2,"trees":12}
```

The same external acceptance script runs in the `eino-published` CI job, in
addition to both modules' Windows/Linux vet/test and Linux three-round race
jobs. See the [Go workflow](../../.github/workflows/ci.yml) and
[GitHub Actions](https://github.com/Leeminjing/go-forest/actions/workflows/ci.yml).
Local completed checks and limits are in [validation.md](validation.md);
the two-axis review and resolved findings are in [review.md](review.md).

Spec/Design stay ignored locally and are absent from all Git history. Temporary
research, checkpoint and installed validation binaries stay under ignored
`.scratch/`. No Focus migration or changes to either Agent state machine were
included in this delivery.
