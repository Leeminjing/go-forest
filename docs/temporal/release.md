# Temporal adapter delivery — 2026-10-25

Published independent modules:

| Module | Version | Source commit |
| --- | --- | --- |
| `github.com/Leeminjing/go-forest/adapters/temporal` | `v0.1.0` | `d62da76` |
| `github.com/Leeminjing/go-forest/examples/temporal` | `v0.1.0` | `e87c282` |

The example requires the released adapter and has no replacement directive.
Dependencies remain Core `v0.3.0`, Agent `v0.2.0`, GORM adapter `v0.1.0` and
Temporal SDK `v1.50.0`. The adapter/example require Go 1.26; Core remains Go 1.23.
Existing release tags were not changed.

`scripts/verify-temporal-release.py` passed after publication on Windows. It
created a consumer outside this repository with GOWORK=off, downloaded all five
fixed Forest modules, ran module verification, rejected all replacements and
checkout source directories, compiled a consumer-owned Client interface and
installed the example CLI with `go install .../cmd/demo@v0.1.0`.

The installed executable then completed a real Temporal Workflow using the
deterministic Forest Agent and PostgreSQL through GORM. Observed result:
`Plan=plan:oak`, `Changes=1`, `Version=1`, a nonempty native RunID and the original
business/Workflow ID. Checkpoints used a fresh external temporary directory.

The full [CI workflow](../../.github/workflows/ci.yml) runs Windows/Linux module
checks plus real Temporal/PostgreSQL, three Linux race rounds, Worker process
Kill+Wait, persistent Server restart and the same published CLI verification.
See [GitHub Actions](https://github.com/Leeminjing/go-forest/actions/workflows/ci.yml),
[acceptance evidence and limits](validation.md), and [two-axis review](review.md).

The official CLI v1.9.1 archive was checked against its pinned SHA-256 and reports
Temporal Server 1.32.0. Services use owned isolated test storage; this is process
recovery evidence, not production clustering or arbitrary exactly-once effects.

No Forest Core, Agent state machine, Focus-Go or Python Focus code was changed.
Spec/Design remain ignored local files and are absent from Git history. Native
source research, service state, test logs and installed evidence binaries remain
under ignored `.scratch/`; Python bytecode is ignored as temporary output.
