# Ecosystem conformance release

2026-10-27. Published module: `examples/ecosystem/v0.1.2`, source
`b6cdc812163b333c4ad3ebf43e9fbc52bd2e05e8`. Initial implementation `e2186ab`;
independent review corrections are included in the release source. The existing
TestKit `v0.1.0` and all previous Core/Adapter/example tags are unchanged.

## Source and native gates

- Windows Go 1.26.5: complete ecosystem tests/vet against real PostgreSQL 18 and
  NATS 2.12.4 passed (47.483 seconds). Desktop/mobile Chromium acceptance passed.
  Focused process (23.375s), pinning (6.394s) and actual-join drain (6.421s) tests
  passed after the relevant review/reporting corrections.
- Linux Go 1.26.10: the existing full harness ran vet and three race rounds for
  GORM, NATS, Telemetry, Observatory and ecosystem against its own real services.
  The ecosystem package passed in 112.634s, including real child processes and
  all original restart/crash/approval/Unknown tests. Child race logs are checked
  even when a child is intentionally killed.
- Spec review closed one empty-evidence coverage gap; Standards review closed
  two maintenance suggestions. Both focused re-reviews reported zero remaining
  findings. See [review](review.md).

The [A01–A12 matrix](validation.md) maps actual tests to requirements. The
[comparison](comparison.md) records why message redelivery, Agent checkpoints,
SQL receipts and Forest scopes retain different meanings.

## Published external consumer

`python scripts/verify-ecosystem-release.py` passed on Windows in a fresh
temporary project outside this checkout, with GOWORK=off, normal Go checksum
verification, `go mod verify`, no Replace and no checkout module directories.
It checked these exact versions:

| Module | Version |
| --- | --- |
| Core | v0.3.0 |
| go-forest-agent | v0.2.0 |
| Gin / gRPC / GORM / NATS Adapter | v0.1.0 each |
| Telemetry | v0.1.0 |
| Observatory | v0.2.1 |
| ecosystem | v0.1.2 |
| TestKit | v0.1.0 |

Module and test-package graphs contain no Temporal. The external consumer passed
vet/build, installed the fixed-version demo CLI, then used that binary with real
PostgreSQL/JetStream/Agent and desktop/mobile Chromium. The task completed with
`model v1: 5`; repeated input returned the completed result; runtime counts,
disconnect freshness and responsive layout checks passed.

The source native/race fixtures and published CLI are independent evidence;
neither substitutes for the other.

## CI, privacy and scope

The existing 49-job main workflow retains every prior gate. Its ecosystem job
now runs `--race --count 3 --browser --published`, including final-source new
tests and the fixed published module graph. The [main workflow's actual result](https://github.com/Leeminjing/go-forest/actions/workflows/ci.yml?query=branch%3Amain)
is the final delivery gate, independently of the local passes above.

Relative to `c2f7ac9`, Core, stable Adapters, TestKit implementation, root go.mod
and go.sum and all ecosystem production Go files are unchanged. Agent remains
`4e01beb`; Focus-Go remains `4bd07f3`, with clean workspaces. No Temporal
dependency, new large application, universal wrapper or Core random-topology
research was introduced.

Spec/Design and scratch are ignored and absent from all reachable Git history.
New authors, committers, the annotated tagger and authenticated pushes are
Leeminjing. Only owned verification processes/containers are stopped and joined;
logs and screenshots remain in ignored local evidence storage.

These checks validate explicit native facts supplied by the fixtures. They do
not promise cross-resource atomicity, exactly-once effects, distributed leases
or safe replay of an unknown tool outcome.
