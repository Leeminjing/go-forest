# Core performance acceptance

The approved Spec/Design stay local and ignored. This work changes four internal
Core files, adds regression tests and an independent Go 1.23 measurement module.
The existing Core API, Go floor, transaction validation and lifecycle protocol
remain unchanged. No release tag is rewritten or created by this experiment.

## Correctness evidence

| Boundary | Evidence |
| --- | --- |
| Local routing | Per-Graph `(consumer, port)` descriptor index; Ref still resolves actual capability from its pinned graph |
| Candidate preparation | Index completed before factories/Start/Readiness; real Construction/Startup Nest dependencies and Runtime rejection exercised |
| Old calls | One held invocation reads its original provider and Many order after Rebind, unlink/reinsert, provider and consumer replacement |
| Identity and cardinality | Original Ref cannot resolve a new physical consumer; optional and One/Many errors retain their categories |
| Callback isolation | Mutating a supplied Many slice cannot change later results or graph descriptors |
| Resource retention | Existing lifecycle/state-machine tests retained; internal regression observes zero old graph refs and cleared stopped implementation fields while diagnostics remain |
| Allocation slope | Baseline regression reproduced 472 -> 106,680 B/op with 1,024 unrelated bindings; candidate passes the same bounded allocation check |

The first complete CI run for the optimized Core,
[40646918517](https://github.com/Leeminjing/go-forest/actions/runs/40646918517),
passed all **65** jobs: the original 56 plus nine candidate/performance jobs.
The candidate jobs cover every module on Windows and Linux, Linux race3, five
real-service groups (ecosystem, cross-adapter, Temporal/Eino, Temporal,
incremental topology), and existing Observatory/framework browser acceptance.
Original fixed-version external consumers remain separate and also passed.

## Candidate dependency provenance

`scripts/test-candidate-core.py` archives a named commit into a new private
source directory. Only its copied module files receive a Core replacement.
The user's checkout, published Adapter versions and Agent/Focus repositories
are not changed. A PATH Go guard checks the exact selected Core directory and
source hashes before test/build/run/vet/list, including child worker builds.
The runner also verifies Core package directories from `go list -deps -json`.
Stdlib-only TestKit absence is explicitly recorded, not treated as a failed
module lookup that can be silently ignored.

Artifacts contain the source identity, original/effective module hashes,
commands, per-Go-invocation module proofs and a completion marker. Candidate
proofs do not masquerade as the unchanged published-version acceptance jobs.

## Measurement protocol

[Results](results.md) link raw matched measurements and describe all collection
corrections. Call benchmarks use ten repeated samples, Go 1.23.12 Linux/amd64,
PGO off, one CPU unless the parallel matrix explicitly varies it. Docker has an
8-CPU quota and 8 GiB memory limit. Profile/load/long-run measurements are separate
from uninstrumented timing and race correctness. All large inputs are created
through public Forest APIs and validate their actual dimensions/results.

The summarizer refuses different harness revisions, Go versions, platform/CPU
limits, environment, benchmark arguments or dimensions in a pair. Its regression
checks incompatible dimensions/Go versions/quotas and the permitted timeout-only
change. Results are medians/min/max, with no inferred statistical significance.

The measurement runner never overwrites a run directory. A complete individual
benchmark from a partially completed matrix can be used; files without PASS are
excluded from summary. Setup/download/timeout failures remain separate evidence.
The Controller tree cap is respected rather than changed for the experiment.

## Scope and remaining costs

Each immutable graph owns its index map/slices; bindings contain no capability
object cache. Construction takes sorting/grouping time and additional storage.
The core still clones and validates the whole graph, scans retired diagnostics,
and traverses old graph nodes when the final scope releases. This experiment
measures those costs rather than rewriting them. Business objects/resources,
diagnostic metadata, current/held graphs, Go heap and process RSS are reported
separately. Topology rollback does not undo external effects.

## Native Windows completion

Go 1.23.12 root/performance/topology/TestKit checks passed. Native Windows Go
1.26.5 real-service acceptance passed all five candidate groups, including actual
PostgreSQL/NATS/Temporal services, process Kill+Wait/recovery and ecosystem
browser checks. Separate candidate Observatory and frameworks browser suites
also passed. [Run/provenance counts](data/windows-acceptance.json) retain the
successful run identities; the failed pre-test CLI download is separately noted
in results and remains in local evidence.

All production Core files still match the measured c44f272 bytes. The later
measurement-only review changes do not replace the measured runtime with a
different implementation. Raw evidence has a local `.gitattributes` override
so CRLF conversion cannot invalidate byte checksums on checkout.
