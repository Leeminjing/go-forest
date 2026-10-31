# Planning optimization validation

The measured production boundary is Topology only. Forest Core, stable
Adapters, TestKit, Agent Runtime and Focus-Go are unchanged. Local behavior
Spec and implementation Design remain ignored and outside Git history.

## Completed acceptance

| Boundary | Evidence / result |
| --- | --- |
| Exact old codec and error seams | Frozen v0.2.1 values/bytes/digests, 4 MiB limit, mutable input/output, forged claims and historical retry tests passed before and after implementation. |
| Minimum toolchain and concurrency | Topology Windows/Linux Go 1.23 vet/tests and Linux three-round race passed in the complete source CI. |
| Journal safety and real process death | Existing v1/mixed-v2, prefix, partial write, Sync/Stat, pending reconciliation, cold restore, rollback and real Kill+Wait tests passed. |
| Native application composition | Source Topology with real Gin/PostgreSQL passed Linux three-round race in CI; Windows native real-service acceptance also passed. Its selected module and production hashes are retained. |
| Existing compatibility matrix | Core extended state machine, TestKit, every existing Adapter, real services, browser and fixed published consumers retained; all 71 jobs passed. |
| Ordinary performance | 650 observations per side, identical frozen inputs and timing harness, all 65 operation/case pairs retained. |
| Work attribution | 65 paired cases with the same final tool `90c203d`; real normalization, encoding, sorting, hashing and complete Proposal comparison observed. |
| Return-value audit | Nine cases across four builds agree, using the actual final instrumented source. All frozen input and source-file hashes checked. |
| Method integrity | Compiler/platform, CPU identity, quotas, selected environment, actual non-VCS binary build settings, fixed dependency sums and ordinary commands agree. Negative tests reject valid-journal substitution and compiler/CPU/quota/environment/flag/source changes. |
| Scope and delivery | Measured production files still match `f8078b9`; no Core, stable Adapter, TestKit, Agent or Focus-Go code change and no release/tag movement. |

The complete production-source CI is
[run 41164845206](https://github.com/Leeminjing/go-forest/actions/runs/41164845206),
commit `fe3c6fbba60232597ff50b50b4428310e9a6a598`. Its 71 completed jobs are
recorded in `data/acceptance/code-ci.json`. Later evidence/helper updates do not
change measured Topology production files; their provenance regressions also
run in the bounded planning smoke job. The live final-commit CI is available
from the repository's Actions page.

The final compiler confirmation was performed from the original saved build
information after the result audit. It is retained separately from that audit's
original method-at-start metadata. This extends the pairing proof without
rewriting raw observations or rerunning them under different settings.

## Compatibility and safety

Before implementation, new regressions passed against the original code:
released v0.2.1 codec values/bytes/digests, exact 4 MiB boundary, complete
Proposal-claim rejection, nil/empty distinctions and historical request retry.
The codec golden was produced by a repository-external consumer of v0.2.1 with
no replace, not regenerated from the candidate.

The candidate preserves Capture's partial result on failure, directory and
descriptor checks, complete Proposal revalidation, stale-position rejection,
phase-dependent rebuilding, Many order, resource identity and rollback rules.
Existing negative tests retain malformed frame handling, verified-prefix
isolation, partial writes, Sync/Stat failure, cancellation and pending-intent
reconciliation. Existing subprocess tests still use actual Kill+Wait.

No byte/hash domains, journal interpretation versions, intent/request hashing,
receipt handling or public error classes are intentionally changed. A private
canonical representation cannot be supplied by callers, retained in a global
cache or used to authorize a later request.

## CI environment correction

The first candidate run, `41164618143`, failed browser installation before
application tests: ubuntu-latest resolved to Ubuntu 26.04, whereas the pinned
Playwright 1.58.0 installer rejected that platform. Its failed job logs and
pre-cancellation job snapshot are retained. The superseded run was canceled
after preserving those failures to release runners for the corrected complete
run.

Commit `fe3c6fb` pins only browser-bearing jobs to Ubuntu 24.04, which is in
[the pinned version's supported platforms](https://github.com/microsoft/playwright/blob/v1.58.0/docs/src/intro-python.md#system-requirements).
It keeps the same installer, tests and timeouts. Other jobs retain their prior
platform selection. This is separate from the earlier intermittent NATS drain
timeout in the v0.2.1 release record; that older issue has no confirmed root
cause or fix in this change.

## Native Eino fixture ordering correction

Final packaging CI `41167346986` completed 70 jobs successfully and failed the
Temporal/Eino candidate integration at `management_test.go`: "producer not
joining" after 0.01 seconds. The test treated producer entry after Send as proof
that the consuming visitor had already returned. A legal scheduling delay can
leave the visitor active at that instant; Joining is then correctly zero.

An owned module copy with a gated visitor reproduced this exact old assertion
deterministically against a real Temporal 1.9.1 service and native Eino. The
minimal test-only correction waits for the actual Joining status while checking
that the held producer/call has not finished. It uses the existing bounded
predicate helper and context, and keeps every subsequent generation, version,
error, resource-retention and real-exit assertion. No timeout, Core, stable
Adapter or example production code changed. The corrected actual-service test
passed 100 native Windows repetitions; both independent reviewers confirmed
that acceptance strength was retained. The full final source CI is rerun.

The failed service log, controlled original test and failure, targeted result,
source identity and complete failed-run job record are kept under
`data/acceptance/temporal-eino-*`. This fixture has no Topology dependency; the
measured Topology production files and all performance observations are unchanged.

## Baseline storage incident

The first baseline archive attempt failed with EIO when D: had about 56 MiB
free; it produced no benchmark observations. After checking SHA-256 against
retained ZIP archives, only three explicitly identified regenerable old
Temporal executable files were removed. No source, historical raw evidence,
profiles or ZIP archives were deleted. The rejected broad cleanup was not
retried. The complete replacement baseline was frozen and finished before
production editing.
