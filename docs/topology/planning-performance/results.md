# Operation-local Topology planning reuse

Topology now keeps one privately owned canonical value, encoded byte slice and
tree index for each external Manifest admitted to an operation. Preview reuses
those values for comparison, Catalog checking, planning and the target digest.
Definition fact keys and sorted port facts are built once per distinct complete
definition identity in that operation. Binding indexes are built on demand.

This is not a cross-request cache. Reconfigure still reconstructs the expected
Proposal from the current Forest and Store, checks the historical target when
applicable, and compares the complete caller Proposal, including nil/empty
differences. Duplicate request handling retains its original ordering. The
planner's output simulation owns separate writable maps and still validates its
result. Journal records independently validate their own input; their canonical
values do not borrow a Controller's earlier proof.

## Frozen experiment

- Production before: `a9f75600afaeba5aee0b407bcd08bcb85eb2fdeb`.
- Production after: `f8078b9156bbd1d962d433f0af17d05a0c067fe6`.
- Ordinary timing harness: `199de69774523ef3a985b7b24a1e7772e2523458` on both
  sides. Core is published v0.3.1 with no Core replacement. Only Topology is
  replaced with its archived source. Subsequent commits add validation,
  diagnostic helpers, CI and evidence without changing measured production.
- All 650 baseline observations and 65 attribution cases finished before the
  first production edit. The semantic attribution-tool update
  `90c203d0bee697937f4d3f65b9184a533aeb56c4` also completed its baseline before
  that edit. Its Capture alias follows the work into the new private helper.
- Linux amd64, Go 1.23.12, Docker `golang:1.23`, 8 CPU quota and 8 GiB memory
  limit. Ordinary benchmarks use `-cpu=1 -pgo=off -benchtime=1x`, ten fresh
  independent samples per operation/case. Profile, memory and attribution
  processes use GOMAXPROCS=4. This is a shared development host, not a dedicated
  production latency test. No speed or wall-clock pass threshold is imposed.
- N=32/1,024/10,000, E=16/256/256; H=0/16/64 additional real Reconfigure
  operations, plus N=10,000/H=4 replay. Replace, Runtime Rebind, no-op and Many
  reordering have distinct cases. The primary synthetic forests are roots;
  deep Nest, BindNest and dependency phases remain correctness regressions,
  not an implied nesting performance result.
- Both sides consume the exact same captured before/target JSON, Generation
  and real online journal bytes. Input inventories and physical record kinds
  are retained. Every timed Commit uses a fresh equivalent real prefix.
  Setup, input checks, final topology/Generation checks and Close occur outside
  the timed interval. Commit excludes its external Preview; PreviewCommit
  includes Preview followed by Reconfigure, with complete revalidation.

The raw paired measurements, command lines, selected modules, source hashes,
profiles, memory observations, exact compressed inputs, attribution and result
audits are published under `data/`. `samples.csv` and `summary.json` contain all
65 operation/case pairs and min/median/max values, including unchanged or
regressing observations. They are cumulative allocation measurements, not
resident memory measurements.

Generated Go executables are retained locally and excluded from public evidence;
their build information and source identities are included. Input manifests,
journals, raw observations and profiles are retained. The final pairing proof
also checks actual binary settings (including GOAMD64/CGO_ENABLED) and fixed
dependency checksums, with a separate post-audit compiler confirmation.

## Paired ordinary results

Medians of ten independent, uninstrumented one-iteration samples. These are a
new paired run, not a direct comparison against a timing from an older study.
All large rows below use N=10,000, E=256, H=0. Milliseconds and cumulative MiB
per operation are shown separately.

| Operation | Before ms | After ms | Before MiB/op | After MiB/op |
| --- | ---: | ---: | ---: | ---: |
| Capture | 13.819 | 16.539 | 12.583 | 12.580 |
| Normalize | 7.259 | 6.739 | 7.960 | 7.956 |
| Encode | 9.590 | 6.505 | 9.853 | 7.958 |
| Compare | 26.278 | 17.243 | 24.337 | 16.185 |
| Digest | 9.906 | 8.094 | 11.762 | 9.868 |
| Preview | 179.140 | 81.887 | 139.311 | 59.482 |
| Commit | 548.464 | 394.837 | 330.784 | 211.219 |
| PreviewCommit | 644.343 | 474.539 | 470.011 | 270.715 |
| Open | 538.893 | 461.638 | 290.050 | 242.560 |

Capture's primary elapsed median increased while its cumulative allocation was
essentially unchanged. Two ABBA rounds gave large Capture medians 18.630/18.078
ms (before/after), whereas Preview was 185.835/76.656 ms. This shows why the
study does not claim all entry points became faster. The same interleaved
control gave medium PreviewCommit 71.252/47.131 ms. Each side has four samples
per control; original results and ranges remain in `data/interleaved/`.

| Case / operation | Before ms | After ms | Before MiB/op | After MiB/op |
| --- | ---: | ---: | ---: | ---: |
| small / PreviewCommit | 6.263 | 4.721 | 1.924 | 1.127 |
| medium / PreviewCommit | 71.743 | 49.784 | 56.243 | 31.018 |
| large / PreviewCommit | 644.343 | 474.539 | 470.011 | 270.715 |
| history / PreviewCommit | 5.628 | 4.412 | 1.953 | 1.154 |
| rebind / PreviewCommit | 6.514 | 3.514 | 1.919 | 1.123 |
| noop / PreviewCommit | 6.218 | 3.794 | 1.831 | 1.043 |
| many / PreviewCommit | 6.585 | 3.822 | 2.043 | 1.249 |
| open-history / Open | 101.100 | 89.152 | 47.444 | 40.060 |
| open-large / Open | 1662.396 | 1372.953 | 961.880 | 794.982 |

Open measures full parsing/validation/locking on a newly copied, warm-cache
journal; it is not a cold-disk startup latency measurement. The large H=4 input
has 14 physical records. Small H=16 and H=64 have 38 and 134 records respectively,
including real session, intent and observation records, not only selections.

## Work attribution

The baseline Preview CPU profile put 33.33% cumulative samples in Normalize,
27.12% in Catalog.plan, 20.90% in json.Marshal and 7.34% in factKey. These overlap
and are not additive. It justified both canonical reuse and the operation-local
complete-identity fact-key/port-fact reuse. The latter removes repeated JSON
fact-key encoding and sorting for thousands of trees sharing a definition.

Direct actual invocation counts from the paired final attribution tool:

| N=10,000/E=256 path | Full normalize before/after | Direct Manifest JSON before/after | All Topology JSON before/after | treeIndex helper before/after | Full Proposal equality before/after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Preview | 7/3 | 9/3 | 40528/11 | 7/2 | 0/0 |
| Commit | 12/7 | 17/8 | 61071/40 | 14/4 | 1/1 |
| PreviewCommit | 19/10 | 26/11 | 101599/51 | 21/6 | 1/1 |
| Open | 7/5 | 11/5 | 10289/31 | 7/2 | 0/0 |

Manifest hash counts for Preview/Commit/PreviewCommit remain 2/4/6 respectively
on both sides. Reconfigure still performs one complete Proposal equality
check. Replay reuses a record-local target digest for its diagnostic Entry;
frame-chain, intent, plan, receipt and field-whitelist checks remain independent.

The one traced Preview's inclusive Catalog.plan allocation was approximately
67.80/27.70 MiB before/after; its own allocation excluding traced child spans
was 7.79 MiB/4,720 bytes. Instrumented values include probe costs and are not
ordinary benchmark allocations. `data/attribution.json` retains every stage's
inclusive/self time and allocations, including complete Proposal comparison.

## Live memory and process high-water marks

These are single separate diagnostic runs, not medians. Allocated is the
explicit operation's cumulative allocation window. Held heap is post-GC with
the fixture/Store and closure inputs still retained. Closed heap is after Close,
dropping the closures and one GC. Peak RSS is the complete process high-water
mark including setup and cleanup. Open uses N=10,000/H=4; other rows use H=0.

| Operation | Side | Window allocated MiB | Held heap MiB | Closed heap MiB | Peak RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| Preview | before | 147.31 | 23.27 | 4.57 | 95.95 |
| Preview | after | 67.51 | 23.27 | 13.09 | 87.85 |
| Commit | before | 342.80 | 26.58 | 4.57 | 102.93 |
| Commit | after | 231.19 | 26.55 | 14.87 | 102.05 |
| PreviewCommit | before | 503.03 | 27.06 | 6.57 | 98.48 |
| PreviewCommit | after | 295.87 | 24.57 | 4.56 | 92.84 |
| Open | before | 992.08 | 51.79 | 4.56 | 144.27 |
| Open | after | 835.02 | 54.81 | 4.58 | 140.66 |

Cumulative allocation falls, but live heap does not uniformly fall. In
particular, single post-close Preview/Commit samples retain more heap in the
candidate and Open's held heap is higher. This experiment does not attribute
that variation to a specific pool/stack, establish a steady-state retention
improvement or claim leak elimination. Raw in-use profiles and observations
are retained. Actual resource closure, old Scope completion and instance
retention remain separate correctness assertions; none were weakened to lower
these numbers.

## Measurement boundaries

Attribution binaries are separate AST-instrumented copies. Function-span time
and allocations include probe overhead; nested inclusive figures are not
additive. They identify work and allocation sources rather than application
latency. JSON/hash counts include necessary enclosing Intent, record and receipt
encoding; only `json-manifest` counts direct Manifest values. Sort `Bytes`
fields count items, not bytes. `tree-index` counts the helper's actual map
construction; canonicalization also creates its own validated node index.

The original allocation profiles are kept but may lag the tail's latest GC
cycles. The independent audit harness at `4d5eae4` adds two GC cycles before
writing supplementary allocation profiles, equally for old and new code. This
does not alter the frozen ordinary benchmark. Allocation profiles are
process-cumulative and include fixture setup; CPU profiling starts after setup.
Only the explicit MemStats allocation window measures the individual operation.
Heap/live and RSS/peak observations are separate; process RSS includes setup,
Go runtime retention and close, and is not equivalent to live application data.

Four separately built result-audit binaries (old/new, ordinary/instrumented)
retain raw outputs and compare all returned canonical values, encoding, digest,
Diff, Proposal, receipt and fixed-journal projections. Proposal position is
first checked against the real Store; only random StoreID/digest are renamed,
while sequence remains. Receipt session, generation and every instance are
checked against the actual Store/Forest before identity renaming; retained vs
new instance relations remain observable. Open compares complete Inspect,
History and Journal on identical frozen bytes without removing any field.

## Limits and remaining costs

Standalone Normalize still has to copy, sort, validate and encode an untrusted
Manifest. Independent Compare still validates both external inputs. Native
Forest.Apply, Sync frequency, record format, history retention, public query
detachment and projection verification remain in place. These results isolate
repeated management work; they do not establish distributed consistency,
production P95/P99, or a faster business/database workload.

No release tag is created or rewritten in this study. Core v0.3.1 and Topology
v0.2.1 remain the previously verified released baselines.

## Reproduction

Use Linux Go 1.23.12 with the quotas and environment in `data/paired-method.json`.
The complete ordinary run is `scripts/measure-planning.py --source-ref <ref>
--harness-ref 199de69774523ef3a985b7b24a1e7772e2523458 --label <label>
--matrix study --count 10 --profiles`. Run the baseline first, then pass its
exact `inputs` directory to the candidate with `--inputs`; the runner verifies
the file inventory. Filesystem/caches remain part of the recorded environment.

`scripts/attribute-planning.py` uses the same frozen attribution tool on both
primary runs. `scripts/interleave-planning.py` runs the unchanged harness in
ABBA order. `scripts/audit-planning.py` verifies primary and attribution sources,
consumes the actual attribution archives and overlays the independent audit
harness at `4d5eae4`; `scripts/test-planning-evidence.py` exercises its negative
provenance seams. `scripts/report-planning.py` checks pairing before producing
samples, summaries, compressed evidence and hashes. Complete actual arguments
and paths for this run are retained in each artifact group's `commands.json`.
