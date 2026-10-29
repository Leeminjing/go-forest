# Journal incremental state and replay measurements

The journal now validates a record into a private delta, writes the complete
frame, performs the original Sync, and then publishes that delta under the
existing Store lock. Scan uses the same validator and publishes only verified
records. It no longer JSON-clones all earlier histories, entries, receipts,
facts and session/request maps for every record. No protocol, history retention,
sync frequency, public ownership boundary or Forest transaction changed.

## Frozen experiment

- Before: `ea90a93b091a4e68b05ad4f10d0e8746694273be`.
- After runtime: `6cff55a7d8488f669de59a5f45eaf91534c59095`.
- Both use the exact harness `ad828d99c53da4d6c7acf432174dd0c7e668891c`
  and published Core **v0.3.1 without a Core replacement**. Only topology is
  replaced with its respective archived source. Later commits add tests,
  audit helpers and documentation; measured production files remain identical.
- Linux amd64, Go 1.23.12, Docker `golang:1.23`, 8 CPU quota and 8 GiB memory
  limit; benchmark `-cpu=1`, fixed `-benchtime=1x`, ten independent samples.
  Memory/profile commands use GOMAXPROCS=4. Full CPU/build/module/source metadata
  is saved separately for each side.
- The baseline source and harness were frozen before implementation. The
  baseline's long management tail finished during source editing, rather than
  the entire matrix finishing before any edit as originally planned. Its
  isolated old-source archive was unchanged; no candidate measurement began
  before baseline completion. This scheduling deviation does not mix candidate
  code into the baseline. The candidate consumes the **same original bytes and IDs**, checked against the
  baseline inventory, not regenerated similar-looking histories.
- Twelve real API-produced journals, 60 operation pairs plus 21 management
  pairs: **1,620 observations**. N=8/128/1,024/10,000; H varies independently
  where practical. H counts additional historical selections: revisions=H+1,
  physical records=H+2. This offline corpus has E=0 and zero sessions. The
  separate online management fixtures have E=16/256 and genuine session,
  intent and observation records. They are not represented as equivalent loads.

The exact compressed inputs, actual physical record/request/session counts,
sample CSV, comparison JSON, command lines, raw logs, CPU/alloc/heap profiles
and byte hashes are in [data](data/). `sha256.json` covers the raw primary
artifact files. Supplementary acceptance evidence has a separate manifest.

## History growth

Median ordinary benchmark values; elapsed time includes real Write/Sync for
Append and real Open/locking/validation for Open, with fixture copy, history
creation and Close outside the timed region. B/op is cumulative allocation,
not retained memory. Full distributions are in `comparison.json`.

| N | H | Append ms before → after | Append B/op before → after | Open ms before → after | Open B/op before → after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 0 | 1.963 → 3.412 | 67,592 → 52,528 | 0.234 → 0.403 | 75,992 → 72,960 |
| 8 | 16 | 2.422 → 2.846 | 190,888 → 55,672 | 5.019 → 4.010 | 2,201,600 → 1,079,608 |
| 8 | 64 | 4.532 → 3.695 | 529,728 → 51,576 | 56.939 → 15.055 | 19,902,192 → 4,082,016 |
| 8 | 256 | 10.249 → 4.510 | 1,898,952 → 51,576 | 794.061 → 62.273 | 258,857,268 → 16,114,976 |
| 8 | 1,024 | 27.400 → 6.172 | 7,705,032 → 51,576 | 13,438.210 → 256.598 | 3,892,362,572 → 64,590,324 |
| 128 | 64 | 20.248 → 9.514 | 5,720,152 → 866,392 | 620.900 → 197.437 | 218,967,588 → 60,838,124 |
| 1,024 | 16 | 42.676 → 26.445 | 16,497,968 → 6,654,448 | 471.498 → 343.310 | 207,098,452 → 124,450,804 |
| 10,000 | 4 | 191.089 → 166.819 | 107,944,024 → 64,355,480 | 723.491 → 989.195 | 441,307,420 → 358,477,132 |

N8/H1024 contains 1,026 records, 1,025 revisions/requests and 2,383,165 bytes
on both sides. The candidate's Append allocation is roughly flat as H grows;
Open no longer repeatedly copies the growing verified prefix. Native map/slice
growth still has amortized storage costs, and validation still reads all input.

Do not infer that every latency improved. Short histories, N10,000 Open and
unchanged management paths were slower in the primary candidate run. Unchanged
Core Apply/Snapshot costs also increased, indicating substantial timing variation
between those sequential runs. Allocation measurements identify the eliminated
work more directly. Interleaved controls are recorded separately below; primary
slower observations remain in the dataset.

Five interleaved pairs reuse those exact archived sources, harness and
N8/H1024 bytes, alternating which side runs first. Results are supplementary,
not substituted into the primary table. Both sides still perform actual Sync.

| Control | ms median before → after | before range | after range |
| --- | ---: | ---: | ---: |
| N8/H1024 Append | 52.085 → 8.291 | 37.402–59.884 | 5.956–40.510 |
| N8/H1024 Open | 17,504.365 → 184.099 | 16,947.776–17,679.483 | 174.370–200.769 |
| N10,000/E256 ApplyRebind | 102.313 → 107.582 | 93.924–114.127 | 93.283–119.532 |
| N10,000/E256 Preview | 207.878 → 240.558 | 193.953–279.062 | 216.032–268.829 |
| N10,000/E256 CommitFresh | 928.728 → 705.689 | 924.542–1,194.099 | 683.479–741.160 |

Long-history allocation remains about 3.892 GB → 64.589 MB for Open; native
Apply stays about 10.720 MB per operation. Timing ranges of the unchanged
controls overlap. Durable commit improves in this repeat but was slower in the
primary run, reinforcing the limited latency claim. The frozen input digest
was rechecked; review also added digest preflight to the repeat tool before
future measurements. The original run's copied runner is retained as executed.

## Whole management operations

N10,000/E256, fresh-prefix operations, ten samples; external Preview is excluded
from CommitFresh timing, while Controller's internal revalidation remains.

| Operation | ms before → after | MiB/op before → after |
| --- | ---: | ---: |
| Preview | 176.464 → 329.551 | 139.26 → 139.29 |
| Native ApplyRebind | 84.466 → 140.852 | 10.22 → 10.22 |
| Durable CommitFresh | 785.709 → 960.286 | 406.32 → 332.82 |

The commit allocation reduction is real but partial. These measurements do not
support a universal whole-commit latency improvement. Core graph copying,
planning, normalization, new-record encoding and verification remain expensive.
Public Inspect/History/Journal copies are measured separately and retained.

## Profiles

Baseline N128/H64 Open CPU profile attributes 76.19% of cumulative samples to
`journalState.clone`; that function no longer exists in the candidate. The
remaining candidate costs include strict JSON decoding, normalization/hashing,
allocation and original plan validation. No safety checks were disabled.

The sampled allocation profile's `appendLocked` stack drops from 79.10 MB to
5.12 MB, with 68.86 MB previously attributable to clone. Profiles run twelve
instrumented copy/Open/Append/Close batches, so their whole-process totals
include setup/reopen work. The saved append-focused view isolates that stack;
its printed percentages use the **whole profile** denominator, not a new
append-only denominator. Profile elapsed time is not a benchmark result.

## Cumulative, retained and peak memory

Independent process probes; same N8/H1024 bytes. Each probe retains the complete
Store for the After sample, then closes it, drops it, forces GC and records
Released. The input byte slice is deliberately kept alive throughout.

| Open probe | Before | After |
| --- | ---: | ---: |
| Operation TotalAlloc delta (MiB) | 4,463.48 | 61.78 |
| GC live HeapAlloc before operation (MiB) | 2.35 | 2.35 |
| GC live HeapAlloc with Store (MiB) | 7.44 | 5.25 |
| GC live HeapAlloc after release (MiB) | 4.43 | 4.42 |
| Linux VmHWM at After (MiB) | 31.81 | 13.75 |

Append probes separately report 11.36 → 0.073 MiB cumulative allocation, with
all 1,026 old records still represented and one genuine new selection. The
GOMAXPROCS=4 process probes are not interchangeable with `-cpu=1` B/op. TotalAlloc
also includes a small fixed `/proc` probe overhead. RSS/peak are OS process
samples, not per-operation Go live-object counts; VmHWM covers the process so
far, including fixture preparation. Released sampling follows count queries and
heap-profile writing, which themselves allocate. RSS need not fall after GC.

Retained memory is not uniformly lower: N10,000/H4's GC HeapAlloc with Store is
21.33 → 23.01 MiB, while its cumulative allocation drops. All historical
manifests, receipts and audit records remain retained; their content/representation
and native container capacities can dominate the live heap. No diagnostic or
business evidence was removed to achieve these results.

## Limits and reproduction

Historical selection/rollback still searches history, so repeated recovery can
retain higher-order lookup work even after clone removal. No total-replay
linearity, constant-time commits, external effect rollback or exactly-once
guarantee is claimed. This bounded matrix does not reach journal/frame/record
limits, simulate power loss or provide production P99 estimates.

```powershell
python scripts/measure-journal.py --topology-ref ea90a93 --harness-ref ad828d9 --label baseline --matrix study --count 10 --profiles --management
python scripts/measure-journal.py --topology-ref 6cff55a --harness-ref ad828d9 --label candidate --matrix study --count 10 --profiles --management --inputs <baseline-input-directory>
python scripts/summarize-journal.py --before <baseline-run> --after <candidate-run> --inputs <baseline-input-directory> --out <new-output-directory>
python scripts/audit-journal-replay.py --before <baseline-run> --after <candidate-run> --inputs <baseline-input-directory> --out <new-audit-directory>
```

Use the same Linux/container/toolchain/resource configuration for paired timing.
Keep correctness/race/service runs outside timed runs. The old v0.1.0/v0.2.0
topology tags are unchanged; these measurements describe the new source, not a
republished old release.
