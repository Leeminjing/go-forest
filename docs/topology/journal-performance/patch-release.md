# Topology Journal patch v0.2.1 release acceptance

Tag `extensions/topology/v0.2.1` is annotated, authored/tagged by Leeminjing, and points to `205924b5968fcf59dc391bf103de113e1cd682ba`, the already-accepted Journal runtime. The root latest release remains v0.3.1; previous tags are unchanged. No normalization/planning optimization is included.

Release: https://github.com/Leeminjing/go-forest/releases/tag/extensions/topology/v0.2.1

## Actual external consumer

`tests/topology-patch-consumer` pins Core v0.3.1 and Topology v0.2.1. `verify-topology-patch-release.py` copies it to a new directory outside the repository with a new empty module cache, GOWORK=off, GOTOOLCHAIN=local, empty GOFLAGS, public GOPROXY/GOSUMDB and no replace. Actual downloaded production bytes must match the tag; modules and compiled package directories must belong to that fresh cache. Version sums, source hashes, available origin identity, commands, vet and execution evidence are saved.

- Exact released-v0.1 bytes, original Boot and four request/receipt pairs, including failed request classification and conflict on modified old payload. No constructors or history append during retries.
- Real public Preview/Replace/Rebind/Rollback/Invoke, retaining unchanged native instances. Results [1,2,1,2,2], generations [1,2,3,4].
- Close then reopen, create a fresh Forest, explicitly Restore and admit; generation starts at 1, session changes and stable constructor runs again. This consumer's cold recovery is within one process; it does not replace the genuine Kill+Wait tests already accepted on the exact tagged source.

## Publication and public-index delay

The first source preflight had a malformed PowerShell replacement argument and accidentally requested the not-yet-published version, creating an avoidable negative-index wait. Structured subprocess arguments corrected the preflight and it passed before tagging. The first post-publication fresh-cache attempts then found the correct tag through Git, but public Go proxy/checksum services still cached that earlier unknown-version response. Both failed downloads were preserved and are not counted as successful consumer runs. Checksum verification was never disabled; original tag bytes were not moved. Later bounded availability waits only postpone starting the actual test; no failed functional test is automatically retried. All final consumers still perform ordinary Go download and checksum/source validation from new caches.

## Outcome

Windows Go1.23.12 (count1) and Linux Go1.23.12 (race,count3) both passed from separate new external projects and empty caches. Their source digests and consumer go.sum match.

Module Sum: `h1:wdqBbRLkGlvE/wQiJhk5EWjXk3/GXjB/JFnFUaaVR8o=`. GoModSum: `h1:cWqjCMIOeqpYO9byGaVg0n1V1S0F4hSNsQRIxkPDljU=`.

Raw logs, module/download/build provenance, source hashes, original failed attempts, index waiting records and preserved-byte manifest are in [patch-release-data](patch-release-data/). The full source/publication workflow on `abe025b` [40812513069](https://github.com/Leeminjing/go-forest/actions/runs/40812513069) completed successfully on attempt 2: all 70 latest checks are successful. The failed first attempts remain recorded.

The next planning-performance Spec and Design are local ignored documents. No baseline capture or production implementation for that next phase was started, and no speedup is claimed for it. Agent Runtime, Focus-Go and stable Core/Adapter implementations remain unchanged.

## Retained CI incident

The first new 70-job source CI encountered an existing candidate-ecosystem drain-test timeout at its unchanged 120-second bound, in addition to the known public-index download failure. The exact failed log was retained. A focused reproduction on independent PostgreSQL18/NATS2.12.4, Go1.26.10, unchanged candidate Core and the same race test passed 20 repetitions. That does not establish a root cause or a fix; no Core/Adapter/test timeout/assertion was changed. The unchanged full failed-job replay subsequently passed; no root cause or code fix is claimed. The original failure and focused-loop output remain available in patch-release-data.
