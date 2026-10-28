# Core performance workloads

This independent Go 1.23 module measures real Forest calls without network or
business I/O. It is a measurement harness, not a production extension.

Run from the repository root:

```sh
python scripts/measure-core.py --core-ref <commit> --harness-ref <frozen-harness-commit> --label before --matrix study --count 10 --benchtime 1s --profiles --extras
python scripts/measure-core.py --core-ref <candidate> --harness-ref <same-frozen-harness-commit> --label after --matrix study --count 10 --benchtime 1s --profiles --extras
python scripts/summarize-core.py --before <before-directory> --after <after-directory> --out <new-output-directory>
```

Each run archives both sources, verifies module selection, records the Go binary,
source hashes, command dimensions and environment, and writes an exclusive
`.scratch/core-performance/<run-id>/` directory. A matrix may be split with
`--only`/`--skip`; match the harness revision for each paired case. Failed files
remain evidence but the summarizer excludes individual benchmarks without PASS.
Large topology Controller cases respect the existing 10,000-tree cap. Large
fixtures can take longer than the measured work; setup is outside call timing.

`RefPinned` holds one Scope around a benchmark loop. `RefEnter` includes scope and
consumer-capability entry. `RefParallel` reports aggregate wall-time throughput,
not request latency. `ControllerEnter` measures its admission path separately.
Many degree, optional-unavailable and declared port count are explicit dimensions.

Management uses fixed iteration counts for real Replace/durable commits and
separates external Preview. Profile runs are separate from uninstrumented timing;
CPU samples exclude fixture construction, heap/alloc profiles include the live
fixture. Load mode reports planned-arrival-to-completion latency including queue,
execution latency, failures/drops and unfinished requests. These synthetic Go API
latencies are not HTTP service SLOs.

Longevity opens real files and allocates payloads inside factories, retains a
bounded set of old calls, then joins and verifies actual close. Diagnostic
records remain. Resource handles, diagnostics, GC heap and cumulative allocation
are reported separately. An application's retained closed Context can keep graph
metadata reachable; probes intentionally keep only scalar identities and channels.

`go test ./...` checks fixture/workload correctness. Run race tests separately.
Performance conclusions use repeated raw observations from matched environments;
shared CI smoke has no fixed nanosecond pass threshold. See repository
`docs/core-performance/` for the measured results and limitations.
