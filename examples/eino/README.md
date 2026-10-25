# Two native Agent engines in one Forest

Go 1.23+. Uses released go-forest-agent v0.2.0, Forest v0.3.0 and Eino v0.9.22.
No keys, external model API, web server, database or broker needed.

Install the fixed release from any directory:

```sh
go install github.com/Leeminjing/go-forest/examples/eino/cmd/demo@v0.1.0
demo -checkpoints /path/to/checkpoints
```

From this module's source directory:

```sh
go run ./cmd/demo -checkpoints .scratch/checkpoints
go test ./...
```

The actual Eino ChatModelAgent requests a native tool; the tool resolves a
consumer-owned Forest Lookup Ref. The actual Forest Agent requests its own
native Tool contract, whose implementation consumes its own Lookup interface.
Catalog provider imports neither consumer implementation. Both read the shared
catalog twice, through declared dependencies. A separate native Eino Graph has
lookup and formatting nodes with a branch, invisible as internal Forest nodes.

The application owns a finite-text Answer interface. Two bridge trees satisfy
it structurally. Rebind switches only the application's target: existing work
retains its initial Scope/engine, new work uses the new engine. Native Eino and
Forest Agent states are not translated or replaced by this narrow interface.
Native approval/interrupt/unknown outcomes return ContinuationRequired with the
correct engine and run reference, not a fake text answer.

The finite Forest Agent bridge explicitly uses Create/Advance under the caller's
single bounded Scope. It never nests Continue. Native long-run drivers retain
their own entry rules: Continue opens a Scope per Advance, and Eino Run/Resume
each opens a fresh protected execution. Do not wrap those drivers in one shared
long-lived Scope and claim unchanged version semantics.

Catalog owns two children (native Eino and Graph), exports them explicitly, and
opens Lookup to descendants through BindNest. Independent bridges and Forest
Agent dependencies use Link. Native Eino graph nodes are never mapped to trees.
The CLI outputs both results, generation=2 and trees=12 after engine rebind.

Tests run the real engines concurrently, native Graph Invoke/Stream, a blocked
old Eino/tool call across rebind plus catalog/Eino replacement, and native
approval handoff. Native checkpoint state uses separate directories. The Eino
adapter's own suite covers cancellation/Join and process-kill/resume.
