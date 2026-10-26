# Native Fx + dynamic Forest

Four ordinary trees: an ownership root, a nested Fx Module, an independent
business provider and an application consumer. Fx keeps its own DI graph. The
business package imports neither Fx nor the consumer's concrete interface types.

The consumer owns its Service/Source contracts; ordinary Links connect it to
the Fx export and connect Fx's Ref-based adapter to the business implementation.
Nest establishes resource ownership, without turning Fx constructors into trees.

```sh
go run github.com/Leeminjing/go-forest/examples/fx/cmd/demo@v0.1.0
```

The deterministic demo uses a real Fx App and loopback HTTP server. It invokes
v1, replaces the business tree, then invokes v2 through a managed background
unit and a real HTTP request:

```json
{"Before":"v1:oak/v1:oak","Background":"v2:elm/v2:elm","HTTP":{"text":"v2:ash/v2:ash","generation":2}}
```

The integration test blocks an old call between two reads and demonstrates
`v1:hold/v1:hold` while new calls use v2. HTTP and background calls each pin their
own generation. Shutdown quiesces producers before closing the Forest. No model
key, database, Temporal server, signal takeover or external service is needed.

The adapter does not hot-swap the entire Fx App or rewrite its DI graph. See
[adapter lifecycle and ownership rules](../../adapters/fx).
