# Fx Tree

An optional, independent module for composing a native Uber Fx application as
one Forest tree. Core has no Fx dependency. Requires Go 1.23; pins Fx v1.24.0.

```sh
go get github.com/Leeminjing/go-forest/adapters/fx@v0.1.0
```

`Definition[T](f, Options)` constructs a new native App for each planted instance.
`BuildModule(env, bridge)` returns an ordinary `fx.Option`: keep native Modules,
Provide, Invoke, In/Out, names, groups, Private and lifecycle annotations. Only
the public `T` export is populated. Private objects require a public facade;
there is no container lookup API. Options.TreeID must match the planted tree;
reuse a module factory with separate tree configurations to plant it twice.

The tree publishes `service` (`Access[T]`) and `control` (`Control.Status`).
Consumers define their own narrow interfaces and Port declarations. Create
Forest Refs in BuildModule and inject **an adapter holding those Refs** into Fx.
Static Fx constructor injection never makes an already injected raw pointer
change when Forest bindings change.

```go
// Inside an existing Forest invocation, using the consumer's own Service type:
err := access.Use(ctx, func(callCtx context.Context, service Service) error {
    return service.Execute(callCtx) // propagate callCtx to every dynamic Ref
})
```

The callback receives the cancellable call Context explicitly. It must not retain
the service or start unregistered work. `Go(callCtx, fn)` registers a child before
spawning; `Guard` registers synchronous callbacks. Both reject late work. Children
are cancelled and actually joined before the enclosing invocation releases its
generation. Callback errors and native errors remain checkable with errors.Is/As.

## Preparation and admission

1. `Forest.Apply` constructs and starts Fx in the candidate topology. Fx New can
   already run constructors and Invoke; they must not execute business work.
2. OnStart prepares resources. `Bridge.Handler` rejects business with HTTP 503;
   `Bridge.Loop` waits for Enable. Preparation can fail without publishing.
3. After successful Apply, the host explicitly calls `Enable(ctx, f, treeID)`.
   It opens admission, without starting Fx twice or changing Generation.
4. A synchronous external entry uses `Bridge.Run` to enter a fresh Scope. Internal
   dependencies use declared Refs, preserving the caller's pinned generation.

Native lifecycle order and rollback remain Fx's. A private public-API Lifecycle
decorator passes cooperative hook deadlines, but native Start/Stop themselves
do not abandon a still-running hook goroutine. An uncooperative OnStart can keep
Apply waiting. Context cancellation cannot force arbitrary Go code to terminate.
The adapter's StartTimeout/StopTimeout govern these cooperative hook Contexts;
the standalone Fx App.Run timeout policy is intentionally not used.

## Resource ownership and shutdown

Register owned cleanup with `ticket, err := bridge.Defer(cleanup)` immediately
after allocation, before any later failure. Normal OnStop calls `ticket.Close`;
the final ledger handles resources never successfully started. A ticket executes
cleanup once, joins its actual return, and retains a failed result. Borrowed
resources are not registered for closing. Correct reverse registration order is
the module's responsibility, not inferred from arbitrary closure dependencies.

OnStart registers transports with `OwnIngress(stop, done)` **before** starting
Serve/Accept. `stop` must stop admission without synchronously waiting on its
own hook or handler; `done` proves the producer actually exited. A cancelled
Context alone does not interrupt a native Accept loop. Long-lived producers use
`Loop`, and open `Run` only for each actual work item, not while idle. OnStart
failure joins already prepared ingress before Fx can roll back its resources.

Normal host shutdown:

```text
Quiesce → seal/cancel producers and requests → join actual work
        → Apply(Rebind/Remove/RemoveSubtree) → Forest.Wait/Close
        → native Fx OnStop → final owned-resource cleanup
```

Quiesce does not destroy service objects that are still in the active topology.
Its caller's deadline ends only the wait; repeat waits use the same drain.
Management helpers require a fresh host Context and reject calls from inside a
Forest invocation or managed loop, preventing self-wait deadlocks.

Direct Remove/Close still retains old scopes. It cannot promise to wake every
framework producer; quiesce first. Cleanup errors leave the instance visibly
draining/failed, rather than reporting released resources. Status separates
admission, real calls, producers, children, hooks and pending cleanup.

## Compatibility boundary

- This is trusted in-process composition, not a sandbox for arbitrary fx.Option.
- Modules must not replace/decorate the reserved Lifecycle or bypass ingress.
- Constructor/Invoke side effects, failed-start partial resources, native tasks
  and shutdown actions need explicit ownership. Only OnStop is insufficient.
- No raw App, lifecycle actuator, already-running borrowed App, global lookup,
  App.Run/OS-signal takeover or automatic Shutdowner-to-Remove translation.
- HTTP is synchronous; Hijacker/Unwrap is not exposed. Detached response work,
  WebSockets and escaped streams are outside this release's managed boundary.
- The whole Fx tree is not HotSwappable/Reparentable. Replace or Rebind ordinary
  business providers; arbitrary Fx Apps cannot be assumed safe to run in parallel.
- Topology atomicity does not roll back arbitrary external side effects.

See [the real composition example](../../examples/fx),
[acceptance evidence](../../docs/fx/validation.md) and
[framework adapter conventions](../../docs/adapter-guide.md).
