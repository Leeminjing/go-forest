# Temporal trees

Independent Go 1.26 module. Pins Temporal SDK v1.50.0 and Forest v0.3.0;
no GORM, Agent or product dependency is added to this adapter or Core.

```sh
go get github.com/Leeminjing/go-forest/adapters/temporal@v0.1.0
```

`ClientDefinition` publishes `client` (`ClientAccess.Use`), borrowing the native
client for a controlled callback. Retain namespace/WorkflowID/RunID and detached
values; consume WorkflowRun.Get, query values and history iterators inside the
callback. Native clients, run handles and iterators must not escape. Requests
may have reached the service even if their response was lost: reconcile stable
IDs. Native SDK errors/options remain available. A long Get pins its local
Scope like any other long callback; it is not the Workflow's durable lifetime.

`WorkerDefinition` publishes diagnostic `control` (`Control.Status`). Supply the
actual TreeID, task queue, native options, consumer-defined Ports and
BuildRegistration. BuildRegistration creates typed Refs while BuildEnv is open;
the returned Register callback receives native WorkflowRegistry/ActivityRegistry,
not Start/Stop. Keep registries/functions/configuration immutable after preparation.

## Admission is explicit

Build/Start/Ready connect and register without native Worker.Start. They never
create business workflows, provision namespaces or consume queued tasks.

```go
generation, err := f.Apply(ctx, plan)
// Check err, then use a fresh top-level Context:
err = temporalforest.Enable(ctx, f, "jobs/worker")
```

Enable/Quiesce/Inspect are host management entry points, not tree-to-tree service
locators. They reject nested/Activity scopes and resolve the current physical
instance. A successful Apply leaves the worker disabled; Enable failure cannot
roll back that published topology. Starting can already execute activities before
native Start returns. Native Start has no Context interruption: its call may keep
waiting while status remains visible. Repeated Enable is idempotent only while
running; stopped/failed native workers are never restarted.

## Scope and real resource completion

The outer Activity interceptor admits under a mutex, enters a fresh Forest
Scope, verifies its own worker is still present, then executes the native chain.
Activity functions use their declared Ref dependencies and propagate this ctx.
One attempt pins one version; retries and later activities may see newer versions.
Workflow code/replay never accesses Forest, ordinary network clients, GORM or
Agent. Original activity results are replayed from Temporal history.

Synchronous activities must finish resource use before return. Wrap every late
resource callback in Guard; launch owned producers using Go before they can
outlive the method. Admission is sealed and admitted work joined inside Scope.
Do not launch opaque/untracked goroutines, retain raw providers, or put clients,
Refs/Scopes into activity results/history. Converters, logging, propagation and
Workflow hooks cannot use Forest business dependencies outside this boundary.
Native ApplicationError/CancelError types are preserved without join wrappers.

Quiesce closes business admission and requests native Stop once. SDK Stop can
return after WorkerStopTimeout while user code still runs. Status distinguishes
NativeStopped from Inflight and final stopped. Caller deadlines end only the
wait; the same cleanup operation continues. Core Stop waits for actual cleanup
and closes the owned client once. Noncooperative work can remain draining forever.

Shutdown: serialize management/topology changes; Quiesce successfully, return
from its call, Remove, then Forest.Wait/Close. Do not synchronously Quiesce from
an Activity. Direct Remove/Close rejects new business admission but does not
force-kill old code or automatically execute the external stop protocol.

Each Client and Worker owns a separate SDK connection. No arbitrary borrowed
client lifetime is assumed. Worker/Client definitions are not HotSwappable or
Reparentable. Keep these stable and replace compatible Runtime business providers.
Changing workflow code, registrations or queues requires native Temporal
compatibility/deployment rules and explicit worker drain/rebuild.

## Supported surface and limits

Initial managed surface is ordinary Workflows and synchronous remote Activities.
BackgroundActivityContext, OnFatalError and the outer Activity interceptor are
reserved. Eager activities are disabled. Client-injected WorkerInterceptors,
plugins, sessions, LocalActivityWorkerOnly and local/standalone Activity entry
are rejected. Register has no Nexus method. Worker interceptors inside the
guard are supported under the resource contract. Unknown opaque engines are not
automatically safe. ErrResultPending with escaped work is unsupported: detected
ownership violation closes admission and retains the attempt/resources rather
than falsely proving release. Terminate the process to end such unowned work.

Status contains bounded identity/attempt summaries and aggregate native errors.
Default SDK logging omits bodies/tokens. A custom native Logger owns its own
redaction policy. SDK network and application errors still return to callers.
Generation is local; restarted processes have different worker identities.

Activities can retry or overlap after timeout/process failure. There is no
adapter-level callback retry, global attempt lock or exactly-once promise.
Use business idempotency keys, durable input fingerprints/receipts and explicit
reconciliation for uncertain external commits. Temporal history is not an Agent
checkpoint or a Forest transaction.

## Verification

Fast checks: `go vet ./...`, `go test ./...` (service tests explicitly skip).
Full acceptance from repository root: `python scripts/test-temporal.py`.
Linux concurrency: `python scripts/test-temporal.py --race --count 3`.
Explicit integration fails if real service configuration is missing. Native
tests connect to actual Temporal; the example additionally uses real PostgreSQL,
Worker process Kill+Wait and a persistent Temporal Server restart. No SDK mock
environment substitutes for those gates.
