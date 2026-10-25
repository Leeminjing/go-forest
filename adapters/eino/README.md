# Native Eino trees

Independent Go module, minimum Go 1.23; pins Eino v0.9.22 and Forest v0.3.0.
No dependency on go-forest-agent, and no Eino dependency is added to Core.

`ADKDefinition` publishes `agent` (`ADK`), preserving native messages,
AgentRunOption, AgentEvent, interrupts and targeted ResumeParams. Run/Query/
Resume are blocking, controlled event-consumption calls: enter Forest normally,
call through a declared Ref, and consume events inside the callback. Native
errors in events are returned, not mistaken for successful completion.
`ReadMessage` preserves native message concatenation. Visitors borrow streams;
they must not retain, Close, Copy or call native GetMessage on them. The adapter
owns their deterministic Close, including callback errors and subsequent events.

`GraphDefinition[I,O]` publishes `graph` (`Graph[I,O]`) with Invoke and controlled
output Stream; native nodes/branches remain inside Eino. Stream callbacks only
Recv and must not Close, Copy or retain the reader. Graph Collect/Transform,
generic streaming Model/Tool Port conversion, and AgenticMessage are outside
this initial supported surface, not missing from Eino itself.

## Execution ownership is a required contract

Builders return a Host with Lifecycle and NewSession; each session returns its
native Runner/Runnable and a **required** Lifetime. Factories clean partial
allocations or return the Host together with the error for rollback cleanup.
Prepare/Start/Ready must not run agents/tools or advance business checkpoints.

The adapter attaches an `Execution` to each call Context. For a managed builder:

- Wrap **all** synchronous resource-using model, tool, node, handler and storage
  calls in `Guard(ctx, callback)` (or provide equivalent full external Lifetime).
- A producer that outlives a method must acquire ownership *before* launch via
  `Go(ctx, producer)`. It must close its native writer, handle receiver close,
  and return only after its resource use actually ends.
- Return `ExecutionFrom(ctx)` as Lifetime for these guarded configurations.
  Join seals admission, rejects late callbacks before they touch resources, and
  awaits admitted work. For external tasks, provide a real Cancel/Join contract;
  both it and the adapter's Execution gate are awaited.

An arbitrary opaque Runner is not automatically safe. Presence of a callback
cannot mechanically prove its semantics: the builder is responsible for covering
every resource path, backed by tests. EOF, StreamReader.Close, native OnEnd and
CancelHandle.Wait alone do not prove user work physically stopped. Cancellation
can therefore leave the protected call joining; Forest Close/Wait may report
Draining timeout while resources remain retained. There is no force-kill of Go
goroutines. A panicking external Lifetime poisons the instance and retains its
identity lease/resources rather than falsely completing Stop.

HotSwappable defaults false; opt in only for tested, safely coexisting instances.
Owned resources close in Host.Lifecycle.Stop; borrowed resources have no close
hook and their host must outlive all users. Stop is not an admission/cancel signal
for active execution. Do not save preparation Context/BuildEnv or raw providers.

## Identity, checkpoints and options

Explicitly share one `Owner` across versions/instances of an execution domain.
Its bounded process-local gate serializes namespace+checkpoint identity until
Join completes. ADK RunOptions always require a valid stable CheckpointID.
OnCancelReady exposes Eino's native cancel function; its handle reports native
cancel outcome separately from resource Join. Root identity/cancel options are
reserved, appended last. Do not designate conflicting options for descendants.

A checkpointed GraphHost must declare Checkpoints=true and use the managed
GraphCall.CheckpointID; read/write checkpoint identity is appended last. A host
without a checkpoint store declares false. Passing checkpoint options directly
in GraphCall.Options violates this contract; do not hide a store in such a host.
Other native graph options remain available. No private option fields are read.

FileStore implements native Get/Set of unchanged opaque bytes. It writes engine,
namespace, explicit schema/profile fingerprint and payload checksum in the same
atomically hard-linked revision. No CAS semantics are invented for native Eino.
Missing returns native not-found; corruption/profile mismatch fails without
fallback or overwriting. Keep Eino and go-forest-agent directories/formats
separate. Use one process/Owner per directory; restart only after the prior
process exited. This is process-restart validation, not a distributed lease or
power-loss durability guarantee. Native tool side effects remain the app's duty.

Each Run/Resume has a new Scope; none survives pause/restart. Resume must keep a
compatible execution profile; interface type equality does not imply compatible
graph state. No checkpoint or approval format conversion is performed.

## Validation

`go vet ./...` and `go test ./...`; Linux `go test -race ./... -count=3`.
Tests use real compiled Eino Graphs, ChatModelAgent/Runner, controlled stream
producers, real native interrupts, and a child process killed before another
process resumes the checkpoint. Models/tools are deterministic, without keys.
Native event buffering remains unbounded internally; adapter budgets do not
constitute a process-level memory sandbox.
