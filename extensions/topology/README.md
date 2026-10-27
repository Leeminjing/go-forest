# Topology recovery

An optional Go 1.23 module for saving desired Forest structures, reconstructing
them in a fresh Forest, and returning to historical structures through new native
commits. It depends only on the standard library and Forest `v0.3.0`.

```sh
go get github.com/Leeminjing/go-forest/extensions/topology@v0.1.0
go run github.com/Leeminjing/go-forest/extensions/topology/cmd/demo@v0.1.0
```

The demo captures a root and nested provider, restores their `BindNest`, changes
the provider and rolls back, then creates a second Forest/session. It prints:

```json
{"Before":"v1","After":"v2","Rollback":"v1","Cold":"v1","Generations":[1,2,3],"ColdGeneration":1,"NewSession":true}
```

## Host responsibilities

Use a trusted, existing local directory on Windows or Linux. A nonblocking OS
lock protects the entire management session; never delete the lock file to steal
ownership. Shared/network filesystems and untrusted directory mutation are not
supported. The directory owner controls file permissions/Windows ACLs.

Create a `Catalog` of `Registration` values with native Definitions, exact
DefinitionRefs, a stable `ContractKey` and a SHA-256 `ImplementationDigest` for
the host's release/configuration identity. These are host assertions, not a
mechanical proof of code or behavior equivalence. Do not hash a secret into the
manifest; supply credentials out of band. Distinct closed-over configurations
must use distinct registration refs. Definitions must be available in the new
process; this module never downloads code or deserializes factories.

`Capture` consumes a detached native Snapshot and the matching Catalog. `Encode`,
`Decode` and `Digest` produce a bounded, strict, deterministic Manifest. Only
logical trees, parent relationships, exact definition identities and binding
identities/endpoints/kinds/order are stored. Sparse native binding Order values
become consecutive ordinals, preserving their relative order.

Give `NewController` a **fresh** Forest, opened Store and Catalog. The controller
registers the Catalog. All structural writes must go through the controller;
direct native Apply/Register/Close and unmanaged ingress violate managed-mode
ownership. Detected topology divergence stops further managed writes.

The host still owns Adapter-specific Enable/Quiesce/Join. This module's admission
gate covers only `Controller.Invoke`; it cannot stop a native HTTP listener or
message producer that bypasses that gate. Keep such ingress disabled/quiesced
while managing the graph, and enable it only after the durable observation and
your own resource checks. Existing admitted calls retain their native Scope.

## Workflow

1. `Open` the Store and read `Inspect().Position` (StoreID, sequence, digest).
2. `Seed` an initially empty Store with a captured Manifest, unique Request ID,
   expected Position and explicit host approval note. This selects a desired
   structure; it does not run it.
3. `NewController`, then `Restore` with a **new attempt ID** and approval to
   recreate resources. Restore writes its intent before native Apply and records
   its actual outcome afterwards.
4. `OpenAdmission`, then `Invoke` and the native `forest.Use` entry point. Always
   pass the native callback Context onward.
5. `Preview(target)` / `Reconfigure`, or `PreviewRollback(revision)` / `Rollback`.
   Submit the unchanged Proposal and matching Request.Expected. A Proposal is a
   structural plan, not a side-effect-free proof that resources can be prepared.
6. Re-open admission explicitly after a successful operation. `Close` joins actual
   native cleanup before releasing storage ownership; retry Close after timeout.

Online switching is **full rebuild** in one ChangePlan: remove the previous roots
in the candidate, add every target tree, then insert bindings in target order.
Every target instance is rebuilt. Core still enforces hot-replacement promises,
contracts and resource lifetime. No intermediate empty graph is published.
Identical structures are explicit no-ops, preserving instances and both versions.

For an unsafe hot rollback, explicitly shut down and join the old controller and
all host-managed ingress. Reopen the Store, `SelectHistorical` with host approval,
then restore with a new Forest/session. Selection is recorded as `host-selected`,
not as a past runtime success. This cold path has downtime and can still fail.

See [cmd/demo](cmd/demo/main.go) for a complete native example.

## Crashes and uncertain results

Journal sequence, desired topology Revision and native Generation are separate.
A cold nonempty restore normally commits `0 → 1`; it does not restore the old
Generation counter. Physical identity is `(SessionID, InstanceID)` because native
InstanceID strings can repeat in different Forests. Go objects, Scopes, Agent
checkpoints, Workflow histories and business effects are not restored.

Intent and native Apply cannot form one disk/memory transaction. An unfinished
intent blocks new controllers. After actually terminating/joining the old
execution and checking external resources, the host calls `Resolve` with
`previous` or `target` and a reconciliation note. This chooses a desired structure;
it never fabricates the missing Apply success. A fresh Restore requires a new ID.

The same Request ID and semantic payload return its recorded receipt without
another Apply; a different payload conflicts. Old-session receipts are historical
and cannot make a new empty Forest ready. Unfinished/reconciled attempts never
blindly retry. `Inspect`, `History`, `Journal` and controller `Status` provide
detached observations, including failed attempts and host reconciliation notes.
Failures support `errors.Is`/`errors.As` and preserve native classifications.

Writes use bounded, length-framed, hash-chained records and File.Sync. An I/O
error poisons the current Store and keeps admission closed; close/join the session,
reopen and scan. Any bad/truncated record fails closed with an incomplete verified
prefix in `*Error.Prefix`. No automatic truncation, fallback to an old prefix or
in-place repair occurs. Preserve the damaged original, then explicitly Seed a new
Store if the host chooses a verified structure. An external `ExpectedAnchor` can
detect an outdated backup; without one, a valid whole-prefix rollback is not
detectable. Hashes are integrity checks, not authentication.

The tested durability boundary is actual process termination and acknowledged OS
file synchronization. Linux also syncs the directory at journal creation.
Windows file flushing is not advertised as a portable directory/device-cache
power-loss guarantee. Kill tests are not power-failure certification. Topology
rollback never undoes a database write, published message or file edit and makes
no exactly-once promise.

## Limits and tests

Version 1: 10,000 trees, 50,000 bindings, 4 MiB Manifest, 16 MiB record, 256 MiB /
10,000-record journal, 512-byte identifiers and 4,096-byte approval notes. Reserve
space for a terminal record before intent publication. No compression, automatic
history deletion, distributed leasing or transparent adoption of existing graphs.

```sh
go vet ./...
go test ./... -timeout 90s
go test -race ./... -count=3 -timeout 120s
```

Tests use real Forests, file handles, lifecycle barriers and supervised subprocess
Kill+Wait, including interrupted record writes, preparation, publication and
terminal observation. Subprocesses inherit the race build. They retain each
framework's lifecycle semantics; no stable Adapter, TestKit or Core source change
is required. Acceptance evidence is recorded in
[validation](../../docs/topology/validation.md).
