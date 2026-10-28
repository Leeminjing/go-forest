# Incremental topology: Gin × Business × PostgreSQL

This small host demonstrates actual HTTP requests over native Gin and GORM
trees. Only the business definition is hot-swappable. HTTP and database adapters
retain their original flags, physical instances and pool during change/rollback.

The **first** Gin middleware calls `Controller.Invoke`, sets the pinned request
Context and encloses the complete finite route chain. Native `ginforest.Scope`
is nested afterwards. Reversing those middlewares would bypass managed admission.
The listener can accept TCP while business admission is closed: callers get 503,
and business/database work does not enter. OpenAdmission is an explicit host step.

From the repository root, before release:

```sh
python scripts/test-topology-incremental.py --race --count 3
```

The runner starts and removes its own PostgreSQL container unless the host sets
`FOREST_POSTGRES_DSN`. Source acceptance explicitly replaces **only** the topology
module using a temporary modfile; this is not published-version acceptance.
Gin/GORM/Core are fixed released dependencies. Once topology v0.2.0 is published,
`--published` requires the fixed module without replacement. The module's Go 1.25
requirement comes from Gin/GORM; topology itself continues to support Go 1.23.

With a real database configured, `go run ./cmd/demo` runs a bounded demonstration
and leaves its journal in an explicitly reported local temporary directory.
It writes test receipts into `forest_topology_receipts`; use a dedicated database.

Evidence includes request versions and native Generations, Tree/InstanceIDs,
actual endpoint/engine/pool identity probes and native lifecycle counts. The old
request blocks after its first SQL read; business v2 is prepared while direct
HTTP is rejected; new requests then use v2. The old request completes with v1.
Historical rollback creates a fresh v1 business instance while preserving Gin
and the pool. A v2 database receipt remains v2 after rollback. Finally actual
request exit, lifecycle completion and closed-pool Ping are verified.

Neither the topology journal nor rollback restores old Go objects or compensates
database effects. No workflow engine, Agent, message broker or product migration
is involved.
