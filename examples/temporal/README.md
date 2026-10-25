# Durable Temporal workflow with Forest business trees

Go 1.26+. Native Temporal SDK v1.50.0, Forest v0.3.0, GORM adapter v0.1.0 and
Forest Agent v0.2.0. Deterministic model, no LLM key. The adapter itself has no
Agent/database dependency; this independent example supplies the business seam.

Nine trees: jobs with a worker child, client, database, Agent, model, checkpoint,
events and approval. Worker-to-parent assembly is explicit BindNest; jobs and
Agent declare their own database/executor/model/checkpoint/event/approval Ports.
The native Workflow only imports Temporal orchestration and pure data contracts.

Flow: native Workflow → planning Activity → Forest Agent Read/Create/bounded
Advance → business Activity → GORM/PostgreSQL transaction and receipt → optional
Signal wait → read Activity → workflow result. The optional wait owns no Worker
business Scope; a host actively awaiting native Get still pins its own Scope.
Replacing the jobs provider preserves earlier receipts, while later activities
can observe the new compatible business implementation.

Setup is an explicit admin action, never part of Forest Apply. Run against an
isolated real Temporal Server and PostgreSQL:

```sh
# Set FOREST_POSTGRES_DSN; provision an isolated Temporal service first.
go run ./cmd/demo -setup -address 127.0.0.1:7233 -queue my-isolated-queue \
  -id stable-operation-1 -text oak -directory /path/to/agent-checkpoints
```

Retain operation/Workflow IDs after errors. Workflow submission errors do not
prove the service never accepted them. The business key and input fingerprint
do not contain Attempt or Generation. A unique row stores the mutation and its
full receipt in the same transaction. Duplicate attempts return that receipt;
changed input is rejected before running another Agent. Commit uncertainty
requires reconciliation rather than replaying an arbitrary external effect.
This proves the sample's database protocol, not general exactly-once execution.

Agent uses its unchanged native file checkpoint format and stable run ID.
Read precedes Create; only known ready states Advance. Running/unknown/approval
states return a native continuation reference instead of reissuing tools.
Activity already owns a Scope, so the bridge never calls nested Continue.

`python ../../scripts/test-temporal.py` owns isolated real services and runs all
acceptance plus the CLI. On Linux add `--race --count 3`. CLI v1.9.1 is pinned and
checksum verified; its dev Server is 1.32.0 and uses a persistent SQLite file.
The development server is a real execution engine, not a production deployment
or high-availability claim. Tests kill a real Worker after database commit but
before response, restart another worker on the same queue/checkpoint directory,
then verify one committed change and original workflow history. A separate test
kills/restarts the actual service on the same persistent file and replays history.

Forest Core, Agent state machine and Focus product code remain unchanged.
