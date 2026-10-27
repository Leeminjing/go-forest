# Multi-framework forest

Go 1.26+. Real PostgreSQL/GORM and NATS JetStream, existing Gin/gRPC/Observatory
and released go-forest-agent. Core and Focus repositories are unchanged.

Set `FOREST_POSTGRES_DSN`, `FOREST_NATS_URL`, `FOREST_TOKEN` (16+ characters)
locally; never commit credentials. Configure DB dial timeouts in the DSN.

```sh
go run ./cmd/demo init -namespace forest_demo
go run ./cmd/demo serve -namespace forest_demo -checkpoints .scratch/checkpoints
```

Init explicitly provisions tables/stream/durable. New/Start never do migrations
or broker provisioning. Serve prints loopback HTTP/RPC/Observatory endpoints
without credentials. HTTP POST `/tasks` takes `{"id":"one","input":"compute 2 + 3"}`;
GET `/tasks/one` reads state/result. Supply `Authorization: Bearer <token>`.
Same ID+input is idempotent; a different input conflicts.
Task IDs use 1–128 ASCII letters/digits/dot/underscore/hyphen, starting with a
letter or digit, so their URL and message-header identity remain unchanged.
RPC UnaryCall accepts the same JSON in grpc-go's generated interoperability payload with authorization
metadata (a demo wire schema, no protoc needed).

Tasks+outbox commit together; relay publishes stable event IDs. Consumer writes
inbox+pending work before Ack. Separate driver creates/reads/recovers the stable
Agent Run and persists results. Agent uses its own filesystem checkpoint format.
Offline model calls the calculator Tool then returns `model v1: 5`; no key or
arbitrary shell. Pauses/unknown outcomes remain visible, never blindly replayed.

With `RequireApproval`, the calculator pauses. Authorized hosts use the existing
Agent Executor's revision-checked Decide/Resolve API; the bounded rotating driver
rechecks paused checkpoints and settles once they progress. OutcomeUnknown is
never advanced or replayed without explicit resolution. No approval UI is added.

Exactly **one driver per namespace/checkpoint directory**. Stop prior processes
before recovery. No distributed scheduler, lease, cross-resource transaction or
exactly-once tool claim. Outbox/inbox belong to this application, not adapters.
Close stops the host driver, quiesces/drains consumption, then closes Forest.
Business replacement retains resource/entry trees. Root explicitly exports
public children for administration/Observatory. Agent and required Checkpoint
are siblings to avoid a construction/ownership cycle.

Explicit optional telemetry covers HTTP/RPC, publish, receive and Agent-step
boundaries. `Options.Telemetry` accepts host SDK providers. W3C trace metadata
survives outbox+NATS; each message still starts a new local Forest Scope.
Observatory `/api/v1/runtime` shows counters/freshness, not all-Link traffic.

```sh
python ../../scripts/test-ecosystem.py
python ../../scripts/test-ecosystem.py --race --count 3 # Linux + C compiler
python tests/browser_test.py # existing real services + Playwright/Chromium
```

Runner creates isolated containers and removes only its own. Tests cover native
transactions/cancel, ownership, precommit safety, message version switch, drain,
server reconnect and real application-process kills at three durability barriers.
Explicit integration fails on missing services. Offline tests skip service tests
and are not full acceptance.

The test-only conformance fixtures use published TestKit `v0.1.0` for all four
checks, without Temporal. They add actual same-message redelivery after
SQL-commit/pre-Ack Kill+Wait, independent message/business identities and native
resource probes. Existing production protocols and acceptance tests remain.
See [acceptance](../../docs/ecosystem-conformance/validation.md) and
[execution-model comparison](../../docs/ecosystem-conformance/comparison.md).
For the complete source/race/browser and versioned external CLI gate:

```sh
python ../../scripts/test-ecosystem.py --race --count 3 --browser --published
```
