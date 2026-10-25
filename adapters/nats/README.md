# NATS JetStream Trees

Independent Go 1.26+ module using nats.go v1.55.0; no NATS logic in Core.

`PublisherDefinition` publishes `publisher` (`Publisher`). It accepts native
`*nats.Msg`, requires `Nats-Msg-Id`, and waits for PubAck. Failed publishes wrap
`ErrPublishUnknown` conservatively; reconcile event ID. No automatic retries.

`ConsumerDefinition` publishes `control` (`Control`) and consumes the `handler`
Port. This consumer defines `Handler.Handle(ctx, Message) error`. Return nil only
after durable application success. The adapter controls Ack. Data/headers are
detached; delivery count and stream sequence are available. Bounded diagnostics
contain stage/event identity and counters, never bodies or raw error contents.

Each connection declares `Owned` or `Borrowed`. Only the owner closes a native
connection. The host must keep borrowed connections alive. Never retain a native
resource obtained through Runtime Ref beyond its callback.

Provision stream/durable separately. Start connects/validates without consuming,
creating, updating or deleting broker resources. Required configuration: durable
pull consumer, explicit Ack, finite positive MaxDeliver, AckWait longer than
ProcessTimeout, no BackOff override, MaxAckPending at least local concurrency.
Each worker buffers one message; processing is bounded by Concurrency. Multiple
clients may share a durable, so side effects still need application idempotency.

After Apply, enter a fresh Scope and call Enable(ctx). Each message has a fresh
Scope; idle pulls never pin the graph. Propagate the Handler Context through
Refs. Quiesce closes admission and cancels pulls while accepted work finishes.
Timeout reports unfinished work; Enable rejects while quiescing. Retry Quiesce,
then Remove after success. Serialize management with topology changes. Never
synchronously Quiesce from the Handler itself. Direct Apply(Remove) is not this
stop-admission protocol.

Connections/consumers are not HotSwappable. Replace the Handler behind a stable
consumer: old messages retain v1, new messages see v2. Subscription changes use
explicit quiesce/drain/rebuild; no atomic broker handoff is claimed. Stop cancels
managed handlers and awaits actual return before closing owned resources.
Noncooperative work can remain Draining. Panics fail a delivery without exposing
panic payloads. Failures Nak with delay until MaxDeliver; terminal failures are
diagnostic, with no automatic dead-letter policy.

Integration: `FOREST_INTEGRATION=1`, `FOREST_NATS_URL`. Optional
`FOREST_NATS_RESTART_CONTAINER` authorizes restart of an owned Docker test
container. `python ../../scripts/test-ecosystem.py` supplies isolated resources.
Deduplication windows and Ack handshakes do not provide exactly-once database or
tool side effects.
