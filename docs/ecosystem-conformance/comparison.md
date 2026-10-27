# Third execution model: native JetStream

The same published TestKit checks now operate at three independently assembled
boundaries. Each fixture supplies native facts, rather than implementing a new
universal execution interface.

| Common rule | Fx × Temporal × GORM | Temporal × Eino | Ecosystem without Temporal |
| --- | --- | --- | --- |
| Pinned invocation | Activity attempt through Fx service/Ref | Activity through ADK/Graph/Ref | One NATS delivery callback through handler/Store/next Ref |
| Admission/rollback | Explicit post-commit worker admission | Explicit post-commit worker admission | Prepared JetStream consumer never fetches; committed consumer still needs Enable |
| Notice versus join | Native cancellation/drain and actual work exit | Cancel/stream-close versus actual Eino exit | Quiesce stops admission and waits; it does **not** cancel the accepted handler |
| Execution/effect identity | New attempt/process, original SQL receipt | New attempt/process, sealed Eino result | Same broker message redelivery/new attempt/process, original controlled SQL receipt |

HTTP/RPC requests, outbox publication, message delivery and Agent driver steps
are separate Forest scopes. Trace correlation and business task identity do not
make them one Generation. The new tests deliberately submit under an old entry
generation and deliver after a route change, then reject evidence that combines
both invocations into a single CheckPinned trace.

## Facts that stay native

- JetStream incarnation, StreamSequence and delivery count come from server
  metadata and actual callbacks. A fresh Msg-Id repeating a business operation
  has a new logical delivery identity. It is not relabeled as a redelivery to
  satisfy CheckIdentity's same-task premise.
- The test-only GORM receipt/effect transaction is separate from the original
  inbox transaction, NATS DoubleAck and Agent checkpoint. The crash barrier is
  reached only after the two SQL commits; the parent checks both before Kill.
  There is no cross-resource atomicity or exactly-once promise.
- Original effect provenance is first persisted by A, then read from SQL by B.
  Process identity includes OS PID and a startup nonce; physical Tree identity
  is the actual instance ID qualified by the actual Forest/process domain.
  Generation numbers from different Forest domains are not compared.
- Resource probes observe the creator's native resources. The PostgreSQL probe
  requires exactly one original physical connection and counts its actual Close;
  it rejects reconnection masquerading as the same live connection. A borrowed
  NATS publisher connection remains open until the external owner closes it.
- Quiesce timeout, canceled pull loop, handler return, broker Ack and full owner
  shutdown are distinct observations. CheckDrain is recorded only after the
  native join for that message; all evidence retains the same actual Execution.
- Forest Agent approval, OutcomeUnknown, checkpoint recovery and single-driver
  ownership remain governed by their existing tests and protocol. A successor
  starts only after actual OS Kill+Wait, without inventing a distributed lease.

TestKit `v0.1.0` expresses these four rules unchanged. This experiment neither
adds a universal Adapter wrapper nor starts the separate Core random-topology
research phase.
