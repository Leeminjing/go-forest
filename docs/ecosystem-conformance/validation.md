# Ecosystem TestKit acceptance

Baseline: `c2f7ac9`. This is a third execution model: native JetStream deliveries,
without Temporal. The example uses published TestKit `v0.1.0`, without a local
replacement. All new Go fixtures are test files; existing production Go files,
Core, stable Adapters, TestKit implementation, Agent Runtime and Focus-Go remain
unchanged. Spec and Design stay local and ignored.

| Gate | Actual test / observation |
| --- | --- |
| A01 native full loop | `TestConformanceNativeMessageAndAgentKeepDistinctEvidence`: real Gin submission/outbox, published NATS publisher, actual Consumer callback, SQL receipt/inbox, Ack and original Agent model/tool result |
| A02 Replace/Rebind | `TestConformanceMessagesPinReplaceAndRebindWithSameVersionCounterexample`: barrier between real dependency uses, old/new receipt versions, actual Generation, stable Consumer/database/Store/Agent/checkpoint instances, old native file lifetime |
| A03 bad evidence | Same-generation trace swap, absent required stage and empty trace fail CheckPinned; independently observed calls are never relabeled |
| A04 independent scopes | `TestConformanceEntryAndMessageAreIndependentScopesAcrossRouteChange`: HTTP and RPC subcases, old entry/new message, shared trace ID but distinct invocation identity; mixed journal rejected |
| A05 admission/rollback | `TestConformanceCandidateNeverFetchesAndFailedResourcesReallyClose`: broker delivery/Ack/pending state across the complete blocked candidate and cleanup, old HTTP route, unchanged topology fingerprint, physical cleanup; positive control consumes the original queued message only after explicit Enable |
| A06 actual drain | `TestConformanceQuiesceNotifiesAdmissionWithoutCancelingAcceptedWork`: native inflight/quiescing, timeout, held resources and unchanged accepted context, actual Quiesce join, Ack, then owner Close; CheckDrain observes the original message execution |
| A07 resource ownership | Actual pgx physical connection Close, native NATS ClosedHandler, real handler file; borrowed NATS connection remains usable after Forest Close; bounded child cleanup and race-log inspection |
| A08 Kill+Wait/redelivery | `TestConformanceKilledProcessRedeliveryKeepsOriginalSQLEffect`: independent SQL/broker reads, Kill and OS Wait, same broker incarnation/sequence with increased Delivery, CheckIdentity preserves original SQL provenance, original inbox/effect count one, Agent completion, B graceful resource close |
| A09 duplicates/conflict | `TestConformanceNewTransportDoesNotChangeBusinessEffect`: different Msg-Id/sequence means different logical delivery; same business input reuses SQL receipt, different input returns native ErrConflict and stays unacknowledged; corrupted origin/attempt/task evidence rejected |
| A10 native regression | Existing full ecosystem suite remains, including three original process crash windows, service restarts, Agent approval/Unknown, entry routing and tracing; Windows/Linux, Linux race3 and desktop/mobile Chromium |
| A11 fixed versions | Module/test dependency graphs contain no Temporal; external verifier downloads pinned versions outside checkout, verifies sums and no Replace, builds and runs versioned CLI plus real services/Agent/Chromium |
| A12 review/scope | Independent Spec and Standards review, unchanged protected paths, Leeminjing-only new Git identities; see review and release records |

## Reproduction

With Go 1.26, Docker, Python and the existing Playwright requirements installed:

```sh
python scripts/test-ecosystem.py --race --count 3 --browser --published
```

Linux is required for this command's race flag. On Windows omit `--race`.
The harness owns isolated PostgreSQL 18 and NATS 2.12.4 containers, supports the
existing native restart tests and removes only those containers in `finally`.
The source suite and versioned CLI verification are separate gates: building
the CLI alone does not exercise the new test-only proof trees.

`--serve --directory .scratch/<fresh-directory>` keeps owned services available
for targeted development runs until that directory's `stop` marker appears.
Its environment file contains only the explicit test service configuration.
No default skipped integration test is counted as a native acceptance pass.

The ecosystem package timeout is 600 seconds for the complete three-round
suite, including real AckWait/redelivery and the original process scenarios.
No sleep establishes hot-replace/drain ordering: TestKit barriers and actual
callback/Quiesce/process completion establish it. Native eventual Ack and file
reports are polled with deadlines. Windows report reads retry only native
sharing/lock violations, without accepting malformed evidence.

Execution results, review disposition and the immutable release source are
recorded in `review.md` and `release.md` after those gates complete.
