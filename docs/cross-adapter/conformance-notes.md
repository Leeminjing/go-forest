# Observed cross-adapter conformance boundaries

These are evidence from Fx × Temporal × GORM, not a universal Framework Adapter
Contract or an implemented TestKit. The existing independent adapters are kept
at their published versions. Each candidate below cites the acceptance cases
that actually exercised it.

| Observed invariant | Evidence | Possible reusable test seam |
| --- | --- | --- |
| A downstream borrow inherits the upstream invocation, including derived cancellation | A01, A04, A08 | Enter, block between dependency resolutions, publish a route change, observe both old/new stage identities |
| Successful topology commit does not imply every native producer has enabled admission | A02, A03 | Queue input, prepare/commit, enable dependencies then ingress, observe actual handler/attempt count |
| Cancel/SDK Stop/actual execution exit are distinct observations | A09 | An entered callback ignores cancellation until a controlled release; compare native state, live work and physical resource probes |
| Host waits can pin the whole snapshot even when a particular business service is idle | A09 host-result test | Start a durable waiting execution and a local Get; close host admission and prove the Get exits before retirement |
| Error wrappers can change another framework's interpretation | A07 | Exercise the real upstream engine's retry decision, not merely errors.Is on a returned wrapper |
| Effect identity and execution identity must be separate | A06, A10 | Repeat the same command across versions/processes, verify stable receipt/effect alongside changed physical route identity |
| Persistence/replay and graceful resource drain need separate tests | A10, A11 | Kill one process, restore on another, replay real history, then gracefully drain the restored process |

One concrete application defect was found while implementing these cases: a
plain post-commit observer failure was initially treated like a retryable
pre-commit error. The regression reproduced a second attempt. Business code now
marks that post-commit uncertainty explicitly; the native Activity boundary emits
nonretryable OutcomeUnknown and an independent receipt query confirms the effect.
No Core or adapter patch was required.

Native semantics must stay distinct:

- Temporal owns Workflow history, retry, heartbeat/cancellation notification and
  remote task scheduling. A local Scope is one attempt, not the durable Workflow.
- Fx owns the static constructor/module graph and hook order. A Ref-backed
  adapter makes the boundary dynamic; static raw pointer injection remains static.
- PostgreSQL owns SQL transaction isolation/locking. The sample's operation key,
  digest and receipt/effect transaction provide its idempotency, not Forest Apply.
- Forest owns immutable local routing versions, declared contracts and referenced
  resource lifetimes. Neither a SQL Commit nor a Worker restart is a Forest commit.

Before designing reusable test helpers, compare a second independent execution
engine against these seams. Temporal × Eino is a suitable next bounded experiment:
Activity can borrow an existing Eino graph/agent through a consuming interface;
keep Eino's original graph nodes, streams and native interruption/checkpoints.
Its session Close/Join and native continuation/error semantics need their own
evidence before any common helper API is extracted. This delivery does not add
Eino, unify checkpoint formats, or implement adapters/testkit.
