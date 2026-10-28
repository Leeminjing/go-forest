# Incremental topology acceptance

Source baseline: `14ad67f`. The private approved Spec/Design are ignored and are
not release artifacts. The v0.1.0 tag and interpretation remain unchanged.

Implemented evidence (final review/publication gates tracked separately):

| Behavior | Verification |
| --- | --- |
| Local replacement | Only native Replace for changed business; non-hot unrelated instance retained |
| Runtime routing/order | Rebind preserves native Order and consumer; Many calls and Capture agree with target; prefix/suffix changes are visible |
| Phase closure | Construction/Startup provider replacement, optional addition/removal, binding reinsertion and chained consumers reconstruct the required consumers |
| Ownership | Two-level retained children, forced descendant rebuild, safe move out before old-parent deletion; unsafe move/hot closure blocks before intent |
| Legacy | Original released v0.1.0 producer samples, exact receipts/retries/failures/no-op, real killed pending intent; mixed v1/v2 bytes replay without rewriting prefix |
| Integrity | Rehashed invalid version/facts/impact/plan/erased metadata fail semantic replay |
| Persistence | Intent and observation write/partial/Sync failures do not retry native work or reconstruct stable trees |
| Incremental process loss | Real Kill+Wait at intent Sync, Build, Start, Ready, Apply-returned and observation Sync; fresh session requires reconciliation as appropriate |
| Native application | Actual Gin/TCP, native GORM/PostgreSQL queries and transaction receipts; old v1/v1, new v2/v2, rollback v1/v1; stable endpoint/engine/pool/InstanceIDs |
| Admission | Direct HTTP returns 503 before OpenAdmission and while candidate Start is held; no new business callback enters |
| Drain | Old business Stop cannot precede old request exit; final owned pool is actually closed; existing resource/Close-deadline tests retained |

Local Windows Go 1.23 topology vet/tests passed. Linux Go 1.23 root and topology
vet/three race rounds passed (including instrumented child processes). Real
Windows Gin/PostgreSQL acceptance passed with native Generations 1/2/3, business
instances 1/4/5 and stable db=2/gin=3 in the observed run; actual pointer values
are per-process diagnostic evidence, never durable identities.

Linux Go 1.25 real-service race passed all three rounds, with actual HTTP, SQL,
admission and identity evidence. A separate Windows Go 1.25 run also passed.
Independent Standards review closed with zero findings; Spec review closed its
one target-digest observability finding and independently repeated real Windows
PostgreSQL acceptance. See [review](review.md). Full remote CI and new fixed-version
external acceptance remain final gates, not inferred from these source passes.
Windows hosted CI explicitly uses compile-only mode for the new example (its real
PostgreSQL case is skipped there); the local Windows run above actually used a
PostgreSQL server. Linux hosted CI runs the real service/race test. Existing jobs
and original v0.1.0 published consumer remain present.

Incremental planning does not guarantee globally minimal operations. A target
order requiring new phase binding positions can cause extra, explained consumer
reconstruction and may be blocked by native hot-replacement promises. Preview
cannot prove actual resource availability or capability implementation types.
Topology rollback never compensates committed business effects.
