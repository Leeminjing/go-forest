# Topology recovery acceptance

Baseline: `657a783`. Approved Spec/Design remain local and ignored. This delivery
adds the optional Go 1.23 `extensions/topology` module, depending on released
Forest `v0.3.0`; protected Core, Adapter, TestKit, Agent and Focus-Go sources remain
unchanged.

| Gate | Native/public evidence |
| --- | --- |
| Capture/format | Manifest preserves IDs, definition identities, parent relationships, Link/BindNest and relative binding order; duplicate keys, unsupported format and detached values checked |
| Cold restore | Fresh native Forest, one commit, explicit admission, idempotent attempt, new session; missing catalog fails before construction |
| Phase/resource assembly | Two-level Nest, real Construction/Startup Ref reads, Runtime Many reads in actual provider order, dependency probes during Stop |
| Persistence | OS lock, CAS, framed checksums and sequence chain, stale external anchor, short/failed writes and Sync uncertainty; no automatic repair |
| Process recovery | Actual Kill+Wait before intent, during partial/full unacknowledged intent write, after intent Sync, during Build/Start/Ready, after Apply and after observation Sync |
| Reconciliation | Pending blocks native recreation; host choice recorded separately; damaged original preserved and explicit import creates a different StoreID |
| Rollback | New native Generation and desired Revision; old callback reads its original real file; new callback gets a new resource; no-op preserves instances |
| Rejections | Unsafe hot replacement, exclusive file contention, weakened capability, invalid links, missing dependencies and lifecycle cycles retain the old/empty graph |
| Drain/identity | Cancel is not exit; actual preparing work must return; Close deadline retains both OS lock and referenced native resource until actual join |
| Business effects | Durable business receipt remains unchanged through a topology rollback |
| Independent review | One Standards P2 fixed and one Spec P2 test gap closed; focused re-reviews independently pass, zero remaining findings; [review](review.md) |
| Final platform/release | Windows Go 1.23 and Linux three-round race pass; all 52 source-CI jobs pass; published v0.1.0 consumed externally on both platforms; [release](release.md) |

Released source `9ed7a73`: Windows Go 1.23 vet/full tests passed (topology 3.415s).
Linux Go 1.23 vet/three race rounds passed (topology 8.772s), with actual child
instrumentation and the combined history/restart case. Root Windows vet/tests and
Linux root race regressions passed. The complete [source CI](https://github.com/Leeminjing/go-forest/actions/runs/40511878963)
passed all 52 jobs, including original services, browser and published consumers.

`extensions/topology/v0.1.0` was then published from that exact source. Windows and
Linux empty external projects, with independent module caches and GOWORK=off,
downloaded the fixed module without replace, verified checksums, ran vet and
actually executed Capture/Seed/Restore/Invoke/Reconfigure/Rollback/new-session
recovery. Observed values were 1/2/1/1; native Generations were 1/2/3 followed by
cold Generation 1, with a new session. The added `topology-published` CI gate
repeats that independent download. Expanded main CI has 53 jobs; its actual run
is a separate final delivery check, not inferred from the local/source passes.

The demo actually executes Capture → Seed → Restore → Invoke → Reconfigure →
Rollback → second Forest Restore, yielding v1/v2/v1/v1 with in-process Generations
1/2/3 and cold Generation 1 in a new session.

The finite tests cover process faults, not arbitrary hardware power loss. The
module never restores Go objects or business execution state, cannot atomically
commit the OS journal with Forest Apply, and cannot revoke an ingress that the
host leaves outside managed admission. Full-rebuild switching does not promise
incremental instance retention or universal resource compatibility.
