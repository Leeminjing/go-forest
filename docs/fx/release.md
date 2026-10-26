# Fx adapter v0.1.0 delivery

Delivered two independent Go modules, with Go 1.23 minimum and native Fx v1.24.0:

| Module | Version | Immutable source |
| --- | --- | --- |
| github.com/Leeminjing/go-forest/adapters/fx | v0.1.0 | adapters/fx/v0.1.0 → c087940b177c04a2db63d0bccea9a6dd8a45ce47 |
| github.com/Leeminjing/go-forest/examples/fx | v0.1.0 | examples/fx/v0.1.0 → 632edd92d3ca3e58bafbe1b7d1e968812a5880c1 |

The example pins the published adapter, with no replace directive. Final source
contents were checked against each module tag. Existing module tags are unchanged.

## What was verified

- Real Fx Module construction, lazy/named/grouped/private components and annotated
  native hooks, independent physical Apps and explicit service exports.
- Consumer-defined contracts in both directions, dynamic Ref proxies, Rebind and
  provider Replace without rebuilding the Fx App. Old calls remain on v1 and
  later independent calls use v2.
- Candidate preparation, explicit post-commit admission, real HTTP 503 before
  Enable, failed-plan socket cleanup, managed producers and actual call joins.
- Constructor/Invoke/start/rollback/cleanup failures, noncooperative hooks,
  child-task lifetime, 24 concurrent calls and per-call cancellation isolation.
- Checkable runtime Loop failures kept separate from ownership failures; queued
  and future background callers wake when the example closes.

The 17 adapter top-level tests and two example integration tests pass. Windows
Go 1.26.5 tests/vet and native demo passed. Linux Go 1.23 three-round race tests
passed for the adapter, standalone example and unchanged root module. Adapter
package-local coverage was 90.4%; this is not a whole-repository coverage claim.

The adapter's initial complete CI passed **37/37 jobs**:
[run 40277603106](https://github.com/Leeminjing/go-forest/actions/runs/40277603106).
The expanded CI also includes the standalone example matrix and published
consumer script; final delivery checks are attached to the latest main commit.

## External fixed-version acceptance

On Windows, `python scripts/verify-fx-release.py` ran outside this checkout with
GOWORK=off and normal Go module verification. It downloaded the three fixed
modules, rejected all Replace records and source directories inside the checkout,
built a consumer-owned interface, ran vet, and installed the versioned CLI.

The installed executable ran a real Fx App, a managed background work item, a
business-tree replacement and a real loopback HTTP request. Its verified output:

```json
{"Before":"v1:oak/v1:oak","Background":"v2:elm/v2:elm","HTTP":{"text":"v2:ash/v2:ash","generation":2}}
```

The public proxy initially retained a pre-publication negative lookup; acceptance
was rerun after propagation and passed without disabling checksum verification.
No temporary workspace override participates in published acceptance.

## Review and boundaries

Independent Standards and Spec review each found one P2, both fixed and
independently re-reviewed with no open findings. See [review](review.md) and the
[A01–A12 evidence](validation.md).

Forest Core and its go.mod/go.sum have no changes from f4d6c26. Agent remains
4e01beb and Focus-Go remains 4bd07f3. No Focus migration or Temporal rewrite.

Private Spec/Design and .scratch files remain ignored and absent from Git
history. This extension does not sandbox arbitrary Fx options, hot-replace an
arbitrary App, force-stop goroutines, own process signals, or promise rollback of
external side effects. Modules must obey the explicit resource/ingress contract;
see the [adapter README](../../adapters/fx/README.md).
