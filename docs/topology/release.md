# Topology module v0.1.0

Published tag: [`extensions/topology/v0.1.0`](https://github.com/Leeminjing/go-forest/releases/tag/extensions/topology/v0.1.0).
Annotated tag object `38ab104a50c485d721ce30c9520f57af4e6e6210` points to reviewed
source `9ed7a73b30212452ca101b392ca99401bd2beef0`.

Authors, committers, tagger, authenticated push and GitHub Release author were
verified as Leeminjing. Private Spec/Design are ignored and absent from Git.
No root Core version/tag change is required; this is an independent Go module.

Before publication, all [52 source-CI jobs](https://github.com/Leeminjing/go-forest/actions/runs/40511878963)
passed. Independent Standards and Spec re-reviews closed their respective P2
findings; see [review](review.md). Final-source Windows Go 1.23 and Linux race3
also passed locally with native Kill+Wait and cross-process historical recovery.

After tag publication, `scripts/verify-topology-release.py` passed on Windows
and Linux Go 1.23. Each run created an empty external project and fresh module
cache outside the checkout, disabled go.work, downloaded exact versions and
checked there were no replacements or dependency directories inside the repo:

| Module | Verified version |
| --- | --- |
| github.com/Leeminjing/go-forest | v0.3.0 |
| github.com/Leeminjing/go-forest/extensions/topology | v0.1.0 |

Both actually invoked the public APIs and observed:

```json
{"Values":[1,2,1,1],"Generations":[1,2,3],"ColdGeneration":1,"NewSession":true}
```

The external fixture defines its own consumer interface and native Definitions.
It captures an offline Forest, seeds durable topology, restores, invokes, changes
and rolls back, then uses another Forest/session. This acceptance does not count
a local-replace fixture preflight or `go list` alone as published execution.

Post-publication main CI adds `topology-published` to the 52 source gates. The
result of that 53-job run is checked independently before final delivery. The tag
is immutable; these release notes/CI additions do not rewrite the published code.

See [validation](validation.md) and the [module README](../../extensions/topology/README.md)
for the actual process-fault windows and limitations. Structure recovery does
not recover old Go objects/Scopes, undo business effects or certify power-loss
safety. Online historical switching explicitly rebuilds the entire target graph.
