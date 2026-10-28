# Topology v0.2.0 release

Published [extensions/topology/v0.2.0](https://github.com/Leeminjing/go-forest/releases/tag/extensions/topology/v0.2.0).

| Identity | Value |
| --- | --- |
| Reviewed implementation | `53f14c115c880b0e79d4384e6ecbd2e1a4f68617` |
| Annotated tag target | `2bc93a50c975d3ebff33bb0f91d5bf658a8cc769` |
| Tag object | `1679ecd225c98175ca0b1b85376786f13083c777` |
| Module checksum | `h1:fzICZiJrJ25T1QrmSliLidknKxAj/LvOUnZbTeN9/jU=` |

The tag target adds only fixed-version README instructions to the reviewed Go
implementation. All [55 source-CI jobs](https://github.com/Leeminjing/go-forest/actions/runs/40628544840)
completed successfully before publication. Final-source local Windows Go 1.23
vet/full tests, Linux Go 1.23 vet/race3 and Windows/Linux Go 1.25 real PostgreSQL
acceptance passed. See [review](review.md) and [validation](validation.md).

After publication, Windows and Linux Go 1.23 empty external projects downloaded
exact Core v0.3.0 and topology v0.2.0 into independent module caches, with GOWORK
disabled and no replacements. Module checksums verified. The consumer actually
read released-v0.1 journal bytes, retained an unrelated non-hot tree and runtime
consumer across Replace/Rollback/Rebind, verified original receipt idempotency,
then closed and cold-restored another Forest/session. Both reported:

```json
{"Values":[1,2,1,2,2],"Generations":[1,2,3,4],"ColdGeneration":1,"NewSession":true}
```

The formal Gin/PostgreSQL sample pins the released module and records its actual
checksum in go.sum. Its separate `--published` acceptance requires v0.2.0 without
replacement. Main CI adds `topology-v2-published` (fixed consumer plus native
PostgreSQL/race3) while preserving all original jobs, including published v0.1.0.
The resulting 56-job main run is independently verified before final delivery;
it is not inferred from the 55-job source run.

Authors, committers, tagger, authenticated tag/main pushes and Release author
are Leeminjing. Prior tags are immutable; root Core latest is unchanged.
Spec/Design stay ignored, local and absent from Git history. Core, stable
Adapters, TestKit, Agent Runtime and Focus-Go are unchanged.

New online requests use explicit v2 incremental semantics. Old v0.1 records keep
their original interpretation and receipts. A v0.1 binary cannot read new v2
intent fields; downgrade is not transparent. Unsafe moves/hot replacements are
blocked, and Preview does not prove actual external resource availability.
Historical rollback creates a new topology commit and does not undo business
effects or resurrect old Go objects/Scopes.
