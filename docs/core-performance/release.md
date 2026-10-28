# Core v0.3.1 fixed-version release

Published [v0.3.1](https://github.com/Leeminjing/go-forest/releases/tag/v0.3.1).

| Identity | Value |
| --- | --- |
| Tag target / reviewed Core | `3a4671bbdecdbc42b11067b51e4d47910c6a5b99` |
| Annotated tag object | `6d9768ec2f1d04402a117a79b602388cab589914` |
| Module checksum | `h1:k9Q3jg6ZHJ2rFVIsTF/DbMrgkze3A+zODE0RuufagpY=` |
| go.mod checksum | `h1:UejDPb4g0Q8YdwTVz0OyFLXw1WowR/DizszTlKhGkVc=` |

All [65 source-CI jobs](https://github.com/Leeminjing/go-forest/actions/runs/40662537546)
passed before publication. This compatible patch publishes the graph-owned routing
index; Go 1.23, public API, dependency checks and lifecycle semantics remain.
Performance results and the added index costs are in [results](results.md).

## Published consumer evidence

Windows and Linux **Go 1.23.12** ran an external project in a fresh empty module
cache with GOWORK=off, no workspace or dependency replacements and public proxy /
checksum database validation. The runner verifies the exact version, downloaded
package locations, module checksum, available Origin.Hash and all tagged production Forest
source hashes before vet and actual tests. Linux also ran race3. Both passed:

- A held invocation stays on its old value/generation across Replace and Rebind.
- New independent calls use v2, then the rebound provider v3.
- Old Stop is not called until the actual old invocation returns.
- Lookup allocation remains **16 -> 16 B/op** when 1,024 unrelated bindings are
  added. The same public regression against released v0.3.0 first failed at
  **136 -> 106,681 B/op**; it does not accept an old release under a new label.

[Windows metadata](release-data/windows-metadata.json) ·
[Windows tests](release-data/windows-test.txt) ·
[Linux metadata](release-data/linux-metadata.json) ·
[Linux race3](release-data/linux-test.txt) ·
[prior tag identities](release-data/tags.json) ·
[evidence hashes](release-data/sha256.json)

The first Linux attempt could not reach the public proxy because its container
DNS resolved to loopback; it failed before tests. A fresh-cache run using the
host's network proxy passed. No cached source or local replacement substituted
for the download. Both attempts remain in ignored local evidence.

Reproduce independently from a checkout with tags available:

```sh
python scripts/verify-core-release.py
python scripts/verify-core-release.py --race --count 3
```

The published consumer module is `tests/core-release-consumer`, Go 1.23, fixed
at Core v0.3.1. The runner copies it outside the repository and cleans only its
new owned cache. Existing v0.3.0 external consumers and source-candidate
integration remain distinct; CI adds Windows/Linux `core-patch-published` jobs
without deleting previous checks.

Authors, committer/tagger, authenticated pushes and Release author are Leeminjing.
Prior tags are unchanged. Agent Runtime, Focus-Go, stable Adapters and Topology
production files/dependency pins are not modified. The next Journal optimization
is a separate local Spec/Design draft, not part of this release. Spec/Design
remain ignored and absent from Git history.

## Independent release review

Fixed diff baseline is 3a4671b. Read-only Spec and Standards reviews reported
zero confirmed defects. They checked downloaded metadata, source identity,
actual tests, cache cleanup bounds, immutable prior tags and protected repository
scope. The optional wording about Origin.Hash was clarified: it is checked when
provided, while production source hashes are always required. Both saved
platform downloads supplied the matching Origin.Hash.
