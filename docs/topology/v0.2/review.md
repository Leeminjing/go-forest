# Incremental topology independent review

Baseline: `14ad67f171d46b52724517ddeea8a2f996c72cfa`.
Initial source: `e062b5a`. Approved Spec/Design remain local and ignored.
Two read-only reviewers independently checked Standards and Spec.

## Standards

Zero actionable findings. Independent Windows Go 1.23.12 full topology tests
passed (10.083 s). Protected Core/Adapter/TestKit/root dependency paths are
unchanged; author and committer are Leeminjing; Spec/Design are untracked.
The planner is large but its deterministic stages belong to one responsibility;
no speculative extraction was required merely to shorten a function.

## Spec

One observable gap in the initial source: S01 requested the target digest in
Preview, but Proposal exposed only BeforeDigest and the full Target. The fix adds
explicit TargetDigest, includes that claim in the v2-only request hash, and keeps
the exact released v0.1 hash struct/domain. A red-before-green regression verifies
the visible digest, rejection before journal mutation, and a conflicting altered
retry. Independent recheck of that regression and released-v1 samples passed.

The Spec reviewer also independently ran incremental/legacy/semantic replay tests
and the real Windows Gin/PostgreSQL source fixture. Actual old/new/rollback
requests were v1/v1, v2/v2 and v1/v1 at Generations 1/2/3; raw resource and native
instance identities were retained; HTTP admission returned 503 while preparing.
Zero remaining Spec findings or scope-creep findings.

These are source reviews, not a substitute for full CI or downloading and running
the released module. Those gates are recorded separately.
