# Independent state-machine review

2026-10-27. Baseline `7ae1354`; first implementation `983bf5a`. Two read-only
reviewers separately examined the approved local requirements and engineering
correctness. Private Spec/Design are not included in repository history.

## Spec

Two P2 findings, both closed after focused re-review:

1. The reducer visited operations/parameters in ordinary step plans but not
   Action.Plan or Action.Plans. It could incorrectly claim minimality for a
   concurrent plan with an irrelevant operation. All plan-bearing locations now
   share traversal. Red-before-green tests cover operation deletion and parameter
   simplification in concurrent plans.
2. Preflight/rebase simulated the first winner of an unordered race, allowing a
   later caller to depend on a tree that might not exist under the other legal
   winner. Unordered-race tails now conservatively permit only observations and
   completion of already-started calls. New dependent work requires the native
   prepared-race case. Post-race Wait also requires all previous calls joined:
   otherwise Replace-versus-AddRoot could deadlock a correct Forest behind an
   unreachable release. Exact rejection and safe native continuations have
   regression tests. No unordered winner state is guessed during rebase.

## Standards

One P2 finding, overlapping Spec finding 1: incomplete operation/parameter
reduction for concurrent plans. The shared traversal and clone isolation fix
closed it. No other hard correctness/standards finding remained in the reviewed
paths. The reviewer independently ran the pure model and native known-winner
continuation checks successfully.

## Disposition

Both focused re-reviews report zero outstanding findings. These were defects in
the new verification tools, not confirmed defects in Forest Core; Core was not
changed to satisfy the tests. Final-source Windows/Linux/race/extended and full
CI results are recorded separately from review approval.

The model is finite and independently specified. It validates executed inputs
and native facts, not all Go programs or arbitrary instruction scheduling.
Rule-relative 1-minimality is reported only after the final removal pass and
three same-signature confirmations; incomplete reductions retain the original.
