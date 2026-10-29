# Independent review closeout

Two read-only review passes used the approved local behavior/implementation
documents and the fixed old-source baseline `ea90a93`. Production scope is
limited to Journal transition preparation/publication; no Core or Adapter
runtime change was accepted.

## Findings and resolution

- No confirmed production semantic defect remained in either review. The
  reviewers inspected old-value ownership, deferred map/slice writes, complete
  record validation, v1/v2 interpretation, fact uniqueness, Write/Sync/publication
  order, failed-prefix isolation and Go1.23 compatibility.
- Spec review identified missing physical request/session/kind counts and
  missing completed-Sync cancellation/Stat tests. The checksum/chain-based
  corpus counter and real-Sync wrappers in `3d03349` closed both gaps; the
  reviewer subsequently confirmed closure.
- Standards review identified two P2 evidence-tool issues: repeat measurements
  trusted old metadata without checking the selected input bytes, and the
  supplementary audit hard-coded its success count. Input SHA/inventory
  preflight and dynamic result counts now replace those assumptions. Deliberate
  wrong-input and wrong-harness tests fail before producing successful evidence.
  The fixed audit was rerun: 14 complete public projections and original
  receipts match. The reviewer confirmed both P2 findings closed.
- The final metrics review checked all 81 pairs/1,620 primary observations,
  representative table values, and the distinction between cumulative and live
  memory. No new confirmed defect was reported. Latency regressions, retained-
  heap increases, timing variation and baseline scheduling deviation are
  explicitly retained in the public records.
- Final Spec wording review caught an overbroad claim about pre-existing
  Conflict/Prefix mutation coverage. Explicit failed/cached receipt
  Conflict/FailureCodes mutations and returned-Prefix/repeated-Open checks were
  added and run with native Store/Apply failures. The claim now identifies
  those as new tests rather than attributing them to old coverage.

Reviews did not replace tests or performance runs. The acceptance artifacts
record actual source-mode service/race execution; fixed-version release checks
remain separately identified. Spec/Design stay local and ignored.
