# Fx delivery review

Fixed baseline: `f4d6c26ae0a661b786bf8e792e73d06708512d47`.
Implementation: `a953928`; separate Standards and Spec reviewers read the
committed diff and the approved local requirements. Private requirements remain
ignored and are not copied into history.

## Standards

One P2: the example could leave queued or future Background calls blocked after
its only producer exited. Fixed by signalling both closing admission and actual
producer exit, checking them during enqueue and result waits. The public
TestClosingRuntimeWakesQueuedAndFutureBackgroundCalls failed before the fix and
passes afterwards. No Core/module-boundary violations or material baseline smells
were reported. Independent re-review of `81b1d0c` confirmed closure; example
tests were rerun three times and vet passed.

## Spec

One P2: managed Loop failure was reduced to an error-stage label, losing the
checkable cause. Fixed with a separate ErrRuntime/Failure result and RuntimeFailed
status, returned by Quiesce. It does not poison resource ownership or prevent
final Stop. TestLoopFailureIsCheckableWithoutPoisoningResourceCleanup covers
both a sentinel and panic, and successful eventual resource release. No scope
expansion or other confirmed A01–A12 defect was reported. Independent re-review
of `81b1d0c` confirmed closure; new failure and queue regressions passed ten
repetitions. There are no open confirmed findings on either axis.

Both fixes passed Windows tests/vet and Linux Go 1.23 three-round race validation.
