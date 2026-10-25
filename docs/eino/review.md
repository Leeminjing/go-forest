# Eino adapter review

Review baseline: `68e583d6282a84ab5a48c86a85b238e0e25593ba`.
Two independent read-only review axes were run, followed by focused fixes and
re-review. No approved local Spec/Design was added to Git.

## Standards

Profile fields originally accepted invalid UTF-8, allowing JSON to replace bytes
and make a successfully saved checkpoint fail its own profile check. Profile,
namespace and checkpoint ID validation now reject invalid UTF-8/NUL; regression
coverage exercises all fields and the actual ADK entry point.

A cancellation experiment reproduced an external Lifetime waiting for Cancel
while native execution blocked. A joined Context notification now forwards
Cancel once, independently of native iteration. Final caller cancellation is
checked after all ownership gates drain. The new regression first failed and
then passed. Review verified the AfterFunc/Once/Join ordering.

No remaining documented-standard violations or actionable baseline code smells
were found. The adapter remains a separate module without Agent/Core coupling.

## Spec

Initial review found two acceptance coverage gaps: hot replacement during an
output stream, and adapter integration tests for startup failure, required
dependency removal and unsafe replacement. Both were added using real engines,
with barriers and resource counters rather than timing-based exit assumptions.
Re-review closed both findings and found no remaining specification discrepancy.

Final open findings: Standards 0; Spec 0. Release/download verification is a
separate delivery gate recorded after publication, not inferred from unit tests.
