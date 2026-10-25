# Temporal two-axis review

Fixed baseline: `5cdb6fe9603d77fd5638ec6c7409bf3df763fd92`.
Two read-only reviewers examined independent Standards and Spec axes, then
reviewed the focused corrections. The approved local Spec/Design were not staged.

## Standards

Initial findings:

- The registration parameter's static Registry type hid Start/Stop, but its
  dynamic value was still the real Worker. A callback could assert Worker and
  start during preparation. A private façade with named private fields now
  exposes only native registration methods. The regression first failed and
  then verified neither Worker actuator nor Nexus registry can be extracted.
- Client interceptor slices could be replaced by the caller after validation,
  injecting a WorkerInterceptor outside the Activity guard. Both definitions
  now copy client interceptor/propagator/dial option slices and clone TLS config.
  The real execution regression mutates the original slice and verifies the
  injected outer hook is never invoked.

Re-review closed both. No remaining confirmed documented-standard violations
or actionable baseline code smells were reported.

## Spec

Initial findings:

- Nonempty namespace lacked identity bounds. Both Client and Worker now reject
  overlong, invalid UTF-8 and NUL namespace; default namespace remains supported.
- Evidence gaps for registration panic rollback, repeated successful Enable,
  idle Workflow scope release and no model execution during replay were closed
  with real-service regressions and explicit observations.

Re-review found no remaining specification discrepancy. The Activity error loop
also reproduced a separate issue before review: joining a native nonretryable
ApplicationError converted it into a retryable generic failure. The adapter now
preserves concrete native errors. The unknown-result test proves one attempt,
then explicit reconciliation, rather than automatic repeated effects.

Final open findings: Standards 0; Spec 0. Final platform/race and published-module
checks remain independent delivery gates, recorded in validation/release notes.
