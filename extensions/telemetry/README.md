# Explicit runtime telemetry

Optional Go 1.26 module. `New(f, Options)` accepts at most 128 allowlisted
operation names, bounded detail capacity (up to 256), freshness period and
host-owned OpenTelemetry TracerProvider/MeterProvider. No global SDK changes.
Configure bounded asynchronous export and Views, never a blocking synchronous
exporter on business calls. Hosts own SDK shutdown/exporter diagnostics.

Call `Measure(ctx, operation, callback)` inside an existing Scope; propagate its
callback Context. Calls/errors/in-flight/duration are independent of trace
sampling. Metric attributes use only allowlisted names; no inputs or raw errors.
Generations/session IDs appear in spans/details, not unbounded metric labels.
The instrumentation session ID is **not** Core InstanceID.

Snapshot returns detached cumulative totals since session start, sampling time,
freshness, recent details and discarded-detail count. Integer JSON counters are
strings. Unused/stale operations differ from recent observations. There is no
automatic Ref interception, provider-identity inference or all-Link ranking.

`Definition(typeID)` optionally publishes `runtime`; `ReadRuntime(ctx)` satisfies
Observatory's optional `runtime` Port. Bind explicitly. Bounded local diagnostics
work without SDK/backend; host SDKs enable native exporters. OTel v1.47.0.
