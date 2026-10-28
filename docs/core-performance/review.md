# Core routing and measurement review

Fixed baseline: `5be60184d928d9df23782d15630d6f855cfd0645`.
Independent read-only reviews examined the complete diff through `8a618ab`,
then followed the measurement-only changes. The private approved Spec/Design
were read locally and remain excluded from Git.

## Standards

Initial review found no blocking standards violations or actionable smells.
The Core kept its Go 1.23 standard-library boundary, API and resource semantics;
test/host code remained outside production adapters and the Agent/Focus trees.
All commits use Leeminjing's identity.

Follow-up found one P2 robustness issue: the summary tool compared matching
harnesses without independently requiring matching environments/dimensions.
This did not establish that collected data were mismatched, but could permit a
future misleading comparison. Commit `dca628b` adds a method fingerprint over
actual Go version, platform/CPU model/quota, memory quota, environment, dimensions
and benchmark command, plus counterexamples for incompatible pairings. A changed
execution timeout alone is allowed because it does not alter measured work.

## Spec

Both reviews found no confirmed implementation deviation or scope expansion.
They verified candidate index construction before factories, graph ownership,
identity/phase checks, old Scope isolation, Many ordering/callback separation,
Controller fixture ownership and explicit RSS availability. Final measurements
and complete acceptance remained an execution gate, not an assumed success.

Performance data, resource tradeoffs and final regression results are recorded
in [results](results.md) and [validation](validation.md). This review does not
claim arbitrary new adapters or workloads have been proven by the experiment.

## Evidence closure

Follow-up review confirmed the P2 pairing issue closed and independently counted
1,480 observations / 74 pairs with ten samples per side. It verified headline
medians, management figures and all 18 load records against raw data. The final
ABBA supplement contributes 24 separate observations. Finalization refreshes
all hashes, including builder/source metadata and native Windows acceptance,
and preserves original raw bytes across Git newline settings. No additional
confirmed runtime, spec or standards defect remained.
