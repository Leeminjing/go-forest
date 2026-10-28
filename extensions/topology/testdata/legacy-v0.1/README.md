# Released v0.1.0 evidence

Generated on Windows, 2026-10-28, by the adjacent `producer.go.txt` copied to a
separate module with `GOWORK=off`, no replacements, and these downloaded modules:

- `github.com/Leeminjing/go-forest/extensions/topology v0.1.0`
  `h1:ZPfvkQU6p9lAVV0jYSWzMt+c0N7SkzxFRGcNH8490Cg=`
- `github.com/Leeminjing/go-forest v0.3.0`

`history/journal` contains real native cold construction, a full-rebuild v2
change, no-op, failed native Start, and historical rollback. Its exact original
inputs and receipts are in `history/evidence.json`. The producer closed and
joined before the sample was copied.

For `pending/journal`, the producer first restored v1, then prepared a v2
change. The parent observed `LEGACY_READY` from the actual candidate Factory,
called Process.kill(), and waited for termination before retaining the bytes.
That directory therefore contains an unresolved legacy reconfiguration intent,
not a fabricated successful observation. No lock-file handle is part of a sample.

Tests copy these bytes to temporary directories. They never rewrite the samples
or generate a supposedly legacy journal using the current planner. These files
contain deterministic test definitions, random store/session identities and no
credentials or business data. Windows process testing does not certify arbitrary
filesystem power-loss behavior.
