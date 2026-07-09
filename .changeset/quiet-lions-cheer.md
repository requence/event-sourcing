---
'@requence/event-sourcing': patch
---

Prevent a single failed append or event emission from permanently poisoning
all subsequent operations. The shared serialization chains for `appendEvents`
and `emitEvents` previously re-propagated a rejection to every following
operation, so one error (e.g. a `ConcurrencyError`) could surface on unrelated
streams and take down the whole process. Each operation's outcome is now
isolated to its own caller while preserving ordering.
