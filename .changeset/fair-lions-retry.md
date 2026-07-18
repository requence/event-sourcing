---
'@requence/event-sourcing': minor
---

Add optimistic-concurrency retry to `stream.settled()`. Passing
`settled({ maxRetries })` reloads the aggregate stream and re-applies the
dispatched commands when the append loses the version race
(`ConcurrencyError`), instead of surfacing the error. This complements the
distributed `lock`: the lock serializes writers to avoid the race, while
`maxRetries` recovers from the residual cases it cannot cover (e.g. a lock TTL
lapsing, or single-instance writes that still race the storage constraint).

Defaults to `0` (no retry — the previous behaviour is unchanged). Only the
append is retried, so command handlers must be pure functions of loaded state
and their arguments for retries to be safe. A losing attempt's lock is released
before the retry re-acquires, so retries do not wait out the lock's TTL.
