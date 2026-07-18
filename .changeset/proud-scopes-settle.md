---
'@requence/event-sourcing': minor
---

Add settled defaults for process managers and transactions. Streams dispatched
inside a process manager or a transaction cannot call `settled()` themselves —
the surrounding scope auto-settles them, previously always with default
options. Both scopes now accept defaults for those auto-settles:
`createProcessManager(name, { settled: { maxRetries } })` and
`transaction(handler, { settled: { maxRetries } })`. This makes the
concurrency retry available to unattended writes (cascades, cross-aggregate
invariants) where no caller is present to observe or retry a
`ConcurrencyError`.

A settle retry now also re-applies the commands within the async context
captured at the dispatch site (`AsyncLocalStorage.snapshot()`), so
`AsyncLocalStorage`-based state observed by command handlers and
`postProcessEvent` — request metadata, tenant or branch scopes — matches the
first attempt even when the retry runs later from a different context, such as
an auto-settle. The retry explicitly exits the transaction scope (the first
attempt already passed the commit gate) and the upstream collection scope (the
retry is awaited directly).

The `settled()` guard errors inside process managers and transactions now
point to the scope-level options.
