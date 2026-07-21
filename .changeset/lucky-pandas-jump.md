---
'@requence/event-sourcing': minor
---

Add SurrealDB storage adapter

A new storage adapter backed by SurrealDB (server 3.x, `surrealdb` SDK v2) is available as `@requence/event-sourcing/surreal`. It implements the full storage contract — append-only event log with gapless, commit-ordered global positions via a counter record, per-stream optimistic concurrency with `ConcurrencyError`, checkpoint compare-and-swap, and aggregate-root/projection snapshots. The adapter creates its `SCHEMAFULL` tables automatically with idempotent DDL (opt out via `initSchema: false`; the statements are also exported). `surrealdb` is an optional peer dependency.

The memory adapter's storage implementation is now also exported as `createMemoryStorage`, and both adapters are covered by a shared storage-driver contract test suite.
