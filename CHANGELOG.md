# @requence/event-sourcing

## 1.5.0

### Minor Changes

- [`9f155e3`](https://github.com/requence/event-sourcing/commit/9f155e3ff463a391abf30b52a5eb439862f50bb9) Thanks [@Torsten85](https://github.com/Torsten85)! - Add SurrealDB storage adapter

  A new storage adapter backed by SurrealDB (server 3.x, `surrealdb` SDK v2) is available as `@requence/event-sourcing/surreal`. It implements the full storage contract — append-only event log with gapless, commit-ordered global positions via a counter record, per-stream optimistic concurrency with `ConcurrencyError`, checkpoint compare-and-swap, and aggregate-root/projection snapshots. The adapter creates its `SCHEMAFULL` tables automatically with idempotent DDL (opt out via `initSchema: false`; the statements are also exported). `surrealdb` is an optional peer dependency.

  The memory adapter's storage implementation is now also exported as `createMemoryStorage`, and both adapters are covered by a shared storage-driver contract test suite.

## 1.4.1

### Patch Changes

- [#11](https://github.com/requence/event-sourcing/pull/11) [`a6ba842`](https://github.com/requence/event-sourcing/commit/a6ba8420de13278af47f8d093514c663f67cab39) Thanks [@Torsten85](https://github.com/Torsten85)! - Fix a typo in the drizzle adapter's `projectionSnapshot.get` that called `super.parse` instead of `superjson.parse` when deserializing snapshot metadata. Reading back a projection snapshot previously threw a `TypeError` at runtime.

## 1.4.0

### Minor Changes

- [`afbcbe4`](https://github.com/requence/event-sourcing/commit/afbcbe4b5a853335a9e9606cb4325b2b53a7c463) Thanks [@Torsten85](https://github.com/Torsten85)! - Add settled defaults for process managers and transactions. Streams dispatched
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

## 1.3.0

### Minor Changes

- [`c32fb4d`](https://github.com/requence/event-sourcing/commit/c32fb4dfaa5285d63df1c2389263c77eed949427) Thanks [@Torsten85](https://github.com/Torsten85)! - Add optimistic-concurrency retry to `stream.settled()`. Passing
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

## 1.2.0

### Minor Changes

- [`7ed5770`](https://github.com/requence/event-sourcing/commit/7ed5770c29e16c346ff0890d8bda4c3681719b42) Thanks [@Torsten85](https://github.com/Torsten85)! - Expose the global log `position` on events passed to aggregate root event
  handlers. The runtime always delivered it when folding persisted events during
  stream loading and replay, but the handler type omitted it. It is typed as
  optional because events yielded by a command fold before they are appended and
  therefore have no position yet. This lets aggregates capture log positions in
  their state (e.g. the position of a delete event, to later reconstruct the
  dependency picture as it was just before the deletion).

## 1.1.1

### Patch Changes

- [`33fba96`](https://github.com/requence/event-sourcing/commit/33fba96b4f2b639f582d182e626b022b8e2aa7b1) Thanks [@Torsten85](https://github.com/Torsten85)! - Forward the `lock` option through the drizzle and memory adapter wrappers to
  the core event store. The option was documented (e.g. `redisLock` for
  multi-instance deployments) but both adapters silently dropped it, so custom
  locking strategies never took effect and the default in-memory lock was always
  used.

## 1.1.0

### Minor Changes

- [`2fe58f5`](https://github.com/requence/event-sourcing/commit/2fe58f5bef978f992f040ae1e7602aa07031dfae) Thanks [@Torsten85](https://github.com/Torsten85)! - Add optimistic concurrency control to checkpoints so stateful process managers
  can run across multiple instances without losing updates: when two instances
  fold events into the same process manager concurrently, the losing write is
  rejected, the latest state is reloaded, and the event is re-folded on top of it.
  Projections and stateless/after-effect writes advance the checkpoint
  monotonically. The `CheckpointMethods.upsert` signature changed to
  `upsert(checkpoint, expectedVersion) => boolean` and `Checkpoint` gained a
  `version` field (breaking only for custom storage adapters); the Drizzle
  `checkpoints` table gained a `version` column requiring a regenerated migration.
  Docs now include a multi-instance topology guide clarifying how the write path,
  projections, listeners, and process managers behave when scaling.

## 1.0.4

### Patch Changes

- [`72a4bfe`](https://github.com/requence/event-sourcing/commit/72a4bfea751080deae97842831b399e4b5de7958) Thanks [@Torsten85](https://github.com/Torsten85)! - Prevent a single failed append or event emission from permanently poisoning
  all subsequent operations. The shared serialization chains for `appendEvents`
  and `emitEvents` previously re-propagated a rejection to every following
  operation, so one error (e.g. a `ConcurrencyError`) could surface on unrelated
  streams and take down the whole process. Each operation's outcome is now
  isolated to its own caller while preserving ordering.

## 1.0.3

### Patch Changes

- [`7cbe65b`](https://github.com/requence/event-sourcing/commit/7cbe65b7c0c3aca052173ee4271fe6cc368f8593) Thanks [@Torsten85](https://github.com/Torsten85)! - widen database typing in drizzle adapter

## 1.0.2

### Patch Changes

- [`aeb1ddf`](https://github.com/requence/event-sourcing/commit/aeb1ddfdb8e78f09c7ef5fc36ef71e1f29e19194) Thanks [@Torsten85](https://github.com/Torsten85)! - ensure process manager state is not stale in a multi instance environment

- [`8bb7f8f`](https://github.com/requence/event-sourcing/commit/8bb7f8f6867937b930e9796c2735f448f24a5f51) Thanks [@Torsten85](https://github.com/Torsten85)! - fixed typing issue with utility types

## 1.0.1

### Patch Changes

- [`3556c79`](https://github.com/requence/event-sourcing/commit/3556c7941f699ed29459517c69ce94f92f2f1f43) Thanks [@Torsten85](https://github.com/Torsten85)! - added missing redis lock export and docs

- [`137a58e`](https://github.com/requence/event-sourcing/commit/137a58e154407a29891d033009766fcd7a7b8c7b) Thanks [@Torsten85](https://github.com/Torsten85)! - added README.md and link to docs

## 1.0.0

### Major Changes

- [`ce47357`](https://github.com/requence/event-sourcing/commit/ce473578b636daf9b212999918b0cb5922ab11ce) Thanks [@Torsten85](https://github.com/Torsten85)! - Initial release
