# @requence/event-sourcing

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
