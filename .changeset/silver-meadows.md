---
'@requence/event-sourcing': minor
---

Add optimistic concurrency control to checkpoints so stateful process managers
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
