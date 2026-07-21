import type { Surreal } from 'surrealdb'

export const TABLES = {
  events: 'events',
  counters: 'event_sourcing_counters',
  checkpoints: 'checkpoints',
  aggregateRootSnapshots: 'aggregate_root_snapshots',
  projectionSnapshots: 'projection_snapshots',
  projectionAppliedEvents: 'projection_applied_events',
} as const

// Idempotent DDL — safe to run on every boot. All tables are SCHEMAFULL, so
// SurrealDB rejects writes carrying fields that are not defined here.
export const SCHEMA_STATEMENTS = `
DEFINE TABLE IF NOT EXISTS ${TABLES.events} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS position ON ${TABLES.events} TYPE int;
DEFINE FIELD IF NOT EXISTS streamId ON ${TABLES.events} TYPE string;
DEFINE FIELD IF NOT EXISTS streamType ON ${TABLES.events} TYPE string;
DEFINE FIELD IF NOT EXISTS streamVersion ON ${TABLES.events} TYPE int;
DEFINE FIELD IF NOT EXISTS type ON ${TABLES.events} TYPE string;
DEFINE FIELD IF NOT EXISTS createdAt ON ${TABLES.events} TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS payload ON ${TABLES.events} TYPE string;
DEFINE FIELD IF NOT EXISTS metadata ON ${TABLES.events} TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS causationId ON ${TABLES.events} TYPE option<string>;
DEFINE FIELD IF NOT EXISTS correlationId ON ${TABLES.events} TYPE option<string>;
DEFINE FIELD IF NOT EXISTS actorId ON ${TABLES.events} TYPE option<string>;
DEFINE FIELD IF NOT EXISTS schemaVersion ON ${TABLES.events} TYPE int;
DEFINE INDEX IF NOT EXISTS events_stream_unique ON ${TABLES.events} FIELDS streamId, streamType, streamVersion UNIQUE;
DEFINE INDEX IF NOT EXISTS events_position ON ${TABLES.events} FIELDS position UNIQUE;

DEFINE TABLE IF NOT EXISTS ${TABLES.counters} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS value ON ${TABLES.counters} TYPE int;

DEFINE TABLE IF NOT EXISTS ${TABLES.checkpoints} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS type ON ${TABLES.checkpoints} TYPE string;
DEFINE FIELD IF NOT EXISTS name ON ${TABLES.checkpoints} TYPE string;
DEFINE FIELD IF NOT EXISTS lastEventPosition ON ${TABLES.checkpoints} TYPE int;
DEFINE FIELD IF NOT EXISTS metadata ON ${TABLES.checkpoints} TYPE string;
DEFINE FIELD IF NOT EXISTS version ON ${TABLES.checkpoints} TYPE int DEFAULT 0;

DEFINE TABLE IF NOT EXISTS ${TABLES.aggregateRootSnapshots} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS streamId ON ${TABLES.aggregateRootSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS streamType ON ${TABLES.aggregateRootSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS streamVersion ON ${TABLES.aggregateRootSnapshots} TYPE int;
DEFINE FIELD IF NOT EXISTS state ON ${TABLES.aggregateRootSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS metadata ON ${TABLES.aggregateRootSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS createdAt ON ${TABLES.aggregateRootSnapshots} TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS lastEventPosition ON ${TABLES.aggregateRootSnapshots} TYPE int;
DEFINE FIELD IF NOT EXISTS schemaVersion ON ${TABLES.aggregateRootSnapshots} TYPE int;
DEFINE INDEX IF NOT EXISTS aggregate_root_snapshots_unique ON ${TABLES.aggregateRootSnapshots} FIELDS streamType, streamId, streamVersion UNIQUE;

DEFINE TABLE IF NOT EXISTS ${TABLES.projectionSnapshots} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS projectionId ON ${TABLES.projectionSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS projectionType ON ${TABLES.projectionSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS state ON ${TABLES.projectionSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS metadata ON ${TABLES.projectionSnapshots} TYPE string;
DEFINE FIELD IF NOT EXISTS createdAt ON ${TABLES.projectionSnapshots} TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS lastEventPosition ON ${TABLES.projectionSnapshots} TYPE int;
DEFINE FIELD IF NOT EXISTS schemaVersion ON ${TABLES.projectionSnapshots} TYPE int;
DEFINE INDEX IF NOT EXISTS projection_snapshots_unique ON ${TABLES.projectionSnapshots} FIELDS projectionId, projectionType, lastEventPosition UNIQUE;

DEFINE TABLE IF NOT EXISTS ${TABLES.projectionAppliedEvents} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS appliedEvents ON ${TABLES.projectionAppliedEvents} TYPE int;
`

export async function initSchema(database: Surreal): Promise<void> {
  await database.query(SCHEMA_STATEMENTS)
}
