import { sql } from 'drizzle-orm'
import {
  bigint,
  index,
  integer,
  jsonb,
  pgPolicy,
  pgSchema,
  text,
  timestamp,
  unique,
  uuid,
} from 'drizzle-orm/pg-core'
import type { SuperJSONResult } from 'superjson'

export const eventSourcing = pgSchema('event_sourcing')

export const events = eventSourcing.table(
  'events',
  {
    position: bigint({ mode: 'number' })
      .generatedByDefaultAsIdentity()
      .primaryKey(),
    id: uuid().unique().notNull().defaultRandom(),
    streamId: text().notNull(),
    streamType: text().notNull(),
    streamVersion: bigint({ mode: 'number' }).notNull(),
    type: text().notNull(),
    createdAt: timestamp().defaultNow().notNull(),
    payload: text().notNull(),
    metadata: jsonb().$type<SuperJSONResult>().notNull(),
    causationId: uuid(),
    correlationId: uuid(),
    actorId: text(),
    schemaVersion: integer().notNull(),
  },
  (table) => [
    unique().on(table.streamId, table.streamVersion, table.streamType),
    index().on(table.streamId, table.streamVersion.desc()),
    index('metadata').using('gin', sql`${table.metadata} jsonb_path_ops`),
    pgPolicy('insert only', {
      for: 'insert',
      to: 'public',
      withCheck: sql`true`,
    }),
  ],
)

export const aggregateRootSnapshots = eventSourcing.table(
  'aggregate_root_snapshots',
  {
    id: uuid().primaryKey().defaultRandom(),
    streamId: text().notNull(),
    streamType: text().notNull(),
    streamVersion: bigint({ mode: 'number' }).notNull(),
    state: text().notNull(),
    metadata: text().notNull(),
    createdAt: timestamp().defaultNow().notNull(),
    lastEventPosition: bigint({ mode: 'number' }).notNull(),
    schemaVersion: integer().notNull(),
  },
  (table) => [
    unique().on(table.streamType, table.streamId, table.streamVersion),
    index().on(table.streamType, table.streamId, table.streamVersion.desc()),
  ],
)

export const projectionSnapshots = eventSourcing.table(
  'projection_snapshots',
  {
    id: uuid().primaryKey().defaultRandom(),
    projectionId: text().notNull(),
    projectionType: text().notNull(),
    state: text().notNull(),
    metadata: text().notNull(),
    createdAt: timestamp().defaultNow().notNull(),
    lastEventPosition: bigint({ mode: 'number' }).notNull(),
    schemaVersion: integer().notNull(),
  },
  (table) => [
    unique().on(
      table.projectionId,
      table.projectionType,
      table.lastEventPosition,
    ),
    index().on(
      table.projectionId,
      table.projectionType,
      table.lastEventPosition.desc(),
    ),
  ],
)

export const checkpoints = eventSourcing.table(
  'checkpoints',
  {
    id: uuid().defaultRandom().primaryKey(),
    type: text({ enum: ['projection', 'processManager'] }).notNull(),
    name: text().notNull(),
    lastEventPosition: bigint({ mode: 'number' }).notNull(),
    metadata: text().notNull(),
  },
  (table) => [
    unique().on(table.type, table.name),
    index().on(table.type, table.name),
  ],
)

export const projectionAppliedEvents = eventSourcing.table(
  'projection_applied_events',
  {
    projectionId: uuid().notNull(),
    projectionType: text().notNull(),
    appliedEvents: integer().notNull(),
  },
  (table) => [
    unique().on(table.projectionId, table.projectionType),
    index().on(table.projectionId, table.projectionType),
  ],
)
