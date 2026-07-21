import { RecordId, type Surreal } from 'surrealdb'

import type {
  AggregateRootSnapshotMethods,
  AnyAggregateRoot,
} from '../createAggregateRoot.ts'
import type { CheckpointMethods } from '../createCheckpointApi.ts'
import type { ProjectionSnapshotMethods } from '../createProjection.ts'
import {
  ConcurrencyError,
  createEventStore as createBaseEventStore,
  isStreamEvents,
  isStreamId,
} from '../index.ts'
import type {
  EventStore,
  EventStoreParamsWithAggregateRootSnapshots,
  EventsFromRoot,
  LoadEvents,
  OnProgress,
} from '../createEventStore.ts'
import type { LockCreator } from '../lock.ts'
import { superjson } from '../superjson.ts'
import { TABLES, initSchema as runInitSchema } from './schema.ts'
import type {
  BaseInputEvent,
  BaseOutputEvent,
  MaybePromise,
  StreamId,
} from '../utilityTypes.js'

export { createAggregateRoot } from '../index.ts'
export { extendTypes } from '../superjson.ts'
export { SCHEMA_STATEMENTS, TABLES, initSchema } from './schema.ts'

export type OnEventsAppended = (events: BaseOutputEvent[]) => MaybePromise<void>

interface SurrealEventStoreParams<Root extends AnyAggregateRoot> {
  database: Surreal | (() => Surreal)
  aggregateRoots: Root[]
  autoInit?: boolean
  initSchema?: boolean
  lock?: LockCreator
  postProcessEvent?: EventStoreParamsWithAggregateRootSnapshots<Root>['postProcessEvent']
  onProjectionReplay?: OnProgress
  onProcessManagerRefresh?: OnProgress
  onEventsAppended?: OnEventsAppended
}

const CONFLICT_RETRIES = 10
const CONCURRENCY_MARKER = 'es_concurrency:'
const CHECKPOINT_CONFLICT_MARKER = 'es_checkpoint_conflict'

type QueryResponse =
  | { success: true; result: unknown }
  | { success: false; error: Error & { kind?: string } }

type ServerErrorLike = Error & {
  kind?: string
  isTransactionConflict?: boolean
}

function isThrownError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as ServerErrorLike).kind === 'Thrown'
  )
}

function isAlreadyExistsError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as ServerErrorLike).kind === 'AlreadyExists'
  )
}

function isUniqueIndexViolation(error: unknown): boolean {
  return (
    error instanceof Error && /index .* already contains/i.test(error.message)
  )
}

// Snapshot-isolation conflicts are safe to retry. SurrealDB >= 3.1 reports
// them with a structured `TransactionConflict` detail; older servers only
// expose the human-readable message.
function isTransactionConflict(error: unknown): error is ServerErrorLike {
  if (typeof error !== 'object' || error === null) {
    return false
  }
  const serverError = error as ServerErrorLike
  if (serverError.isTransactionConflict) {
    return true
  }
  return /read or write conflict|transaction can be retried/i.test(
    serverError.message ?? '',
  )
}

// Statements after a failed one only report generic "not executed" errors —
// surface the statement error that actually caused the transaction to fail.
function primaryError(errors: ServerErrorLike[]): ServerErrorLike {
  return (
    errors.find((error) => !/was not executed/i.test(error.message)) ??
    errors[0]
  )
}

function toDate(value: unknown): Date {
  if (value instanceof Date) {
    return value
  }
  const dateTime = value as { toDate?: () => Date }
  if (typeof dateTime?.toDate === 'function') {
    return dateTime.toDate()
  }
  return new Date(String(value))
}

function toOutputEvent(row: any): BaseOutputEvent {
  const { id: _id, ...event } = row
  return {
    ...event,
    position: Number(event.position),
    streamVersion: Number(event.streamVersion),
    createdAt: toDate(event.createdAt),
    payload: superjson.parse(event.payload),
    metadata: superjson.deserialize(event.metadata),
  }
}

// Jittered exponential backoff so concurrent writers racing on the position
// counter do not retry in lockstep.
function conflictBackoff(attempt: number) {
  const ms = Math.min(200, 5 * 2 ** attempt) * (0.5 + Math.random())
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// The head version is computed with array::max instead of
// ORDER BY ... DESC LIMIT 1: inside a transaction the index scan serves that
// query in ascending order with the limit pushed down, returning the lowest
// version instead of the highest.
const APPEND_QUERY = `
BEGIN;
LET $head = array::max(SELECT VALUE streamVersion FROM ${TABLES.events} WHERE streamId = $streamId AND streamType = $streamType) ?? 0;
IF $head != $expectedVersion { THROW "${CONCURRENCY_MARKER}" + <string>$head; };
LET $next = (UPSERT ONLY ${TABLES.counters}:${TABLES.events} SET value += $count RETURN VALUE value);
LET $base = $next - $count;
INSERT INTO ${TABLES.events} $events.map(|$event, $index| $event + { position: $base + $index + 1 });
COMMIT;
`

const CHECKPOINT_UPSERT_QUERY = `
BEGIN;
LET $current = (SELECT VALUE version FROM ONLY $rid);
IF $current = NONE {
  CREATE $rid CONTENT $content;
} ELSE IF $insertOnly = false AND $current = $expectedVersion {
  UPDATE $rid MERGE $changes;
} ELSE {
  THROW "${CHECKPOINT_CONFLICT_MARKER}";
};
COMMIT;
`

export function createSurrealStorage(params: {
  database: Surreal | (() => Surreal)
  initSchema?: boolean
  onEventsAppended?: OnEventsAppended
}): {
  loadEvents: LoadEvents
  appendEvents: (
    streamId: StreamId,
    events: BaseInputEvent[],
    expectedVersion: number,
  ) => Promise<BaseOutputEvent[]>
  checkpoint: CheckpointMethods
  projectionSnapshot: ProjectionSnapshotMethods
  aggregateRootSnapshot: AggregateRootSnapshotMethods
} {
  const db = () =>
    typeof params.database === 'function' ? params.database() : params.database

  let schemaReady: Promise<void> | undefined
  const ready = () => {
    if (params.initSchema === false) {
      return Promise.resolve()
    }
    schemaReady ??= runInitSchema(db())
    return schemaReady
  }

  // Statement failures come back as per-statement error responses, but
  // transaction conflicts reject the query promise itself — normalize both
  // into the errors array so the retry loops handle them uniformly.
  async function runWithResponses(
    query: string,
    bindings: Record<string, unknown>,
  ): Promise<{ responses: QueryResponse[]; errors: ServerErrorLike[] }> {
    try {
      const responses = (await db()
        .query(query, bindings)
        .responses()) as QueryResponse[]
      const errors = responses
        .filter((response) => !response.success)
        .map((response) => (response as { error: ServerErrorLike }).error)
      return { responses, errors }
    } catch (error) {
      if (isTransactionConflict(error)) {
        return { responses: [], errors: [error] }
      }
      throw error
    }
  }

  return {
    async *loadEvents(select, range) {
      await ready()
      const conditions = ['position > $lastPosition']
      const bindings: Record<string, unknown> = {}

      if (select) {
        const selects = Array.isArray(select) ? select : [select]
        const streamTypes = selects.map((select) => select.stream)
        const streamIds = selects
          .map((select) => (isStreamId(select) ? select.id : null))
          .filter(Boolean) as string[]
        const eventTypes = selects.flatMap((select) =>
          isStreamEvents(select) ? select.events : [],
        )
        conditions.push('streamType IN $streamTypes')
        bindings.streamTypes = streamTypes

        if (streamIds.length > 0) {
          conditions.push('streamId IN $streamIds')
          bindings.streamIds = streamIds
        }

        if (eventTypes.length > 0) {
          conditions.push('type IN $eventTypes')
          bindings.eventTypes = eventTypes
        }
      }
      let lastPosition = 0

      if (range?.from) {
        lastPosition = range.from - 1
      }

      // range.to is applied client-side: combining both bounds on the
      // uniquely indexed position field trips a SurrealDB 3.0 planner bug
      // where the exclusive lower bound is ignored.
      const chunkSize = 50
      const query = `SELECT * FROM ${TABLES.events} WHERE ${conditions.join(' AND ')} ORDER BY position ASC LIMIT ${chunkSize};`

      while (true) {
        const [resolvedEvents] = (await db()
          .query(query, { ...bindings, lastPosition })
          .collect()) as [any[]]

        if (resolvedEvents.length === 0) {
          break
        }

        for (const resolvedEvent of resolvedEvents) {
          const position = Number(resolvedEvent.position)
          if (range?.to && position > range.to) {
            return
          }
          yield toOutputEvent(resolvedEvent)
          lastPosition = position
        }
      }
    },

    async appendEvents(streamId, newEvents, expectedVersion) {
      await ready()
      const mappedEvents = newEvents.map((event, index) => ({
        streamId: streamId.id,
        streamType: streamId.stream,
        streamVersion: expectedVersion + 1 + index,
        type: event.type,
        payload: superjson.stringify(event.payload),
        metadata: superjson.serialize(event.metadata),
        causationId: event.causationId ?? undefined,
        actorId: event.actorId ?? undefined,
        correlationId: event.correlationId ?? undefined,
        schemaVersion: event.schemaVersion,
      }))

      for (let attempt = 0; ; attempt++) {
        const { responses, errors } = await runWithResponses(APPEND_QUERY, {
          streamId: streamId.id,
          streamType: streamId.stream,
          expectedVersion,
          count: mappedEvents.length,
          events: mappedEvents,
        })

        if (errors.length === 0) {
          const persistedEvents = responses
            .map((response) => (response.success ? response.result : undefined))
            .find(Array.isArray) as any[]

          const resolvedEvents = persistedEvents
            .map(toOutputEvent)
            .toSorted((eventA, eventB) => eventA.position - eventB.position)

          params.onEventsAppended?.(resolvedEvents)

          return resolvedEvents
        }

        const concurrency = errors.find(
          (error) =>
            isThrownError(error) && error.message.includes(CONCURRENCY_MARKER),
        )
        if (concurrency) {
          const currentVersion = Number(
            new RegExp(`${CONCURRENCY_MARKER}(\\d+)`).exec(
              concurrency.message,
            )![1],
          )
          throw new ConcurrencyError(streamId, expectedVersion, currentVersion)
        }

        if (errors.some(isUniqueIndexViolation)) {
          const [version] = (await db()
            .query(
              `RETURN array::max(SELECT VALUE streamVersion FROM ${TABLES.events} WHERE streamId = $streamId AND streamType = $streamType) ?? 0;`,
              { streamId: streamId.id, streamType: streamId.stream },
            )
            .collect()) as [number]

          const currentVersion = Number(version ?? 0)
          throw new ConcurrencyError(streamId, expectedVersion, currentVersion)
        }

        if (errors.some(isTransactionConflict) && attempt < CONFLICT_RETRIES) {
          await conflictBackoff(attempt)
          continue
        }

        throw primaryError(errors)
      }
    },

    checkpoint: {
      async upsert(checkpoint, expectedVersion) {
        await ready()
        const rid = new RecordId(TABLES.checkpoints, [
          checkpoint.type,
          checkpoint.name,
        ])
        const metadata = superjson.stringify(checkpoint.metadata)
        const changes = {
          lastEventPosition: checkpoint.lastEventPosition,
          metadata,
          version: checkpoint.version,
        }

        // Compare-and-swap: create when no checkpoint exists, otherwise only
        // apply when the stored version still matches `expectedVersion`. A
        // `null` expectation means "insert only" — an existing record is a
        // conflict.
        for (let attempt = 0; ; attempt++) {
          const { errors } = await runWithResponses(CHECKPOINT_UPSERT_QUERY, {
            rid,
            content: {
              type: checkpoint.type,
              name: checkpoint.name,
              ...changes,
            },
            changes,
            insertOnly: expectedVersion === null,
            expectedVersion: expectedVersion ?? 0,
          })

          if (errors.length === 0) {
            return true
          }

          if (
            errors.some(
              (error) =>
                isAlreadyExistsError(error) ||
                (isThrownError(error) &&
                  error.message.includes(CHECKPOINT_CONFLICT_MARKER)),
            )
          ) {
            return false
          }

          if (
            errors.some(isTransactionConflict) &&
            attempt < CONFLICT_RETRIES
          ) {
            await conflictBackoff(attempt)
            continue
          }

          throw primaryError(errors)
        }
      },
      async get(type, name) {
        await ready()
        const rid = new RecordId(TABLES.checkpoints, [type, name])
        const [checkpoint] = (await db()
          .query(`SELECT * OMIT id FROM ONLY $rid;`, { rid })
          .collect()) as [any]

        if (!checkpoint) {
          return null
        }

        return {
          type: checkpoint.type,
          name: checkpoint.name,
          lastEventPosition: Number(checkpoint.lastEventPosition),
          version: Number(checkpoint.version),
          metadata: superjson.parse(checkpoint.metadata),
        }
      },
      async delete(type, name) {
        await ready()
        const rid = new RecordId(TABLES.checkpoints, [type, name])
        await db().query(`DELETE $rid;`, { rid }).collect()
      },
    },

    projectionSnapshot: {
      async put(snapshot) {
        await ready()
        await db()
          .query(`CREATE ${TABLES.projectionSnapshots} CONTENT $content;`, {
            content: {
              projectionId: snapshot.projectionId,
              projectionType: snapshot.projectionType,
              lastEventPosition: snapshot.lastEventPosition,
              schemaVersion: snapshot.schemaVersion,
              state: superjson.stringify(snapshot.state),
              metadata: superjson.stringify(snapshot.metadata),
            },
          })
          .collect()
      },
      async get(select) {
        await ready()
        const [[snapshot]] = (await db()
          .query(
            `SELECT * OMIT id FROM ${TABLES.projectionSnapshots} WHERE projectionId = $id AND projectionType = $projection ORDER BY lastEventPosition DESC LIMIT 1;`,
            { id: select.id, projection: select.projection },
          )
          .collect()) as [any[]]

        if (!snapshot) {
          return null
        }

        return {
          ...snapshot,
          lastEventPosition: Number(snapshot.lastEventPosition),
          schemaVersion: Number(snapshot.schemaVersion),
          createdAt: toDate(snapshot.createdAt),
          state: superjson.parse(snapshot.state),
          metadata: superjson.parse(snapshot.metadata),
        }
      },
      async delete(target, fromPosition) {
        await ready()
        const conditions = ['projectionType = $projection']
        const bindings: Record<string, unknown> = {
          projection: target.projection,
        }

        if (target.id) {
          conditions.push('projectionId = $id')
          bindings.id = target.id
        }

        if (fromPosition) {
          conditions.push('lastEventPosition > $fromPosition')
          bindings.fromPosition = fromPosition
        }

        await db()
          .query(
            `DELETE ${TABLES.projectionSnapshots} WHERE ${conditions.join(' AND ')};`,
            bindings,
          )
          .collect()
      },
      async incrementAppliedCount(select) {
        await ready()
        for (let attempt = 0; ; attempt++) {
          try {
            const [appliedEvents] = (await db()
              .query(
                `UPSERT ONLY ${TABLES.projectionAppliedEvents}:[$projection, $id] SET appliedEvents += 1 RETURN VALUE appliedEvents;`,
                { projection: select.projection, id: select.id },
              )
              .collect()) as [number]

            return Number(appliedEvents)
          } catch (error) {
            if (isTransactionConflict(error) && attempt < CONFLICT_RETRIES) {
              await conflictBackoff(attempt)
              continue
            }
            throw error
          }
        }
      },
    },

    aggregateRootSnapshot: {
      async put(snapshot) {
        await ready()
        await db()
          .query(`CREATE ${TABLES.aggregateRootSnapshots} CONTENT $content;`, {
            content: {
              streamId: snapshot.streamId,
              streamType: snapshot.streamType,
              streamVersion: snapshot.streamVersion,
              lastEventPosition: snapshot.lastEventPosition,
              schemaVersion: snapshot.schemaVersion,
              state: superjson.stringify(snapshot.state),
              metadata: superjson.stringify(snapshot.metadata),
            },
          })
          .collect()
      },
      async get(select) {
        await ready()
        const [[snapshot]] = (await db()
          .query(
            `SELECT * OMIT id FROM ${TABLES.aggregateRootSnapshots} WHERE streamId = $id AND streamType = $stream ORDER BY streamVersion DESC LIMIT 1;`,
            { id: select.id, stream: select.stream },
          )
          .collect()) as [any[]]

        if (!snapshot) {
          return null
        }

        return {
          ...snapshot,
          streamVersion: Number(snapshot.streamVersion),
          lastEventPosition: Number(snapshot.lastEventPosition),
          schemaVersion: Number(snapshot.schemaVersion),
          createdAt: toDate(snapshot.createdAt),
          state: superjson.parse(snapshot.state),
          metadata: superjson.parse(snapshot.metadata),
        }
      },
      async delete(target, fromStreamVersion) {
        await ready()
        await db()
          .query(
            `DELETE ${TABLES.aggregateRootSnapshots} WHERE streamId = $id AND streamType = $stream AND streamVersion > $fromStreamVersion;`,
            {
              id: target.id,
              stream: target.stream,
              fromStreamVersion,
            },
          )
          .collect()
      },
    },
  }
}

export function createEventStore<const Root extends AnyAggregateRoot>({
  database,
  aggregateRoots,
  postProcessEvent,
  onProjectionReplay,
  onProcessManagerRefresh,
  onEventsAppended,
  autoInit,
  initSchema,
  lock,
}: SurrealEventStoreParams<Root>): EventStore<
  EventsFromRoot<Root>,
  Root,
  true
> {
  const storage = createSurrealStorage({
    database,
    initSchema,
    onEventsAppended,
  })

  return createBaseEventStore({
    aggregateRoots,
    postProcessEvent,
    onProjectionReplay,
    onProcessManagerRefresh,
    autoInit,
    lock,
    ...storage,
  }) as any
}
