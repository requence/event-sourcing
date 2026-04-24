import {
  type SQL,
  and,
  asc,
  desc,
  eq,
  gt,
  inArray,
  lte,
  sql,
} from 'drizzle-orm'
import type { NodePgDatabase } from 'drizzle-orm/node-postgres'

import type { AnyAggregateRoot } from '../createAggregateRoot.ts'
import {
  ConcurrencyError,
  createEventStore as createBaseEventStore,
  isStreamEvents,
  isStreamId,
} from '../index.ts'
import {
  aggregateRootSnapshots,
  checkpoints,
  events,
  projectionAppliedEvents,
  projectionSnapshots,
} from './postgres.ts'
import type {
  EventStore,
  EventStoreParamsWithAggregateRootSnapshots,
  EventsFromRoot,
  OnProgress,
} from '../createEventStore.ts'
import { superjson } from '../superjson.ts'
import type {
  BaseOutputEvent,
  MaybePromise,
  OutputEvent,
} from '../utilityTypes.js'

export { createAggregateRoot } from '../index.ts'
export { extendTypes } from '../superjson.ts'

export type OnEventsAppendend = (
  events: BaseOutputEvent[],
) => MaybePromise<void>

interface DrizzleEventStoreParams<Root extends AnyAggregateRoot> {
  database: NodePgDatabase<any> | (() => NodePgDatabase<any>)
  aggregateRoots: Root[]
  autoInit?: boolean
  postProcessEvent?: EventStoreParamsWithAggregateRootSnapshots<Root>['postProcessEvent']
  onProjectionReplay?: OnProgress
  onProcessManagerRefresh?: OnProgress
  onEventsAppended?: OnEventsAppendend
}

function isPgUniqueViolation(
  err: unknown,
): err is { cause: { code: string; constraint?: string } } {
  if (typeof err !== 'object' || err === null) {
    return false
  }

  const cause = (err as any).cause
  if (typeof cause !== 'object' || cause === null) {
    return false
  }

  if (cause.code !== '23505') {
    return false
  }

  return true
}

function createEventsQuery(database: NodePgDatabase<any>) {
  return database
    .select({
      position: events.position,
      type: events.type,
      schemaVersion: events.schemaVersion,
      payload: events.payload,
      streamId: events.streamId,
      streamType: events.streamType,
      streamVersion: events.streamVersion,
      createdAt: events.createdAt,
      actorId: events.actorId,
      correlationId: events.correlationId,
      causationId: events.causationId,
      metadata: events.metadata,
    })
    .from(events)
    .orderBy(asc(events.position))
    .$dynamic()
}

type EventsQuery = ReturnType<typeof createEventsQuery>

export async function fetchEvents<E extends OutputEvent<any> = BaseOutputEvent>(
  database: NodePgDatabase<any>,
  modifyQuery: (query: EventsQuery, table: typeof events) => EventsQuery = (
    q,
  ) => q,
) {
  const resolvedEvents = await modifyQuery(createEventsQuery(database), events)

  return resolvedEvents.map((resolvedEvent) => ({
    ...resolvedEvent,
    payload: superjson.parse(resolvedEvent.payload),
    metadata: superjson.deserialize(resolvedEvent.metadata),
  })) as E[]
}

export function createEventStore<const Root extends AnyAggregateRoot>({
  database,
  aggregateRoots,
  postProcessEvent,
  onProjectionReplay,
  onProcessManagerRefresh,
  onEventsAppended,
  autoInit,
}: DrizzleEventStoreParams<Root>): EventStore<
  EventsFromRoot<Root>,
  Root,
  true
> {
  const db = () => (typeof database === 'function' ? database() : database)

  return createBaseEventStore({
    aggregateRoots,
    postProcessEvent,
    onProjectionReplay,
    onProcessManagerRefresh,
    autoInit,
    async *loadEvents(select, range) {
      const conditions = [] as SQL[]

      if (select) {
        const selects = Array.isArray(select) ? select : [select]
        const streamTypes = selects.map((select) => select.stream)
        const streamIds = selects
          .map((select) => (isStreamId(select) ? select.id : null))
          .filter(Boolean) as string[]
        const eventTypes = selects.flatMap((select) =>
          isStreamEvents(select) ? select.events : [],
        )
        conditions.push(inArray(events.streamType, streamTypes))

        if (streamIds.length > 0) {
          conditions.push(inArray(events.streamId, streamIds))
        }

        if (eventTypes.length > 0) {
          conditions.push(inArray(events.type, eventTypes))
        }
      }
      let lastPosition = 0

      if (range?.from) {
        lastPosition = range.from - 1
      }

      if (range?.to) {
        conditions.push(lte(events.position, range.to))
      }

      const chunkSize = 50

      while (true) {
        const resolvedEvents = await db()
          .select({
            position: events.position,
            type: events.type,
            schemaVersion: events.schemaVersion,
            payload: events.payload,
            streamId: events.streamId,
            streamType: events.streamType,
            streamVersion: events.streamVersion,
            createdAt: events.createdAt,
            actorId: events.actorId,
            correlationId: events.correlationId,
            causationId: events.causationId,
            metadata: events.metadata,
          })
          .from(events)
          .where(and(...conditions, gt(events.position, lastPosition)))
          .limit(chunkSize)
          .orderBy(asc(events.position))

        if (resolvedEvents.length === 0) {
          break
        }

        for (const resolvedEvent of resolvedEvents) {
          yield {
            ...resolvedEvent,
            payload: superjson.parse(resolvedEvent.payload),
            metadata: superjson.deserialize(resolvedEvent.metadata),
          }
          lastPosition = resolvedEvent.position
        }
      }
    },
    async appendEvents(streamId, newEvents, expectedVersion) {
      const mappedEvents = newEvents.map((event, index) => ({
        streamId: streamId.id,
        streamType: streamId.stream,
        streamVersion: expectedVersion + 1 + index,
        type: event.type,
        payload: superjson.stringify(event.payload),
        metadata: superjson.serialize(event.metadata),
        causationId: event.causationId,
        actorId: event.actorId,
        correlationId: event.correlationId,
        schemaVersion: event.schemaVersion,
      }))

      try {
        const persistedEvents = await db()
          .insert(events)
          .values(mappedEvents)
          .returning()

        const resolvedEvents = persistedEvents
          .map((persistendEvent) => ({
            ...persistendEvent,
            payload: superjson.parse(persistendEvent.payload),
            metadata: superjson.deserialize<
              Record<string, unknown> | undefined
            >(persistendEvent.metadata),
          }))
          .toSorted((eventA, eventB) => eventA.position - eventB.position)

        onEventsAppended?.(resolvedEvents)

        return resolvedEvents
      } catch (error) {
        if (isPgUniqueViolation(error)) {
          const [lastStreamEvent] = await db()
            .select({
              streamVersion: events.streamVersion,
            })
            .from(events)
            .where(
              and(
                eq(events.streamId, streamId.id),
                eq(events.streamType, streamId.stream),
              ),
            )
            .orderBy(desc(events.position))
            .limit(1)

          const currentVersion = lastStreamEvent?.streamVersion ?? 0
          throw new ConcurrencyError(streamId, expectedVersion, currentVersion)
        }
        throw error
      }
    },
    checkpoint: {
      async upsert(checkpoint) {
        await db()
          .insert(checkpoints)
          .values({
            lastEventPosition: checkpoint.lastEventPosition,
            name: checkpoint.name,
            type: checkpoint.type,
            metadata: superjson.stringify(checkpoint.metadata),
          })
          .onConflictDoUpdate({
            target: [checkpoints.type, checkpoints.name],
            set: {
              lastEventPosition: checkpoint.lastEventPosition,
              metadata: superjson.stringify(checkpoint.metadata),
            },
          })
      },
      async get(type, name) {
        const [checkpoint] = await db()
          .select({
            type: checkpoints.type,
            name: checkpoints.name,
            lastEventPosition: checkpoints.lastEventPosition,
            metadata: checkpoints.metadata,
          })
          .from(checkpoints)
          .where(and(eq(checkpoints.type, type), eq(checkpoints.name, name)))

        if (!checkpoint) {
          return null
        }

        return {
          ...checkpoint,
          metadata: superjson.parse(checkpoint.metadata),
        }
      },
      async delete(type, name) {
        await db()
          .delete(checkpoints)
          .where(and(eq(checkpoints.type, type), eq(checkpoints.name, name)))
      },
    },

    projectionSnapshot: {
      async put(snapshot) {
        await db()
          .insert(projectionSnapshots)
          .values({
            lastEventPosition: snapshot.lastEventPosition,
            projectionId: snapshot.projectionId,
            projectionType: snapshot.projectionType,
            schemaVersion: snapshot.schemaVersion,
            state: superjson.stringify(snapshot.state),
            metadata: superjson.stringify(snapshot.metadata),
          })
      },
      async get(select) {
        const [snapshot] = await db()
          .select({
            projectionId: projectionSnapshots.projectionId,
            projectionType: projectionSnapshots.projectionType,
            state: projectionSnapshots.state,
            lastEventPosition: projectionSnapshots.lastEventPosition,
            schemaVersion: projectionSnapshots.schemaVersion,
            createdAt: projectionSnapshots.createdAt,
            metadata: projectionSnapshots.metadata,
          })
          .from(projectionSnapshots)
          .where(
            and(
              eq(projectionSnapshots.projectionId, select.id),
              eq(projectionSnapshots.projectionType, select.projection),
            ),
          )
          .orderBy(desc(projectionSnapshots.lastEventPosition))

        if (!snapshot) {
          return null
        }

        return {
          ...snapshot,
          state: superjson.parse(snapshot.state),
          metadata: super.parse(snapshot.metadata),
        }
      },
      async delete(target, fromPosition) {
        const conditions = [
          eq(projectionSnapshots.projectionType, target.projection),
        ]

        if (target.id) {
          conditions.push(eq(projectionSnapshots.projectionId, target.id))
        }

        if (fromPosition) {
          conditions.push(
            gt(projectionSnapshots.lastEventPosition, fromPosition),
          )
        }

        await db()
          .delete(projectionSnapshots)
          .where(and(...conditions))
      },
      async incrementAppliedCount(select) {
        const [applied] = await db()
          .insert(projectionAppliedEvents)
          .values({
            projectionId: select.id,
            projectionType: select.projection,
            appliedEvents: 1,
          })
          .onConflictDoUpdate({
            target: [
              projectionAppliedEvents.projectionId,
              projectionAppliedEvents.projectionType,
            ],
            set: {
              appliedEvents: sql`${projectionAppliedEvents.appliedEvents} + 1`,
            },
          })
          .returning({
            appliedEvents: projectionAppliedEvents.appliedEvents,
          })

        return applied.appliedEvents
      },
    },
    aggregateRootSnapshot: {
      async put(snapshot) {
        await db()
          .insert(aggregateRootSnapshots)
          .values({
            lastEventPosition: snapshot.lastEventPosition,
            schemaVersion: snapshot.schemaVersion,
            state: superjson.stringify(snapshot.state),
            streamId: snapshot.streamId,
            streamType: snapshot.streamType,
            streamVersion: snapshot.streamVersion,
            metadata: superjson.stringify(snapshot.metadata),
          })
      },
      async get(select) {
        const [snapshot] = await db()
          .select({
            streamId: aggregateRootSnapshots.streamId,
            streamType: aggregateRootSnapshots.streamType,
            streamVersion: aggregateRootSnapshots.streamVersion,
            schemaVersion: aggregateRootSnapshots.schemaVersion,
            state: aggregateRootSnapshots.state,
            createdAt: aggregateRootSnapshots.createdAt,
            lastEventPosition: aggregateRootSnapshots.lastEventPosition,
            metadata: aggregateRootSnapshots.metadata,
          })
          .from(aggregateRootSnapshots)
          .where(
            and(
              eq(aggregateRootSnapshots.streamId, select.id),
              eq(aggregateRootSnapshots.streamType, select.stream),
            ),
          )
          .orderBy(desc(aggregateRootSnapshots.streamVersion))

        if (!snapshot) {
          return null
        }

        return {
          ...snapshot,
          state: superjson.parse(snapshot.state),
          metadata: superjson.parse(snapshot.metadata),
        }
      },
      async delete(target, fromStreamVersion) {
        await db()
          .delete(aggregateRootSnapshots)
          .where(
            and(
              eq(aggregateRootSnapshots.streamId, target.id),
              eq(aggregateRootSnapshots.streamType, target.stream),
              gt(aggregateRootSnapshots.streamVersion, fromStreamVersion),
            ),
          )
      },
    },
  }) as any
}
