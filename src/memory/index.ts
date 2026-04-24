import type { AnyAggregateRoot } from '../createAggregateRoot.ts'
import type { Checkpoint } from '../createCheckpointApi.ts'
import {
  type AggregateRootSnapshot,
  ConcurrencyError,
  type Event,
  type ProjectionSnapshot,
  createEventStore as createBaseEventStore,
  isStreamEvents,
  isStreamId,
} from '../index.ts'
import type {
  EventStore,
  EventStoreParamsWithAggregateRootSnapshots,
  EventsFromRoot,
  OnProgress,
} from '../createEventStore.ts'
import type { BaseOutputEvent, MaybePromise } from '../utilityTypes.js'

export { createAggregateRoot } from '../index.ts'

export type OnEventsAppended = (
  events: BaseOutputEvent[],
) => MaybePromise<void>

interface MemoryEventStoreParams<Root extends AnyAggregateRoot> {
  aggregateRoots: Root[]
  autoInit?: boolean
  postProcessEvent?: EventStoreParamsWithAggregateRootSnapshots<Root>['postProcessEvent']
  onProjectionReplay?: OnProgress
  onProcessManagerRefresh?: OnProgress
  onEventsAppended?: OnEventsAppended
}

export function createEventStore<const Root extends AnyAggregateRoot>({
  aggregateRoots,
  postProcessEvent,
  onProjectionReplay,
  onProcessManagerRefresh,
  onEventsAppended,
  autoInit,
}: MemoryEventStoreParams<Root>): EventStore<
  EventsFromRoot<Root>,
  Root,
  true
> {
  const events: Event[] = []
  const checkpoints = new Set<Checkpoint>()
  const aggregateRootSnapshots = new Map<string, AggregateRootSnapshot>()
  const projectionSnapshots = new Set<ProjectionSnapshot>()
  const projectionAppliedCount = new Map<string, number>()

  return createBaseEventStore({
    aggregateRoots,
    postProcessEvent,
    onProjectionReplay,
    onProcessManagerRefresh,
    autoInit,

    async *loadEvents(select, range) {
      let streamTypes: string[] | null = null
      let streamIds: string[] | null = null
      let eventTypes: string[] | null = null

      if (select) {
        const selects = Array.isArray(select) ? select : [select]
        streamTypes = selects.map((select) => select.stream)
        streamIds = selects
          .map((select) => (isStreamId(select) ? select.id : null))
          .filter(Boolean) as string[]
        eventTypes = selects.flatMap((select) =>
          isStreamEvents(select) ? select.events : [],
        )
      }

      const filteredEvents = events.filter((event) => {
        if (streamTypes?.length && !streamTypes.includes(event.streamType)) {
          return false
        }

        if (streamIds?.length && !streamIds.includes(event.streamId)) {
          return false
        }

        if (eventTypes?.length && !eventTypes.includes(event.type)) {
          return false
        }

        if (range?.from && event.position < range.from) {
          return false
        }

        if (range?.to && event.position > range.to) {
          return false
        }

        return true
      })

      for (const event of filteredEvents) {
        yield event
      }
    },

    appendEvents({ id, stream }, newEvents, expectedVersion) {
      const lastEvent = events.findLast(
        (e) => e.streamId === id && e.streamType === stream,
      )

      const currentVersion = lastEvent?.streamVersion ?? 0

      if (currentVersion !== expectedVersion) {
        throw new ConcurrencyError(
          { id, stream },
          expectedVersion,
          currentVersion,
        )
      }

      const extendedEvents: Event[] = newEvents.map((event, index) => ({
        ...event,
        createdAt: new Date(),
        position: events.length + index,
        streamId: id,
        streamType: stream,
        streamVersion: expectedVersion + index + 1,
      }))

      events.push(...extendedEvents)
      onEventsAppended?.(extendedEvents)
      return extendedEvents
    },

    checkpoint: {
      get(type, name) {
        for (const checkpoint of checkpoints) {
          if (checkpoint.type === type && checkpoint.name === name) {
            return checkpoint
          }
        }
      },
      upsert(checkpoint) {
        let persisted: Checkpoint | undefined
        for (const c of checkpoints) {
          if (c.type === checkpoint.type && c.name === checkpoint.name) {
            persisted = c
            break
          }
        }

        if (persisted) {
          persisted.lastEventPosition = checkpoint.lastEventPosition
          persisted.metadata = checkpoint.metadata
        } else {
          checkpoints.add(checkpoint)
        }
      },
      delete(type, name) {
        for (const checkpoint of checkpoints) {
          if (checkpoint.type === type && checkpoint.name === name) {
            checkpoints.delete(checkpoint)
          }
        }
      },
    },

    projectionSnapshot: {
      put(snapshot) {
        projectionSnapshots.add(snapshot)
      },
      get(select) {
        return Array.from(projectionSnapshots)
          .toReversed()
          .find(
            (snapshot) =>
              snapshot.projectionId === select.id &&
              snapshot.projectionType === select.projection,
          )
      },
      delete(target, fromPosition) {
        for (const snapshot of projectionSnapshots) {
          if (target.id && snapshot.projectionId !== target.id) {
            continue
          }

          if (snapshot.projectionType !== target.projection) {
            continue
          }

          if (fromPosition && snapshot.lastEventPosition < fromPosition) {
            continue
          }

          projectionSnapshots.delete(snapshot)
        }
      },
      incrementAppliedCount(select) {
        const key = `${select.projection}-${select.id}`
        const nextCount = (projectionAppliedCount.get(key) ?? 0) + 1
        projectionAppliedCount.set(key, nextCount)
        return nextCount
      },
    },

    aggregateRootSnapshot: {
      put(snapshot) {
        aggregateRootSnapshots.set(
          `${snapshot.streamType}:${snapshot.streamId}`,
          snapshot,
        )
      },
      get(select) {
        return aggregateRootSnapshots.get(`${select.stream}:${select.id}`)
      },
      delete(target, fromStreamVersion) {
        const key = `${target.stream}:${target.id}`
        const snapshot = aggregateRootSnapshots.get(key)
        if (snapshot && snapshot.streamVersion > fromStreamVersion) {
          aggregateRootSnapshots.delete(key)
        }
      },
    },
  }) as any
}
