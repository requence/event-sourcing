import { setTimeout } from 'node:timers/promises'

import { z } from 'zod/v4'

import type { AnyAggregateRoot } from '../createAggregateRoot.ts'
import type { Checkpoint } from '../createCheckpointApi.ts'
import {
  type AggregateRootSnapshot,
  ConcurrencyError,
  type Event,
  type ProjectionSnapshot,
  createAggregateRoot,
  createEventStore,
  isStreamEvents,
  isStreamId,
} from '../index.ts'
import type { LockCreator } from '../lock.ts'

export function createUserAggregate() {
  return createAggregateRoot('users')
    .withEvents({
      UserCreated: z.object({
        name: z.string(),
      }),
      UserUpdated: z.object({
        age: z.int(),
      }),
      Test: z.function({
        input: [z.number()],
        output: z.string(),
      }),
    })
    .withCommands((event) => ({
      create(name: string) {
        return event('UserCreated', { name })
      },
      update(age: number) {
        return event('UserUpdated', { age })
      },
    }))
}

export function setupEventStore<const Root extends AnyAggregateRoot>(
  aggregateRoot: Root | Root[],
  options?: { preloadEvents?: Event[]; autoInit?: boolean; lock?: LockCreator },
) {
  const aggregateRoots = Array.isArray(aggregateRoot)
    ? aggregateRoot
    : [aggregateRoot]
  const pseudoEventStore: Event[] = options?.preloadEvents
    ? [...options.preloadEvents]
    : []
  const pseudoCheckpointStore = new Set<Checkpoint>()
  const pseudoAggregateRootSnapshotStore = new Map<
    string,
    AggregateRootSnapshot
  >()
  const pseudoProjectionSnapshotStore = new Set<ProjectionSnapshot>()
  const pseudoProjectionAppliedCount = new Map<string, number>()
  const eventStore = createEventStore({
    aggregateRoots,
    lock: options?.lock,
    autoInit: options?.autoInit,
    appendEvents({ id, stream }, events, expectedVersion) {
      const lastEvent = pseudoEventStore.findLast(
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

      const extendedEvents: Event[] = events.map((event, index) => ({
        ...event,
        createdAt: new Date(),
        position: pseudoEventStore.length + index,
        streamId: id,
        streamType: stream,
        streamVersion: expectedVersion + index + 1,
      }))

      pseudoEventStore.push(...extendedEvents)
      return extendedEvents
    },
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

      const filteredEvents = pseudoEventStore.filter((event) => {
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

      await setTimeout(10)

      for (const event of filteredEvents) {
        yield event
      }
    },

    checkpoint: {
      get(type, name) {
        for (const checkpoint of pseudoCheckpointStore) {
          if (checkpoint.type === type && checkpoint.name === name) {
            return checkpoint
          }
        }
      },
      upsert(checkpoint) {
        let persistedCheckpoint: Checkpoint | undefined
        for (const c of pseudoCheckpointStore) {
          if (c.type === checkpoint.type && c.name === checkpoint.name) {
            persistedCheckpoint = c
            break
          }
        }

        if (persistedCheckpoint) {
          persistedCheckpoint.lastEventPosition = checkpoint.lastEventPosition
        } else {
          pseudoCheckpointStore.add(checkpoint)
        }
      },
      delete(type, name) {
        for (const checkpoint of pseudoCheckpointStore) {
          if (checkpoint.type === type && checkpoint.name === name) {
            pseudoCheckpointStore.delete(checkpoint)
          }
        }
      },
    },
    aggregateRootSnapshot: {
      put(snapshot) {
        pseudoAggregateRootSnapshotStore.set(snapshot.streamId, snapshot)
      },
      get(select) {
        return pseudoAggregateRootSnapshotStore.get(select.id)
      },
      delete(target, fromPosition) {
        for (const [key, value] of pseudoAggregateRootSnapshotStore) {
          if (key === target.id && value.lastEventPosition > fromPosition) {
            pseudoAggregateRootSnapshotStore.delete(key)
          }
        }
      },
    },
    projectionSnapshot: {
      put(snapshot) {
        pseudoProjectionSnapshotStore.add(snapshot)
      },
      get(select) {
        return Array.from(pseudoProjectionSnapshotStore)
          .toReversed()
          .find(
            (snapshot) =>
              snapshot.projectionId === select.id &&
              snapshot.projectionType === select.projection,
          )
      },
      delete(target, fromPosition) {
        for (const snapshot of pseudoProjectionSnapshotStore) {
          if (target.id && snapshot.projectionId !== target.id) {
            continue
          }

          if (snapshot.projectionType !== target.projection) {
            continue
          }

          if (fromPosition && snapshot.lastEventPosition < fromPosition) {
            continue
          }

          pseudoProjectionSnapshotStore.delete(snapshot)
        }
      },
      incrementAppliedCount(select) {
        const key = `${select.projection}-${select.id}`
        const nextCount = (pseudoProjectionAppliedCount.get(key) ?? 0) + 1
        pseudoProjectionAppliedCount.set(key, nextCount)
        return nextCount
      },
    },
  })

  return {
    eventStore,
    pseudoCheckpointStore,
    pseudoEventStore,
    pseudoAggregateRootSnapshotStore,
    pseudoProjectionSnapshotStore,
    pseudoProjectionAppliedCount,
  }
}
