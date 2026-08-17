import { describe, expect, it } from 'bun:test'

import { z } from 'zod/v4'

import type { Checkpoint } from '../createCheckpointApi.ts'
import type { BaseOutputEvent } from '../utilityTypes.js'
import {
  ConcurrencyError,
  type Event,
  createAggregateRoot,
  createEventStore,
  isStreamEvents,
  isStreamId,
} from '../index.ts'

/**
 * Two event store instances against one backing store — the shape of a
 * horizontally-scaled deployment, where every instance runs its own copy of
 * every projection but they all write to the same projection tables.
 *
 * A projection decides what it has already applied from its own in-memory
 * `lastEventPosition`, which cannot see a peer. So when two instances catch up
 * over the same backlog, or one catches up while the other applies events it
 * just appended, the same event reaches both handlers. These tests pin down that
 * each event's handler runs exactly once across the cluster.
 */
function createSharedDatabase() {
  const events: Event[] = []
  const checkpoints = new Set<Checkpoint>()

  return {
    events,
    checkpoints,

    appendEvents(
      { id, stream }: { id: string; stream: string },
      newEvents: any[],
      expectedVersion: number,
    ) {
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

      const appended: Event[] = newEvents.map((event, index) => ({
        ...event,
        createdAt: new Date(),
        position: events.length + index,
        streamId: id,
        streamType: stream,
        streamVersion: expectedVersion + index + 1,
      }))

      events.push(...appended)
      return appended
    },

    async *loadEvents(
      select: any,
      range?: { from?: number; to?: number },
    ): AsyncGenerator<BaseOutputEvent> {
      let streamTypes: string[] | null = null
      let streamIds: string[] | null = null
      let eventTypes: string[] | null = null

      if (select) {
        const selects = Array.isArray(select) ? select : [select]
        streamTypes = selects.map((s: any) => s.stream)
        streamIds = selects
          .map((s: any) => (isStreamId(s) ? s.id : null))
          .filter(Boolean) as string[]
        eventTypes = selects.flatMap((s: any) =>
          isStreamEvents(s) ? s.events : [],
        )
      }

      for (const event of events) {
        if (streamTypes?.length && !streamTypes.includes(event.streamType)) {
          continue
        }
        if (streamIds?.length && !streamIds.includes(event.streamId)) {
          continue
        }
        if (eventTypes?.length && !eventTypes.includes(event.type)) {
          continue
        }
        if (range?.from !== undefined && event.position < range.from) {
          continue
        }
        if (range?.to !== undefined && event.position > range.to) {
          continue
        }
        yield event
      }
    },

    checkpoint: {
      get(type: string, name: string) {
        for (const cp of checkpoints) {
          if (cp.type === type && cp.name === name) {
            return cp
          }
        }
      },
      upsert(checkpoint: Checkpoint, expectedVersion: number | null) {
        let persisted: Checkpoint | undefined
        for (const c of checkpoints) {
          if (c.type === checkpoint.type && c.name === checkpoint.name) {
            persisted = c
            break
          }
        }

        if (persisted) {
          // The optimistic-concurrency guard a real driver implements with
          // `INSERT … ON CONFLICT DO UPDATE … WHERE version = expected`.
          if (
            expectedVersion === null ||
            persisted.version !== expectedVersion
          ) {
            return false
          }
          persisted.lastEventPosition = checkpoint.lastEventPosition
          persisted.metadata = checkpoint.metadata
          persisted.version = checkpoint.version
        } else {
          if (expectedVersion !== null) {
            return false
          }
          checkpoints.add({ ...checkpoint })
        }

        return true
      },
      delete(type: string, name: string) {
        for (const cp of checkpoints) {
          if (cp.type === type && cp.name === name) {
            checkpoints.delete(cp)
          }
        }
      },
    },
  }
}

function counterAggregate() {
  return createAggregateRoot('counters')
    .withEvents({ CounterIncremented: z.object({ amount: z.number() }) })
    .withCommands((event) => ({
      increment(amount: number) {
        return event('CounterIncremented', { amount })
      },
    }))
}

function createInstance(sharedDb: ReturnType<typeof createSharedDatabase>) {
  const aggregate = counterAggregate()

  const eventStore = createEventStore({
    aggregateRoots: [aggregate],
    autoInit: false,
    appendEvents: sharedDb.appendEvents.bind(sharedDb),
    loadEvents: sharedDb.loadEvents.bind(sharedDb),
    checkpoint: sharedDb.checkpoint,
  })

  return { aggregate, eventStore }
}

/** Puts a backlog of events in the store with no projection to consume them. */
async function seedBacklog(
  sharedDb: ReturnType<typeof createSharedDatabase>,
  count: number,
) {
  const seeder = createInstance(sharedDb)
  await seeder.eventStore.init()
  await seeder.eventStore.isReady()

  const stream = seeder.aggregate.newStream()
  for (let i = 0; i < count; i++) {
    stream.increment(1)
  }
  await stream.settled()
}

describe('Projection multi-instance', () => {
  it('applies each backlogged event once when two instances catch up at the same time', async () => {
    const sharedDb = createSharedDatabase()
    await seedBacklog(sharedDb, 5)

    // Stands in for the shared projection table both instances write to.
    const applied: number[] = []

    const instances = [createInstance(sharedDb), createInstance(sharedDb)]
    for (const instance of instances) {
      instance.eventStore.createProjection('counter').withEventHandlers({
        async onCounterIncremented({ position }) {
          applied.push(position)
        },
      })
    }

    await Promise.all(instances.map((i) => i.eventStore.init()))
    await Promise.all(instances.map((i) => i.eventStore.isReady()))

    expect(applied.toSorted((a, b) => a - b)).toEqual([0, 1, 2, 3, 4])
  })

  it('does not re-apply an event to a handler that cannot take it twice', async () => {
    const sharedDb = createSharedDatabase()
    await seedBacklog(sharedDb, 5)

    // A primary key, in miniature: the second insert of a row is an error, which
    // is what kills a real instance mid-hydration.
    const rows = new Set<number>()
    const insert = (position: number) => {
      if (rows.has(position)) {
        throw new Error(`duplicate key: position ${position} already applied`)
      }
      rows.add(position)
    }

    const instances = [createInstance(sharedDb), createInstance(sharedDb)]
    for (const instance of instances) {
      instance.eventStore.createProjection('counter').withEventHandlers({
        async onCounterIncremented({ position }) {
          insert(position)
        },
      })
    }

    await Promise.all(instances.map((i) => i.eventStore.init()))
    await Promise.all(instances.map((i) => i.eventStore.isReady()))

    expect([...rows].toSorted((a, b) => a - b)).toEqual([0, 1, 2, 3, 4])
  })

  it('leaves an event to be retried when its handler fails', async () => {
    // Taking ownership of an event before running its handler must not turn a
    // failed handler into a silently skipped event: the checkpoint has to end up
    // back where it was so a later catch-up picks the event up again.
    const sharedDb = createSharedDatabase()
    await seedBacklog(sharedDb, 3)

    let failing = true
    const applied: number[] = []

    const first = createInstance(sharedDb)
    first.eventStore.createProjection('counter').withEventHandlers({
      async onCounterIncremented({ position }) {
        if (failing && position === 1) {
          throw new Error('handler blew up')
        }
        applied.push(position)
      },
    })

    await expect(first.eventStore.init()).rejects.toThrow('handler blew up')

    failing = false
    const second = createInstance(sharedDb)
    second.eventStore.createProjection('counter').withEventHandlers({
      async onCounterIncremented({ position }) {
        applied.push(position)
      },
    })

    await second.eventStore.init()
    await second.eventStore.isReady()

    // Event 1 has to have been applied by the second instance, and events it
    // never reached must not be lost either.
    expect(applied.toSorted((a, b) => a - b)).toEqual([0, 1, 2])
  })
})
