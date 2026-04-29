import { describe, expect, it } from 'bun:test'

import { z } from 'zod/v4'

import type { Checkpoint } from '../createCheckpointApi.ts'
import type { BaseOutputEvent, MaybePromise } from '../utilityTypes.js'
import {
  type Event,
  ConcurrencyError,
  createAggregateRoot,
  createEventStore,
  isStreamEvents,
  isStreamId,
} from '../index.ts'

/**
 * Shared backing store — simulates a real database that multiple
 * event store instances connect to (e.g. Postgres + RabbitMQ).
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

      const extendedEvents: Event[] = newEvents.map((event, index) => ({
        ...event,
        createdAt: new Date(),
        position: events.length + index,
        streamId: id,
        streamType: stream,
        streamVersion: expectedVersion + index + 1,
      }))

      events.push(...extendedEvents)
      return extendedEvents
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
        if (streamTypes?.length && !streamTypes.includes(event.streamType))
          continue
        if (streamIds?.length && !streamIds.includes(event.streamId)) continue
        if (eventTypes?.length && !eventTypes.includes(event.type)) continue
        if (range?.from !== undefined && event.position < range.from) continue
        if (range?.to !== undefined && event.position > range.to) continue
        yield event
      }
    },
    checkpoint: {
      get(type: string, name: string) {
        for (const cp of checkpoints) {
          if (cp.type === type && cp.name === name) return cp
        }
      },
      upsert(checkpoint: Checkpoint) {
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

/**
 * Creates an event store instance that connects to the shared database.
 * Each instance gets its own aggregate root instance (just like a real
 * multi-instance deployment would).
 */
function createInstance(
  sharedDb: ReturnType<typeof createSharedDatabase>,
  opts?: {
    onEventsEmitted?: (events: BaseOutputEvent[]) => MaybePromise<void>
  },
) {
  const aggregate = createAggregateRoot('counters')
    .withEvents({
      CounterIncremented: z.object({ amount: z.number() }),
    })
    .withCommands((event) => ({
      increment(amount: number) {
        return event('CounterIncremented', { amount })
      },
    }))

  const eventStore = createEventStore({
    aggregateRoots: [aggregate],
    autoInit: false,
    appendEvents(...args) {
      const appended = sharedDb.appendEvents(...args)
      // Simulate event distribution (like RabbitMQ fanout)
      opts?.onEventsEmitted?.(appended)
      return appended
    },
    loadEvents: sharedDb.loadEvents.bind(sharedDb),
    checkpoint: sharedDb.checkpoint,
  })

  return { aggregate, eventStore }
}

describe('ProcessManager Multi-Instance', () => {
  it('state does not diverge when two instances process events independently', async () => {
    const sharedDb = createSharedDatabase()

    // --- Instance A ---
    const instanceA = createInstance(sharedDb)
    const pmA = instanceA.eventStore
      .createProcessManager('counter-pm')
      .withState({ total: 0 })
      .withEventHandlers((state) => ({
        async onCounterIncremented({ payload }) {
          state.total += payload.amount
        },
      }))

    await instanceA.eventStore.init()
    await instanceA.eventStore.isReady()

    // --- Instance B (simulates second deployment) ---
    const instanceB = createInstance(sharedDb)
    const pmB = instanceB.eventStore
      .createProcessManager('counter-pm')
      .withState({ total: 0 })
      .withEventHandlers((state) => ({
        async onCounterIncremented({ payload }) {
          state.total += payload.amount
        },
      }))

    await instanceB.eventStore.init()
    await instanceB.eventStore.isReady()

    // Both instances should read the same initial state from the checkpoint
    expect(await pmA.state()).toEqual({ total: 0 })
    expect(await pmB.state()).toEqual({ total: 0 })

    // Instance A processes an event
    await instanceA.aggregate.newStream().increment(10).settled()

    // Instance A reads updated state from checkpoint
    expect(await pmA.state()).toEqual({ total: 10 })

    // Instance B also reads the correct state from the shared checkpoint
    // (state is loaded from DB, not stale memory)
    expect(await pmB.state()).toEqual({ total: 10 })

    // Instance B processes a second event — it loads { total: 10 } from
    // the checkpoint before processing, so the result is correct.
    await instanceB.aggregate.newStream().increment(5).settled()

    const stateBAfterSecondEvent = await pmB.state()
    expect(stateBAfterSecondEvent).toEqual({ total: 15 })

    // The checkpoint in the database reflects the correct cumulative state
    const checkpoint = sharedDb.checkpoint.get('processManager', 'counter-pm')!
    expect(checkpoint.metadata).toEqual({ state: { total: 15 } })

    // Instance A also sees the correct state (loaded from shared checkpoint)
    expect(await pmA.state()).toEqual({ total: 15 })
  })
})
