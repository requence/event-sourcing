import { AsyncLocalStorage } from 'node:async_hooks'
import { setTimeout } from 'node:timers/promises'

import { describe, expect, it } from 'bun:test'
import { z } from 'zod/v4'

import { createAggregateRoot } from '../createAggregateRoot.ts'
import { createUserAggregate, setupEventStore } from './setup.ts'
import { CommandError } from '../errors.ts'
import lock from '../lock.ts'

describe('transaction', () => {
  const createAggregateRoots = () => {
    const aggA = createAggregateRoot('A')
      .withEvents({
        ACreated: z.object({
          name: z.string(),
        }),
      })
      .withCommands((event) => ({
        async create(name: string) {
          await setTimeout(200)
          return event('ACreated', { name })
        },
      }))

    const aggB = createAggregateRoot('B')
      .withEvents({
        BCreated: z.object({
          name: z.string(),
        }),
      })
      .withCommands((event) => ({
        create(name: string) {
          return event('BCreated', { name })
        },
        async update() {
          await setTimeout(100)
          throw new Error('Cannot update')
        },
      }))

    return { aggA, aggB }
  }

  it('can commit events', async () => {
    const { aggA, aggB } = createAggregateRoots()
    const { eventStore, pseudoEventStore } = setupEventStore([aggA, aggB])

    let aCreated = 0
    let bCreated = 0

    eventStore.createProcessManager('test').withEventHandlers({
      onACreated() {
        aCreated += 1
      },
      onBCreated() {
        bCreated += 1
      },
    })

    await eventStore.transaction(() => {
      aggA.newStream().create('valueA')
      aggB.newStream().create('valueB')
    })

    expect(pseudoEventStore.length).toBe(2)
    expect(aCreated).toBe(1)
    expect(bCreated).toBe(1)
  })

  it('does not commit on error inside transaction block', async () => {
    const { aggA, aggB } = createAggregateRoots()
    const { eventStore, pseudoEventStore } = setupEventStore([aggA, aggB])

    expect(
      eventStore.transaction(() => {
        aggA.newStream().create('valueA')
        aggB.newStream().create('valueB')
        throw new Error('stop')
      }),
    ).rejects.toThrow('stop')

    expect(pseudoEventStore.length).toBe(0)
  })

  it('does not commit on error in command', async () => {
    const { aggA, aggB } = createAggregateRoots()
    const { eventStore, pseudoEventStore } = setupEventStore([aggA, aggB])

    expect(
      eventStore.transaction(() => {
        aggA.newStream().create('valueA')
        aggB.newStream().create('valueB').update()
      }),
    ).rejects.toThrow(CommandError)

    expect(pseudoEventStore.length).toBe(0)
  })

  it('awaits multiple commands', async () => {
    const userAggregate = createUserAggregate()
    const { eventStore, pseudoEventStore } = setupEventStore(userAggregate)

    await eventStore.transaction(async () => {
      userAggregate.newStream().create('Torsten').update(40)
    })

    expect(pseudoEventStore.length).toBe(2)
  })

  it('keeps order of created events', async () => {
    const aggA = createAggregateRoot('a')
      .withEvents({
        Created: z.literal('a'),
      })
      .withCommands((event) => ({
        create: () => event('Created', 'a'),
      }))

    const aggB = createAggregateRoot('a')
      .withEvents({
        Created: z.literal('b'),
        Updated: z.literal('b'),
      })
      .withCommands((event) => ({
        create: () => event('Created', 'b'),
        update: () => event('Updated', 'b'),
      }))

    const { eventStore, pseudoEventStore } = setupEventStore([aggA, aggB])

    await eventStore.transaction(async () => {
      aggA.newStream('a1').create()
      aggB.newStream('b1').create().update()
      aggA.newStream('a2').create()
    })

    expect(pseudoEventStore.at(0)).toMatchObject({
      type: 'Created',
      payload: 'a',
      streamId: 'a1',
    })
    expect(pseudoEventStore.at(1)).toMatchObject({
      type: 'Created',
      payload: 'b',
      streamId: 'b1',
    })
    expect(pseudoEventStore.at(2)).toMatchObject({
      type: 'Updated',
      payload: 'b',
      streamId: 'b1',
    })
    expect(pseudoEventStore.at(3)).toMatchObject({
      type: 'Created',
      payload: 'a',
      streamId: 'a2',
    })
  })

  it('can access state inside transaction', async () => {
    expect.assertions(1)

    const aggregateRoot = createAggregateRoot('test')
      .withInitialState({
        created: false,
      })
      .withEvents({
        Created: z.null(),
      })
      .withEventHandlers((state) => ({
        onCreated() {
          state.created = true
        },
      }))
      .withCommands((_, event) => ({
        create() {
          return event('Created', null)
        },
      }))
    const { eventStore } = setupEventStore(aggregateRoot)
    await eventStore.transaction(async () => {
      const state = await aggregateRoot.newStream().create().state()
      expect(state.created).toBeTrue()
    })
  })

  it('applies settled defaults from options to dispatched streams', async () => {
    const context = new AsyncLocalStorage<string>()

    const slowAggregate = createAggregateRoot('slow')
      .withEvents({
        Written: z.string(),
      })
      .withCommands((event) => ({
        async write(delay: number, name: string) {
          await setTimeout(delay)
          return event('Written', `${name}:${context.getStore() ?? 'none'}`)
        },
      }))

    const { eventStore, pseudoEventStore } = setupEventStore(slowAggregate, {
      lock: lock(40), // 40ms
    })

    // The transaction's write holds the lock on slow:a, but its 100ms command
    // outlives the 40ms lock TTL, so the direct write slips in and advances
    // the stream to v1. The transaction's append then loses the race — without
    // the settled defaults declared below the ConcurrencyError would surface
    // from the transaction; with them the auto-settle reloads and re-applies
    // the command.
    await Promise.all([
      eventStore.transaction(
        async () => {
          context.run('tx', () => {
            slowAggregate.loadStream('a').write(100, 'TX')
          })
        },
        { settled: { maxRetries: 3 } },
      ),
      (async () => {
        await setTimeout(30)
        await slowAggregate.loadStream('a').write(5, 'direct').settled()
      })(),
    ])

    const persisted = pseudoEventStore.filter(
      (event) => event.streamType === 'slow',
    )
    // 'TX:tx' also proves the retry re-ran the command inside the dispatch
    // site's async context — the AsyncLocalStorage value survived the retry.
    expect(persisted.map((event) => event.payload)).toEqual([
      'direct:none',
      'TX:tx',
    ])
    expect(persisted.map((event) => event.streamVersion)).toEqual([1, 2])
  })
})
