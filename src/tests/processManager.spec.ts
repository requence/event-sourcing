import { AsyncLocalStorage } from 'node:async_hooks'
import { setTimeout } from 'node:timers/promises'

import { describe, expect, it } from 'bun:test'

import { createAggregateRoot } from '../createAggregateRoot.ts'
import { createUserAggregate, setupEventStore } from './setup.ts'
import lock from '../lock.ts'
import { skipRefreshing } from '../refresh.ts'

describe('ProcessManager', () => {
  it('processes events', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore, pseudoCheckpointStore } =
      setupEventStore(usersAggregate)

    let called = false
    eventStore.createProcessManager('my-process-manager').withEventHandlers({
      async onUserCreated() {
        called = true
      },
    })

    await usersAggregate.newStream().create('Torsten').settled()
    expect(Array.from(pseudoCheckpointStore)[0]).toMatchObject({
      lastEventPosition: 0,
      name: 'my-process-manager',
      type: 'processManager',
    })

    expect(called).toBe(true)
  })

  it('processes events with state', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore, pseudoCheckpointStore } =
      setupEventStore(usersAggregate)

    let called = false
    eventStore
      .createProcessManager('my-process-manager')
      .withState({
        usersCreated: 0,
      })
      .withEventHandlers((state) => ({
        async onUserCreated() {
          state.usersCreated += 1
          called = true
        },
      }))

    await usersAggregate.newStream().create('Torsten').settled()

    expect(Array.from(pseudoCheckpointStore)[0]).toMatchObject({
      lastEventPosition: 0,
      name: 'my-process-manager',
      type: 'processManager',
      metadata: { state: { usersCreated: 1 } },
    })

    expect(called).toBe(true)
  })

  it('can execute after effect', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const orderOfOperations: string[] = []
    eventStore
      .createProcessManager('userA')
      .withEventHandlers({
        onUserCreated() {
          orderOfOperations.push('created-a')
        },
      })
      .withAfterEffects({
        afterUserCreated() {
          orderOfOperations.push('after-a')
        },
      })

    eventStore.createProcessManager('userB').withEventHandlers({
      onUserCreated() {
        orderOfOperations.push('created-b')
      },
    })

    await usersAggregate.newStream().create('Torsten').settled()

    expect(orderOfOperations).toEqual(['created-a', 'created-b', 'after-a'])
  })

  it('throws when settled() is called inside event handler', () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    eventStore.createProcessManager('test').withEventHandlers({
      onUserCreated({ streamId }) {
        expect(() => usersAggregate.loadStream(streamId).settled()).toThrow(
          'aggregateRoot.settled() cannot be called inside a process manager',
        )
      },
    })

    usersAggregate.newStream().create('Torsten')
  })

  it('detects infinite loops', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    eventStore.createProcessManager('test').withEventHandlers({
      onUserUpdated({ streamId }) {
        usersAggregate.loadStream(streamId).update(41)
      },
    })

    expect(
      usersAggregate.newStream().create('Torsten').update(40).settled(),
    ).rejects.toThrow(
      /^Infinite loop detected. Process Manager "test" listens and emits same event "UserUpdated" for stream users:(.+), causing a recurring event cycle$/,
    )
  })

  it('can be refreshed', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    let increment = 1
    const processManager = eventStore
      .createProcessManager('test')
      .withState({
        executed: 0,
      })
      .withEventHandlers((state) => ({
        onUserCreated() {
          state.executed += increment
        },
        onUserUpdated() {
          state.executed += increment
        },
      }))

    await usersAggregate.newStream().create('Torsten').update(40).settled()
    expect(processManager.state()).resolves.toEqual({ executed: 2 })
    increment = 2
    await processManager.refreshState()
    expect(processManager.state()).resolves.toEqual({ executed: 4 })
  })

  it('will not refresh without state', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    let called = 0
    eventStore.createProcessManager('test').withEventHandlers({
      onUserCreated() {
        called += 1
      },
    })

    await usersAggregate.newStream().create('Torsten').settled()
    expect(called).toBe(1)

    await eventStore.rebuild()
    expect(called).toBe(1)
  })

  it('can skip execution while refreshing', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    let called = 0
    eventStore
      .createProcessManager('test')
      .withState({})
      .withEventHandlers((_state) => ({
        onUserCreated() {
          skipRefreshing()
          called += 1
        },
      }))

    await usersAggregate.newStream().create('Torsten').settled()
    expect(called).toBe(1)

    await eventStore.rebuild()
    expect(called).toBe(1)
  })

  it('applies settled defaults from options to dispatched streams', async () => {
    const context = new AsyncLocalStorage<string>()

    const sourceAggregate = createAggregateRoot('sources')
      .withEvents(({ z }) => ({
        SourceCreated: z.string(),
      }))
      .withCommands((event) => ({
        create(name: string) {
          return event('SourceCreated', name)
        },
      }))

    const targetAggregate = createAggregateRoot('targets')
      .withEvents(({ z }) => ({
        Written: z.string(),
      }))
      .withCommands((event) => ({
        async write(delay: number, name: string) {
          await setTimeout(delay)
          return event('Written', `${name}:${context.getStore() ?? 'none'}`)
        },
      }))

    const { eventStore, pseudoEventStore } = setupEventStore(
      [sourceAggregate, targetAggregate],
      { lock: lock(40) }, // 40ms
    )

    eventStore
      .createProcessManager('cascade', { settled: { maxRetries: 3 } })
      .withEventHandlers({
        onSourceCreated() {
          context.run('pm', () => {
            targetAggregate.loadStream('t').write(100, 'PM')
          })
        },
      })

    // The process manager's write holds the lock on targets:t, but its 100ms
    // command outlives the 40ms lock TTL, so the direct write slips in and
    // advances the stream to v1. The process manager's append then loses the
    // race — without the settled defaults declared above the ConcurrencyError
    // would surface through the source stream's settled(); with them the
    // auto-settle reloads and re-applies the command.
    await Promise.all([
      sourceAggregate.newStream().create('s').settled(),
      (async () => {
        await setTimeout(30)
        await targetAggregate.loadStream('t').write(5, 'direct').settled()
      })(),
    ])

    const persisted = pseudoEventStore.filter(
      (event) => event.streamType === 'targets',
    )
    // 'PM:pm' also proves the retry re-ran the command inside the dispatch
    // site's async context — the AsyncLocalStorage value survived the retry.
    expect(persisted.map((event) => event.payload)).toEqual([
      'direct:none',
      'PM:pm',
    ])
    expect(persisted.map((event) => event.streamVersion)).toEqual([1, 2])
  })
})
