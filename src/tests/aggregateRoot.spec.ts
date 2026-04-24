import { setTimeout } from 'node:timers/promises'

import { describe, expect, it } from 'bun:test'

import { createAggregateRoot } from '../createAggregateRoot.ts'
import { setupEventStore } from './setup.ts'
import {
  CommandError,
  ConcurrencyError,
  EventValidationError,
} from '../errors.ts'
import lock from '../lock.ts'

describe('AggregateRoot', () => {
  it('adds an event', async () => {
    const aggregateRoot = createAggregateRoot('my-aggregate')
      .withInitialState({
        id: null as null | string,
        userName: null as null | string,
      })
      .withEvents(({ z }) => ({
        UserCreated: z.object({ name: z.string() }),
        UserUpdated: z.object({ newName: z.string() }),
      }))
      .withEventHandlers((state) => ({
        onUserCreated(event) {
          state.id = event.streamId
          state.userName = event.payload.name
        },
      }))
      .withCommands((state, event) => ({
        register<T extends string>(name: T) {
          if (state.id !== null) {
            throw new Error('Cannot create already created user')
          }

          return event('UserCreated', { name })
        },
      }))

    const { pseudoEventStore } = setupEventStore(aggregateRoot)
    const root = await aggregateRoot.newStream().register('Torsten').settled()
    expect(pseudoEventStore).toHaveLength(1)
    expect(await root.state()).toMatchObject({
      userName: 'Torsten',
    })
    const { streamId } = pseudoEventStore[0]
    const root2State = await aggregateRoot.loadStream(streamId).state()
    expect(root2State).toMatchObject({
      userName: 'Torsten',
    })

    expect(
      aggregateRoot.loadStream(streamId).register('Other').settled(),
    ).rejects.toThrow(/Cannot create already created user/)
  })

  it('allows commands to execute each other', async () => {
    let subCommandCalled = false

    const aggregateRoot = createAggregateRoot('test')
      .withEvents(({ z }) => ({
        Sub: z.null(),
      }))
      .withCommands((event) => ({
        async sub() {
          subCommandCalled = true
          await setTimeout(20)
          return event('Sub', null)
        },

        parent() {
          return this.sub()
        },
      }))

    const { eventStore } = setupEventStore(aggregateRoot)

    let subEventHandlerCalled = false
    eventStore.createEventListener('test').withEventHandlers({
      onSub() {
        subEventHandlerCalled = true
      },
    })

    await aggregateRoot.newStream().parent().settled()
    expect(subCommandCalled).toBeTrue()
    expect(subEventHandlerCalled).toBeTrue()
  })

  it('allows generator commands to yield* into other generator commands', async () => {
    const aggregateRoot = createAggregateRoot('test')
      .withInitialState({
        a: false,
        b: false,
      })
      .withEvents(({ z }) => ({
        EventA: z.null(),
        EventB: z.null(),
      }))
      .withEventHandlers((state) => ({
        onEventA() {
          state.a = true
        },
        onEventB() {
          state.b = true
        },
      }))
      .withCommands((_state, event) => ({
        async *sub() {
          await setTimeout(20)
          yield event('EventA', null)
          yield event('EventB', null)
        },

        async *parent() {
          yield* this.sub() as any
        },
      }))

    setupEventStore(aggregateRoot)

    const root = await aggregateRoot.newStream().parent().settled()
    const state = await root.state()
    expect(state).toEqual({ a: true, b: true })
  })

  it('makes snapshots', async () => {
    const aggregateRoot = createAggregateRoot('my-aggregate')
      .withInitialState({
        userName: null as null | string,
      })
      .withEvents(({ z }) => ({
        UserCreated: z.object({
          name: z.string(),
        }),
        UserUpdated: z.object({ newName: z.string() }),
      }))
      .withEventHandlers((state) => ({
        onUserCreated: (event) => {
          state.userName = event.payload.name
        },
        onUserUpdated: (event) => {
          state.userName = event.payload.newName
        },
      }))
      .withSnapshots((state, generate, { z }) =>
        generate
          .every(2)
          .schema(
            z.object({
              currentName: z.string().nullable(),
            }),
          )
          .capture(() => ({
            currentName: state.userName,
          }))
          .apply((snapshot) => {
            state.userName = snapshot.state.currentName
          }),
      )
      .withCommands((_state, event) => ({
        register(name: string) {
          return event('UserCreated', {
            name,
          })
        },
        update(newName: string) {
          return event('UserUpdated', {
            newName,
          })
        },
      }))

    const { pseudoAggregateRootSnapshotStore, eventStore } =
      setupEventStore(aggregateRoot)

    eventStore.getAggregateRoot('my-aggregate')

    const root = aggregateRoot
      .newStream()
      .register('Name1')
      .update('Name2')
      .update('Name3')

    expect(await root.state()).toEqual({ userName: 'Name3' })
    await root.settled()
    expect(pseudoAggregateRootSnapshotStore.size).toBe(1)
  })

  it('can yield multiple events', async () => {
    const usersAggregate = createAggregateRoot('users')
      .withInitialState<{ name?: string; age?: number }>({})
      .withEvents(({ z }) => ({
        UserCreated: z.object({
          name: z.string(),
        }),
        UserNameUpdated: z.object({
          name: z.string(),
        }),
        UserAgeUpdated: z.object({
          age: z.int(),
        }),
      }))
      .withEventHandlers((state) => ({
        onUserCreated: (event) => {
          state.name = event.payload.name
        },
        onUserAgeUpdated: (event) => {
          state.age = event.payload.age
        },
        onUserNameUpdated: (event) => {
          state.name = event.payload.name
        },
      }))
      .withCommands((_state, event) => ({
        create(name: string) {
          return event('UserCreated', { name })
        },
        *update(name: string, age: number) {
          yield event('UserNameUpdated', { name })
          yield event('UserAgeUpdated', { age })
        },
      }))
    const { pseudoEventStore } = setupEventStore(usersAggregate)
    const myAggregate = await usersAggregate
      .newStream()
      .create('Torsten')
      .update('Tester', 10)
      .settled()

    expect(pseudoEventStore).toHaveLength(3)
    expect(await myAggregate.state()).toEqual({
      name: 'Tester',
      age: 10,
    })
  })

  it('handles event validation errors', async () => {
    let errorHandlerError: EventValidationError | undefined
    const aggregateRoot = createAggregateRoot('test')
      .withEvents(({ z }) => ({
        Create: z.string().min(10),
      }))
      .withCommands((event) => ({
        create() {
          return event('Create', 'hello').onValidationError(async (error) => {
            await setTimeout(100)
            errorHandlerError = error
          })
        },
      }))

    setupEventStore(aggregateRoot)

    try {
      await aggregateRoot.newStream().create().settled()
      expect.unreachable()
    } catch (error) {
      expect(error).toBeInstanceOf(CommandError)
      expect((error as CommandError<any>).originalError).toBeInstanceOf(
        EventValidationError,
      )
      expect(errorHandlerError).toEqual(
        (error as CommandError<any>).originalError,
      )
    }
  })

  it('throws on state when error in command', async () => {
    const aggregateRoot = createAggregateRoot('user')
      .withEvents(({ z }) => ({
        CreateUser: z.null(),
      }))
      .withCommands((event) => ({
        create() {
          return event('CreateUser', null)
        },
        update() {
          throw new Error('stop')
        },
      }))

    setupEventStore(aggregateRoot)

    expect(aggregateRoot.newStream().create().update().state()).rejects.toThrow(
      'stop',
    )
  })

  it('updates state through chained commands', async () => {
    const aggregateRoot = createAggregateRoot('user')
      .withInitialState({
        alive: false,
      })
      .withEvents(({ z }) => ({
        CreateUser: z.null(),
        KillUser: z.null(),
        ReviveUser: z.null(),
      }))
      .withEventHandlers((state) => ({
        onCreateUser() {
          state.alive = true
        },
        onKillUser() {
          state.alive = false
        },
        onReviveUser() {
          state.alive = true
        },
      }))
      .withCommands((state, event) => ({
        create() {
          if (state.alive) {
            throw new Error('Already alive')
          }
          return event('CreateUser', null)
        },
        kill() {
          if (!state.alive) {
            throw new Error('Already dead')
          }
          return event('KillUser', null)
        },
        revive() {
          if (state.alive) {
            throw new Error('Not killed')
          }
          return event('ReviveUser', null)
        },
      }))

    const { pseudoEventStore } = setupEventStore(aggregateRoot)

    await aggregateRoot.newStream().create().kill().revive().settled()
    expect(pseudoEventStore.length).toBe(3)
  })

  it('resolves state before side effects', async () => {
    const aggregateRoot = createAggregateRoot('test')
      .withInitialState({
        created: false,
      })
      .withEvents(({ z }) => ({
        Create: z.null(),
      }))
      .withEventHandlers((state) => ({
        onCreate() {
          state.created = true
        },
      }))
      .withCommands((_, event) => ({
        create() {
          return event('Create', null)
        },
      }))

    const { eventStore } = setupEventStore(aggregateRoot)
    let eventHandlerCalled = false
    eventStore.createEventListener('test').withGlobalEventHandler(() => {
      eventHandlerCalled = true
    })

    const root = aggregateRoot.newStream().create()
    const state = await root.state()
    expect(state.created).toBeTrue()
    expect(eventHandlerCalled).toBeFalse()
    await root.settled()
    expect(eventHandlerCalled).toBeTrue()
  })

  it('appends events immediately', async () => {
    expect.assertions(1)
    const aggregateRoot = createAggregateRoot('test')
      .withEvents(({ z }) => ({
        Create: z.null(),
        Update: z.null(),
      }))
      .withCommands((event) => ({
        create() {
          return event('Create', null)
        },
        update() {
          return event('Update', null)
        },
      }))

    const { eventStore, pseudoEventStore } = setupEventStore(aggregateRoot)

    eventStore.createProcessManager('a').withEventHandlers({
      onCreate(event) {
        aggregateRoot.loadStream(event.streamId).update()
      },
    })

    eventStore.createProcessManager('b').withEventHandlers({
      onUpdate() {
        expect(pseudoEventStore).toHaveLength(2)
      },
    })

    await aggregateRoot.newStream().create().settled()
  })
})

describe('hotAggregateRoot', () => {
  it('resolves race conditions internally when executing commands', async () => {
    const aggregateRoot = createAggregateRoot('slow')
      .withEvents(({ z }) => ({
        Created: z.null(),
      }))
      .withCommands((event) => ({
        create: () => event('Created'),
      }))

    const { eventStore } = setupEventStore(aggregateRoot)

    let called = 0
    eventStore.createEventListener('test').withEventHandlers({
      onCreated() {
        called += 1
      },
    })

    const rootA = aggregateRoot.loadStream('A').create()
    const rootB = aggregateRoot.loadStream('A').create()

    await Promise.all([rootA.settled(), rootB.settled()])
    expect(called).toBe(2)
  })

  it('resolves race conditions internally when accessing state', async () => {
    const aggregateRoot = createAggregateRoot('slow')
      .withInitialState({
        name: null as string | null,
      })
      .withEvents(({ z }) => ({
        Created: z.string(),
      }))
      .withEventHandlers((state) => ({
        onCreated(event) {
          state.name = event.payload
        },
      }))
      .withCommands((_state, event) => ({
        create: (name: string) => event('Created', name),
      }))

    setupEventStore(aggregateRoot)

    await aggregateRoot.loadStream('A').create('Torsten').settled()

    // if a state call would be blocking, the test would fail due to timeout of 100ms
    await aggregateRoot.loadStream('A').state()
    await aggregateRoot.loadStream('A').state()
  }, 100)

  it('throws on slow execution of commands', async () => {
    const slowAggregateRoot = createAggregateRoot('slow')
      .withEvents(({ z }) => ({
        Created: z.string(),
      }))
      .withCommands((event) => ({
        async create(delay: number, name: string) {
          await setTimeout(delay)
          return event('Created', name)
        },
      }))

    const { eventStore } = setupEventStore(slowAggregateRoot, {
      lock: lock(40), // 40ms
    })

    let called = 0
    let name: string | undefined
    eventStore.createEventListener('test').withEventHandlers({
      onCreated(event) {
        called += 1
        name = event.payload
      },
    })

    expect(
      Promise.all([
        // locks stream:a, lock auto releases after 40ms while still in create command
        slowAggregateRoot.loadStream('a').create(100, 'A').settled(),
        // waits for lock then emits event
        slowAggregateRoot.loadStream('a').create(5, 'B').settled(),
      ]),
    ).rejects.toThrow(ConcurrencyError)

    expect(called).toBe(1)
    expect(name).toBe('B') // 'A' never made it
  })

  it('auto extends locks', async () => {
    const aggregateRoot = createAggregateRoot('slow')
      .withEvents(({ z }) => ({
        Ping: z.null(),
      }))
      .withCommands((event) => ({
        async *ping() {
          // this delays total execution 10*5 = 50ms
          for (let i = 0; i < 10; i++) {
            await setTimeout(5)
            // yielding event will extend the lock to 40 / 2 = 20ms
            yield event('Ping')
          }
        },
      }))

    const { eventStore } = setupEventStore(aggregateRoot, {
      lock: lock(40), // 40ms < 50ms
    })

    let called = 0
    eventStore.createEventListener('test').withEventHandlers({
      onPing() {
        called += 1
      },
    })

    expect(
      Promise.all([
        aggregateRoot.loadStream('a').ping().settled(),
        //aggregateRoot.loadStream('a').ping().settled(),
      ]),
    ).resolves.toHaveLength(1)

    expect(called).toBe(10)
  })
})
