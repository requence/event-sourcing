import { setTimeout } from 'node:timers/promises'

import { describe, expect, it } from 'bun:test'
import { z } from 'zod/v4'

import { createAggregateRoot } from '../createAggregateRoot.ts'
import { createUserAggregate, setupEventStore } from './setup.ts'
import { ProjectionReplayLoopError } from '../errors.ts'
import { skipReplay } from '../replay.ts'

describe('Projection', () => {
  it('projects', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore, pseudoCheckpointStore } =
      setupEventStore(usersAggregate)

    const users = new Map<string, { name: string; age?: number }>()

    eventStore.createProjection('user').withEventHandlers({
      onUserCreated(event) {
        users.set(event.streamId, { name: event.payload.name })
      },
      onUserUpdated(event) {
        users.get(event.streamId)!.age = event.payload.age
      },
    })

    await usersAggregate.newStream().create('Torsten').settled()
    expect(users.size).toBe(1)
    const id = Array.from(users.keys()).at(0)!
    await usersAggregate.loadStream(id).update(40).settled()

    expect(users.get(id)).toEqual({ name: 'Torsten', age: 40 })
    expect(pseudoCheckpointStore).toHaveLength(1)
    expect(Array.from(pseudoCheckpointStore)[0]).toMatchObject({
      type: 'projection',
      name: 'user',
      lastEventPosition: 1,
    })
    let userCount = 0
    eventStore.createProjection('userCount').withEventHandlers({
      onUserCreated() {
        userCount++
      },
    })

    await eventStore.isReady()

    expect(userCount).toBe(1)
    expect(pseudoCheckpointStore.size).toBe(2)
    expect(Array.from(pseudoCheckpointStore)[1]).toMatchObject({
      type: 'projection',
      name: 'userCount',
      lastEventPosition: 1,
    })
  })

  it('projects multiple versions', async () => {
    const aggregate = createAggregateRoot('users').withEvents(
      ({ version }) => ({
        UserCreated: z.object({
          name: z.string(),
        }),
        UserUpdated: [
          z.object({
            age: z.int(),
          }),
          version(1, z.object({ adult: z.boolean() })),
        ],
      }),
    )

    const streamId = crypto.randomUUID()

    const { eventStore } = setupEventStore(aggregate, {
      preloadEvents: [
        {
          type: 'UserCreated',
          payload: {
            name: 'Torsten',
          },
          createdAt: new Date(),
          position: 0,
          schemaVersion: 0,
          streamId,
          streamType: 'users',
          streamVersion: 0,
        },
        {
          type: 'UserUpdated',
          payload: {
            age: 40,
          },
          createdAt: new Date(),
          position: 1,
          schemaVersion: 0,
          streamId,
          streamType: 'users',
          streamVersion: 1,
        },
        {
          type: 'UserUpdated',
          payload: {
            adult: true,
          },
          createdAt: new Date(),
          position: 2,
          schemaVersion: 1,
          streamId,
          streamType: 'users',
          streamVersion: 3,
        },
      ],
    })

    const users = new Map<
      string,
      { name: string; age?: number; adult?: boolean }
    >()

    eventStore.createProjection('user').withEventHandlers({
      onUserCreated(event) {
        users.set(event.streamId, { name: event.payload.name })
      },
      onUserUpdated(event) {
        if (event.schemaVersion === 0) {
          users.get(event.streamId)!.age = event.payload.age
        } else {
          users.get(event.streamId)!.adult = event.payload.adult
        }
      },
    })

    await eventStore.isReady()

    expect(users.get(streamId)).toEqual({
      name: 'Torsten',
      age: 40,
      adult: true,
    })
  })

  it('projects exclusively', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const users = new Map<string, { name: string; age?: number }>()

    const orderOfOperations: string[] = []
    eventStore.createProjection('user').withEventHandlers({
      async onUserCreated(event) {
        orderOfOperations.push('creating user')
        users.set(event.streamId, { name: event.payload.name })
        await setTimeout(100)
        orderOfOperations.push('created user')
      },
      async onUserUpdated(event) {
        orderOfOperations.push('updating user')
        users.get(event.streamId)!.age = event.payload.age
        await setTimeout(100)
        orderOfOperations.push('updated user')
      },
    })

    await usersAggregate
      .newStream()
      .create('Torsten')
      .update(39)
      .update(40)
      .settled()
    expect(users.size).toBe(1)
    const id = Array.from(users.keys()).at(0)!
    expect(users.get(id)).toEqual({ name: 'Torsten', age: 40 })
    expect(orderOfOperations).toEqual([
      'creating user',
      'created user',
      'updating user',
      'updated user',
      'updating user',
      'updated user',
    ])
  })

  it('projects concurrently', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const users = new Map<string, { name: string; age?: number }>()
    const orderOfOperations: string[] = []
    eventStore
      .createProjection('user')
      .withEventHandlers({
        async onUserCreated(event) {
          orderOfOperations.push('creating user')
          users.set(event.streamId, { name: event.payload.name })
          await setTimeout(100)
          orderOfOperations.push('created user')
        },
        async onUserUpdated(event) {
          orderOfOperations.push('updating user')
          users.get(event.streamId)!.age = event.payload.age
          await setTimeout(100)
          orderOfOperations.push('updated user')
        },
      })
      .concurrent()

    await usersAggregate
      .newStream()
      .create('Torsten')
      .update(39)
      .update(40)
      .settled()
    expect(users.size).toBe(1)
    const id = Array.from(users.keys()).at(0)!
    expect(users.get(id)).toEqual({ name: 'Torsten', age: 40 })
    expect(orderOfOperations.slice(0, 3)).toEqual([
      'creating user',
      'updating user',
      'updating user',
    ])
  })

  it('makes snapshots', async () => {
    const usersAggregate = createUserAggregate()
    const {
      eventStore,
      pseudoProjectionAppliedCount,
      pseudoProjectionSnapshotStore,
    } = setupEventStore(usersAggregate)

    const users = new Map<string, { name: string; age?: number }>()
    let createdHandled = 0
    let updateHandled = 0
    const projection = eventStore
      .createProjection('user')
      .withEventHandlers({
        onUserCreated(event) {
          createdHandled += 1
          users.set(event.streamId, { name: event.payload.name })
        },
        onUserUpdated(event) {
          updateHandled += 1
          users.get(event.streamId)!.age = event.payload.age
        },
      })
      .withSnapshots((s) =>
        s
          .every(2)
          .schema(z.tuple([z.string(), z.int().optional()]))
          .capture((event) => {
            const persistedUser = users.get(event.streamId)
            return persistedUser
              ? [persistedUser.name, persistedUser.age]
              : null
          })
          .apply((snapshot) => {
            users.set(snapshot.projectionId, {
              name: snapshot.state[0],
              age: snapshot.state[1],
            })
          }),
      )
      .withReplay({
        deleteAll() {
          users.clear()
        },
        deleteOne(id) {
          users.delete(id)
        },
      })

    await usersAggregate
      .newStream()
      .create('Torsten')
      .update(30)
      .update(40)
      .settled()
    const id = Array.from(users.keys()).at(0)!

    expect(users.get(id)).toEqual({ name: 'Torsten', age: 40 })
    expect(pseudoProjectionAppliedCount.get(`user-${id}`)).toBe(3)
    expect(pseudoProjectionSnapshotStore.size).toBe(1)
    expect(Array.from(pseudoProjectionSnapshotStore).at(0)).toMatchObject({
      state: ['Torsten', 30],
    })

    await projection.replayUntil(id, 2)
    expect(createdHandled).toBe(1) // only one create bc of replay
    expect(updateHandled).toBe(3) // 3 updates, bc update 1 got snapshotted
  })

  it('throws when aggregateRoot is accessed inside event handler', () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    eventStore.createProjection('test').withEventHandlers({
      onUserCreated() {
        expect(() => usersAggregate.loadStream('x')).toThrow(
          'aggregateRoot.loadStream("x") cannot be called inside a projection. Attempted on aggregate stream users within Projection test.',
        )

        expect(() => usersAggregate.newStream()).toThrow(
          'aggregateRoot.newStream() cannot be called inside a projection. Attempted on aggregate stream users within Projection test.',
        )
      },
    })

    usersAggregate.newStream().create('Torsten')
  })

  it('can rewind', async () => {
    const root = createAggregateRoot('test')
      .withEvents({
        TestCreated: z.object({ name: z.string() }),
        TestUpdated: z.object({ name: z.string() }),
        TestDeleted: z.null(),
        TestRevived: z.null(),
      })
      .withCommands((event) => ({
        create(name: string) {
          return event('TestCreated', { name })
        },
        update(newName: string) {
          return event('TestUpdated', { name: newName })
        },
        delete() {
          return event('TestDeleted')
        },
        revive() {
          return event('TestRevived')
        },
      }))

    const { eventStore, pseudoEventStore } = setupEventStore(root)

    const store = new Map<string, { name: string }>()

    eventStore
      .createProjection('test')
      .withEventHandlers({
        onTestCreated({ streamId, payload }) {
          if (store.has(streamId)) {
            expect.unreachable()
          }
          store.set(streamId, payload)
        },
        onTestUpdated({ streamId, payload }) {
          if (!store.has(streamId)) {
            expect.unreachable()
          }
          store.set(streamId, payload)
        },
        onTestDeleted({ streamId }) {
          if (!store.has(streamId)) {
            expect.unreachable()
          }
          store.delete(streamId)
        },
        async onTestRevived({ streamId }, { replayUntil }) {
          const event = pseudoEventStore.findLast(
            (event) =>
              event.streamId === streamId && event.type === 'TestDeleted',
          )!
          await replayUntil(event.position - 1)
        },
      })
      .withReplay({
        deleteAll() {
          store.clear()
        },
        deleteOne(id) {
          store.delete(id)
        },
      })

    const stream = root.newStream()
    await stream.create('A').update('B').settled()

    expect(store.size).toBe(1)
    expect(store.get(stream.streamId)?.name).toBe('B')

    await stream.delete().settled()
    expect(store.size).toBe(0)

    await stream.revive().settled()
    expect(store.size).toBe(1)
    expect(store.get(stream.streamId)?.name).toBe('B')
  })

  it('throws on rewind cycle', async () => {
    const root = createAggregateRoot('test')
      .withEvents({
        TestCreated: z.null(),
        TestUpdated: z.object({ name: z.string() }),
      })
      .withCommands((event) => ({
        *create(name: string) {
          yield event('TestCreated')
          yield event('TestUpdated', { name })
        },
      }))

    const { eventStore } = setupEventStore(root)

    eventStore.createProjection('test').withEventHandlers({
      async onTestUpdated({ position }, { replayUntil }) {
        await replayUntil(position)
      },
    })

    const stream = root.newStream()
    expect(stream.create('A').settled()).rejects.toThrow(
      ProjectionReplayLoopError,
    )
  })
  it('can be replay all after init', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const orderOfOperations: string[] = []
    const testProjection = eventStore
      .createProjection('test')
      .withEventHandlers({
        onUserCreated() {
          orderOfOperations.push('User created')
        },
        onUserUpdated() {
          orderOfOperations.push('User updated')
        },
      })
      .withReplay({
        deleteAll() {
          orderOfOperations.push('User deleted')
        },
      })

    await usersAggregate.newStream().create('Torsten').update(40).settled()
    await setTimeout(50)

    await testProjection.replay()
    expect(orderOfOperations).toEqual([
      'User created',
      'User updated',
      'User deleted',
      'User created',
      'User updated',
    ])
  })

  it('can be replay one after init', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const orderOfOperations: string[] = []
    const testProjection = eventStore
      .createProjection('test')
      .withEventHandlers({
        onUserCreated({ streamId }) {
          orderOfOperations.push(`User created ${streamId}`)
        },
        onUserUpdated({ streamId }) {
          orderOfOperations.push(`User updated ${streamId}`)
        },
      })
      .withReplay({
        deleteOne(id) {
          orderOfOperations.push(`User deleted ${id}`)
        },
      })

    await usersAggregate
      .newStream('my-id')
      .create('Torsten')
      .update(40)
      .settled()
    await setTimeout(50)

    await testProjection.replayOne('my-id')
    expect(orderOfOperations).toEqual([
      'User created my-id',
      'User updated my-id',
      'User deleted my-id',
      'User created my-id',
      'User updated my-id',
    ])
  })
  it('can skip execution while replaying', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    let called = 0
    eventStore
      .createProjection('test')
      .withEventHandlers({
        onUserCreated() {
          skipReplay()
          called += 1
        },
      })
      .withReplay({
        deleteAll() {},
      })

    await usersAggregate.newStream().create('Torsten').settled()
    expect(called).toBe(1)

    await eventStore.rebuild()
    expect(called).toBe(1)
  })
})
