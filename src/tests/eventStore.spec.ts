import { describe, expect, it } from 'bun:test'

import { ConcurrencyError } from '../index.ts'
import { createUserAggregate, setupEventStore } from './setup.ts'

describe('Event Store', () => {
  it('can rebuild all', async () => {
    const usersAggregate = createUserAggregate()

    let rebuilding = false
    const { eventStore, pseudoEventStore } = setupEventStore(usersAggregate)
    const users = new Map<string, { name: string; age?: number }>()
    eventStore
      .createProjection('users')
      .withEventHandlers({
        onUserCreated({ streamId, payload }) {
          users.set(streamId, payload)
        },
        onUserUpdated({ streamId, payload }) {
          users.set(streamId, {
            ...users.get(streamId)!,
            age: rebuilding ? payload.age + 1 : payload.age,
          })
        },
      })
      .withReplay({
        deleteAll() {
          users.clear()
        },
      })

    let knowsUser = false
    eventStore
      .createProcessManager('users')
      .withState({
        knownUsersIds: new Set<string>(),
      })
      .withEventHandlers((state) => ({
        onUserCreated({ streamId }) {
          if (rebuilding) {
            state.knownUsersIds.add(streamId)
          }

          usersAggregate.loadStream(streamId).update(40)
        },
        onUserUpdated({ streamId }) {
          knowsUser = state.knownUsersIds.has(streamId)
        },
      }))

    const userId = crypto.randomUUID()

    await usersAggregate.loadStream(userId).create('Torsten').settled()
    expect(users.get(userId)).toEqual({
      name: 'Torsten',
      age: 40,
    })
    expect(knowsUser).toBeFalse()
    expect(pseudoEventStore.length).toBe(2)

    rebuilding = true
    await eventStore.rebuild()
    expect(pseudoEventStore.length).toBe(2)
    expect(users.get(userId)).toEqual({
      name: 'Torsten',
      age: 41,
    })
    expect(knowsUser).toBeTrue()
  })

  it('does not poison unrelated writes when one append fails', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore, pseudoEventStore } = setupEventStore(usersAggregate)
    await eventStore.init()

    const userA = crypto.randomUUID()
    const userB = crypto.randomUUID()

    // First create succeeds → stream A is now at version 1.
    await usersAggregate.loadStream(userA).create('A').settled()

    // A duplicate create builds a fresh stream at expected version 0, but the
    // store is at version 1 → ConcurrencyError (the Tillhub-duplicate scenario).
    await expect(
      usersAggregate.newStream(userA).create('A duplicate').settled(),
    ).rejects.toBeInstanceOf(ConcurrencyError)

    // An unrelated stream must still be writable. Before the fix, the shared
    // append chain was left in a rejected state by the failure above, so this
    // write re-propagated stream A's stale ConcurrencyError instead of running.
    await usersAggregate.newStream(userB).create('B').settled()

    expect(pseudoEventStore.some((e) => e.streamId === userB)).toBeTrue()
  })

  it('throws on interaction with posponed init', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate, { autoInit: false })

    expect(() =>
      usersAggregate.newStream().create('Torsten').settled(),
    ).toThrow(
      'Cannot emit events because the event store is not initialized. Set autoInit to true, or explicitly call .init() before emitting events.',
    )

    eventStore.init()

    expect(() =>
      usersAggregate.newStream().create('Torsten').settled(),
    ).not.toThrow()
  })
})
