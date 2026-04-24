import { describe, expect, it } from 'bun:test'

import { createUserAggregate, setupEventStore } from './setup.ts'

describe('EventListeners', () => {
  it('listens to events', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const orderOfOperations: string[] = []
    eventStore
      .createEventListener('listener')
      .withEventHandlers({
        onUserCreated(event) {
          orderOfOperations.push(event.type)
        },
        onUserUpdated(event) {
          orderOfOperations.push(event.type)
        },
      })
      .withAfterEffects({
        afterUserCreated(event) {
          orderOfOperations.push('After' + event.type)
        },
      })

    await usersAggregate.newStream().create('Torsten').update(40).settled()
    expect(orderOfOperations).toEqual([
      'UserCreated',
      'UserUpdated',
      'AfterUserCreated',
    ])
  })

  it('listens to global events', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const orderOfOperations: string[] = []
    eventStore
      .createEventListener('listener')
      .withGlobalEventHandler((event) => {
        orderOfOperations.push(event.type)
      })
      .withGlobalAfterEffect((event) => {
        orderOfOperations.push(`After${event.type}`)
      })

    await usersAggregate.newStream().create('Torsten').update(40).settled()
    expect(orderOfOperations).toEqual([
      'UserCreated',
      'UserUpdated',
      'AfterUserCreated',
      'AfterUserUpdated',
    ])
  })
})
