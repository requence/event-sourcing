import { setTimeout } from 'node:timers/promises'

import { describe, expect, it } from 'bun:test'
import { z } from 'zod/v4'

import { createUserAggregate, setupEventStore } from './setup.ts'
import { createAggregateRoot } from '../createAggregateRoot.ts'

describe('Projection & ProcessManager', () => {
  it('is settled when all chained events are handled', async () => {
    const usersAggregate = createUserAggregate()
    const { eventStore } = setupEventStore(usersAggregate)

    const users = new Map<string, { name: string; age?: number }>()
    eventStore.createProjection('user').withEventHandlers({
      onUserCreated({ streamId, payload }) {
        if (users.size > 0) {
          throw new Error('wtf')
        }
        users.set(streamId, { name: payload.name })
      },
      async onUserUpdated({ streamId, payload }) {
        await setTimeout(50)
        users.get(streamId)!.age = payload.age
      },
    })

    eventStore.createProcessManager('user').withEventHandlers({
      async onUserCreated({ streamId }) {
        await setTimeout(50)
        usersAggregate.loadStream(streamId).update(40)
      },
    })

    await usersAggregate.newStream().create('Torsten').update(30).settled()

    expect(users.size).toBe(1)
    expect(Array.from(users.values()).at(0)).toEqual({
      name: 'Torsten',
      age: 40,
    })
  })

  it('is settled when all deeply chained events are handled', async () => {
    const groupAggregate = createAggregateRoot('group')
      .withInitialState({
        id: null as string | null,
      })
      .withEvents({
        GroupCreated: z.object({
          name: z.string(),
        }),
        GroupDeleted: z.null(),
      })
      .withEventHandlers((state) => ({
        onGroupCreated({ streamId }) {
          state.id = streamId
        },
      }))
      .withCommands((_state, event) => ({
        create(name: string) {
          return event('GroupCreated', { name })
        },
        delete() {
          return event('GroupDeleted', null)
        },
      }))

    const projectAggregate = createAggregateRoot('project')
      .withEvents({
        ProjectCreated: z.object({
          name: z.string(),
          groupId: z.uuid(),
        }),
        ProjectDeleted: z.null(),
      })
      .withCommands((event) => ({
        create(name: string, groupId: string) {
          return event('ProjectCreated', { name, groupId })
        },
        delete() {
          return event('ProjectDeleted', null)
        },
      }))

    const { eventStore } = setupEventStore([groupAggregate, projectAggregate])

    const groups = new Map<string, string>()
    eventStore.createProjection('group').withEventHandlers({
      onGroupCreated({ streamId: groupId, payload }) {
        groups.set(groupId, payload.name)
      },
      onGroupDeleted({ streamId: groupId }) {
        groups.delete(groupId)
      },
    })

    const projects = new Map<string, string>()
    eventStore.createProjection('project').withEventHandlers({
      onProjectCreated({ streamId: projectId, payload }) {
        projects.set(projectId, payload.name)
      },
      onProjectDeleted({ streamId: projectId }) {
        projects.delete(projectId)
      },
    })

    eventStore
      .createProcessManager('cascade')
      .withState({
        projectsByGroup: new Map<string, Set<string>>(),
      })
      .withEventHandlers((state) => ({
        async onProjectCreated({ streamId: projectId, payload: { groupId } }) {
          await setTimeout(20)
          if (!state.projectsByGroup.has(groupId)) {
            state.projectsByGroup.set(groupId, new Set())
          }
          state.projectsByGroup.get(groupId)!.add(projectId)
        },
        async onGroupDeleted({ streamId: groupId }) {
          await setTimeout(20)
          const projectIds = state.projectsByGroup.get(groupId)

          projectIds?.forEach((projectId) => {
            projectAggregate.loadStream(projectId).delete()
          })
          state.projectsByGroup.delete(groupId)
        },
      }))

    const groupState = await groupAggregate.newStream().create('groupA').state()
    await Promise.all([
      projectAggregate
        .newStream()
        .create('projectA1', groupState.id!)
        .settled(),
      projectAggregate
        .newStream()
        .create('projectA2', groupState.id!)
        .settled(),
    ])

    expect(groups.size).toBe(1)
    expect(projects.size).toBe(2)

    await groupAggregate.loadStream(groupState.id!).delete().settled()

    expect(groups.size).toBe(0)
    expect(projects.size).toBe(0)
  })

  it('can change commands with process manager', async () => {
    const orderOfOperations: string[] = []
    const userAggregate = createAggregateRoot('users')
      .withEvents({
        UserCreated: z.object({
          name: z.string(),
        }),
        UserAgeSet: z.object({
          age: z.int(),
        }),
        UserRoleSet: z.object({
          role: z.string(),
        }),
      })
      .withCommands((event) => ({
        async *create(name: string) {
          await setTimeout(10)
          yield event('UserCreated', { name })
          orderOfOperations.push('created')
        },
        async *setAge(age: number) {
          await setTimeout(20)
          yield event('UserAgeSet', { age })
          orderOfOperations.push('age set')
        },
        async *setRole(role: string) {
          await setTimeout(30)
          yield event('UserRoleSet', { role })
          orderOfOperations.push('role set')
        },
      }))
    const { eventStore } = setupEventStore(userAggregate)

    eventStore.createProcessManager('user').withEventHandlers({
      async onUserCreated({ streamId }) {
        await setTimeout(30)
        orderOfOperations.push('injected')
        userAggregate.loadStream(streamId).setRole('Admin')
      },
    })

    await userAggregate.newStream().create('Torsten').setAge(40).settled()

    expect(orderOfOperations).toEqual([
      'created',
      'age set',
      'injected',
      'role set',
    ])
  })
})
