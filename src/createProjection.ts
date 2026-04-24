import { AsyncLocalStorage } from 'node:async_hooks'

import z from 'zod/v4'

import {
  type CheckpointApi,
  type CheckpointMethods,
  createCheckpointApi,
} from './createCheckpointApi.ts'
import type { LoadEvents, OnProgress } from './createEventStore.ts'
import {
  ProjectionEventHandlerExecutionError,
  ProjectionReplayLoopError,
  ReplaySkipError,
  ValidationError,
} from './errors.ts'
import { isReplaying, withReplaying } from './replay.ts'
import type {
  CompleteSnapshotGenerator,
  InternalSnapshotGenerator,
  SnapshotGenerator,
} from './snapshotGenerator.ts'
import createSnapshotGenerator from './snapshotGenerator.ts'
import type {
  AllEvents,
  BaseOutputEvent,
  EventHandlers,
  EventTemplate,
  Keys,
  MaybePromise,
  Merge,
  OutputEvent,
  StreamEvents,
} from './utilityTypes.d.ts'

const hydratingStore = new AsyncLocalStorage<boolean>()

function isHydrating() {
  return hydratingStore.getStore() ?? false
}

async function withHydrating(handler: () => Promise<void>) {
  await hydratingStore.run(true, handler)
}

type ProjectionInfo = {
  name: string
  event: BaseOutputEvent
}
const projectionInfoStore = new AsyncLocalStorage<ProjectionInfo>()

function withProjectionInfo(
  info: ProjectionInfo,
  handler: () => Promise<void>,
) {
  return projectionInfoStore.run(info, handler)
}

export function getProjectionInfo() {
  return projectionInfoStore.getStore()
}

export function isInsideProjection() {
  return Boolean(getProjectionInfo())
}

type ReplayConfig = {
  deleteOne?: (id: string) => MaybePromise<void>
  deleteAll?: () => MaybePromise<void>
}

type ProjectionEventHandlers<Events extends EventTemplate> = EventHandlers<
  Events,
  {
    replaying: boolean
    replayUntil: (
      position: number,
      filterEvents?: (event: OutputEvent<Events>) => boolean,
    ) => Promise<void>
  }
>

type ReplayProjection<Events extends EventTemplate> = {
  replayOne: (streamId: string) => Promise<void>
  replayUntil: (
    streamId: string,
    position: number,
    filterEvents?: (event: OutputEvent<Events>) => boolean,
  ) => Promise<void>
  replay: () => Promise<void>
}

export type InternalProjection = {
  apply: (event: BaseOutputEvent) => Promise<void>
  setRelatedStreamEvents: (
    generateRelatedStreamEvents: (
      eventHandlers: EventHandlers<EventTemplate>,
    ) => StreamEvents[],
  ) => void
  isReady(): Promise<void>
  canReplay(): boolean
  beginReplay(): Promise<void>
  progressReplay(index: number, event: BaseOutputEvent): Promise<void>
  endReplay(): void
  init(catchUp: boolean): Promise<void>
}

export type ProjectionApi<
  Events extends EventTemplate = never,
  RConfig extends ReplayConfig = never,
  Flags extends {
    withEventHandlers: boolean
    withGlobalEventHandler: boolean
    concurrent: boolean
    withReplay: boolean
    withSnapshots: boolean
  } = {
    withEventHandlers: false
    withGlobalEventHandler: false
    concurrent: false
    withReplay: false
    withSnapshots: true
  },
> = {
  withEventHandlers: (
    handlers: ProjectionEventHandlers<Events>,
  ) => Omit<
    ProjectionApi<Events, RConfig, Merge<Flags, { withEventHandlers: true }>>,
    Keys<Merge<Flags, { withEventHandlers: true }>>
  >
  withGlobalEventHandler: (
    handler: (
      event: AllEvents<Events>,
      methods: ReplayProjection<Events>,
    ) => MaybePromise<void>,
  ) => Omit<
    ProjectionApi<
      Events,
      RConfig,
      Merge<Flags, { withGlobalEventHandler: true }>
    >,
    Keys<Merge<Flags, { withGlobalEventHandler: true }>>
  >
  concurrent: () => Omit<
    ProjectionApi<Events, RConfig, Merge<Flags, { concurrent: true }>>,
    Keys<Merge<Flags, { concurrent: true }>>
  >
  withReplay: <Config extends ReplayConfig>(
    config: Config,
  ) => Omit<
    ProjectionApi<Events, Config, Merge<Flags, { withReplay: true }>>,
    Keys<Merge<Flags, { withReplay: true }>>
  >
  withSnapshots: (
    handler: (
      generator: SnapshotGenerator<Omit<Snapshot, 'schemaVersion' | 'state'>>,
    ) => CompleteSnapshotGenerator,
  ) => Omit<
    ProjectionApi<Events, RConfig, Merge<Flags, { withSnapshots: true }>>,
    Keys<Merge<Flags, { withSnapshots: true }>>
  >
  isReady(): Promise<void>
} & ('deleteOne' extends keyof RConfig
  ? Pick<ReplayProjection<Events>, 'replayOne' | 'replayUntil'>
  : {}) &
  ('deleteAll' extends keyof RConfig
    ? Pick<ReplayProjection<Events>, 'replay'>
    : {})

export type ProjectionApiWithSnapshots<
  Events extends EventTemplate = never,
  RConfig extends ReplayConfig = never,
  Flags extends {
    withEventHandlers: boolean
    withGlobalEventHandler: boolean
    concurrent: boolean
    withReplay: boolean
    withSnapshots: boolean
  } = {
    withEventHandlers: false
    withGlobalEventHandler: false
    concurrent: false
    withReplay: false
    withSnapshots: false
  },
> = ProjectionApi<Events, RConfig, Flags>

const snapshotSchema = z.object({
  projectionId: z.string(),
  projectionType: z.string(),
  lastEventPosition: z.int(),
  state: z.unknown(),
  metadata: z.record(z.string(), z.unknown()).optional(),
  schemaVersion: z.int(),
  createdAt: z.date(),
})

export type Snapshot = z.output<typeof snapshotSchema>

export type ProjectionSnapshotMethods = {
  get: (select: {
    projection: string
    id: string
  }) => MaybePromise<Snapshot | null | undefined>
  put: (snapshot: Snapshot) => MaybePromise<void>
  delete: (
    target: { id?: string; projection: string },
    fromPosition?: number,
  ) => MaybePromise<void>
  incrementAppliedCount: (target: {
    projection: string
    id: string
  }) => MaybePromise<number>
}

export function buildProjectionCreator(params: {
  loadEvents: LoadEvents
  checkpoint: CheckpointMethods
  onReplay?: OnProgress
  snapshot?: ProjectionSnapshotMethods
}): (name: string) => ProjectionApi {
  return (name) => {
    let idle = true
    let replaying = false
    let handlers: ProjectionEventHandlers<any> = {}
    let globalEventHandler:
      | ((event: BaseOutputEvent) => MaybePromise<void>)
      | null = null
    let generateRelatedStreamEvents: (
      eventHandlers: EventHandlers<any>,
    ) => StreamEvents[]
    let relatedStreamEvents: StreamEvents[] | null = null
    let exclusive = true
    let checkpointApi: CheckpointApi
    let lastEventPosition = -1

    let replayConfig: ReplayConfig | null = null

    let snapshotting:
      | (CompleteSnapshotGenerator & InternalSnapshotGenerator)
      | null = null

    const { promise: hydrated, resolve: resolveHydrated } =
      Promise.withResolvers<void>()

    const locks = new Map<string, Promise<void>>()
    const apply = async (event: BaseOutputEvent) => {
      const handler = handlers[`on${event.type}`]
      if (!handler && !globalEventHandler) {
        return Promise.resolve()
      }

      if (isHydrating()) {
        lastEventPosition = event.position
      } else {
        await hydrated
        // event was already applied through hydration
        if (event.position <= lastEventPosition) {
          return
        }
      }

      const unlocked = !exclusive
        ? Promise.resolve()
        : (locks.get(event.streamId)?.catch(() => {}) ?? Promise.resolve())

      const runner = (async () => {
        await unlocked
        await withProjectionInfo({ name, event }, async () => {
          try {
            await handler?.(event, {
              replaying,
              async replayUntil(position, filterEvents) {
                if (position >= event.position) {
                  throw new ProjectionReplayLoopError(
                    `Event handler on${event.type} of projection ${name} tried to replay to the future`,
                    { name, eventHandler: `on${event.type}` },
                    event,
                  )
                }

                locks.delete(event.streamId)
                return api.replayUntil(event.streamId, position, filterEvents)
              },
            })
          } catch (error) {
            if (error instanceof ReplaySkipError) {
              if (isReplaying()) {
                return
              }

              throw new Error(
                'ReplaySkipError should never be thrown outside replay',
              )
            }

            if (
              !(error instanceof Error) ||
              error instanceof ProjectionReplayLoopError
            ) {
              throw error
            }
            throw new ProjectionEventHandlerExecutionError(
              name,
              `on${event.type}`,
              event,
              error,
            )
          }
          try {
            await globalEventHandler?.(event)
          } catch (error) {
            if (!(error instanceof Error)) {
              throw error
            }

            throw new ProjectionEventHandlerExecutionError(
              name,
              'GLOBAL',
              event,
              error,
            )
          }
        })

        if (params.snapshot && snapshotting) {
          const appliedEvents = await params.snapshot.incrementAppliedCount({
            projection: name,
            id: event.streamId,
          })
          const snapshot = await snapshotting.internalCapture(
            event,
            appliedEvents,
          )

          if (snapshot) {
            await params.snapshot.put({
              projectionType: name,
              projectionId: event.streamId,
              lastEventPosition: event.position,
              state: snapshot.state,
              schemaVersion: snapshot.version,
              createdAt: new Date(),
            })
          }
        }

        await checkpointApi.upsert(event)
      })()

      if (exclusive) {
        locks.set(event.streamId, runner)

        runner
          .finally(() => {
            if (locks.get(event.streamId) === runner) {
              locks.delete(event.streamId)
            }
          })
          .catch(() => {})
        return runner
      }

      return Promise.resolve()
    }

    let resolveReplay: null | (() => void) = null
    let resolveReplayOne: null | (() => void) = null
    let replayOneStreamId: null | string = null

    const api = {
      name: 'projection ' + name,
      async isReady() {
        await hydrated
      },
      withEventHandlers(h) {
        handlers = h
        return api
      },
      withGlobalEventHandler(handler) {
        globalEventHandler = handler as any
        return api
      },
      concurrent() {
        exclusive = false
        return api
      },
      setRelatedStreamEvents(generate) {
        generateRelatedStreamEvents = generate
      },
      withReplay(config) {
        replayConfig = config
        return api as any
      },

      withSnapshots(handler) {
        snapshotting = handler(createSnapshotGenerator() as any) as any
        return api
      },

      async apply(event) {
        await apply(event)
      },

      async replayOne(streamId: string) {
        if (!idle) {
          await replayOne(streamId)
          return
        }

        const { promise, resolve } = Promise.withResolvers<void>()
        resolveReplayOne = resolve
        replayOneStreamId = streamId
        return promise
      },

      async replay() {
        if (!idle) {
          await replay()
          return
        }
        const { promise, resolve } = Promise.withResolvers<void>()
        resolveReplay = resolve
        return promise
      },

      canReplay() {
        return Boolean(replayConfig)
      },

      async beginReplay() {
        if (!replayConfig?.deleteAll) {
          throw new Error(
            'this projection does not support replay all. withReplay({ deleteAll }) not implemented.',
          )
        }

        lastEventPosition = -1
        params.onReplay?.begin?.(name)
        await params.snapshot?.delete({ projection: name })
        await replayConfig.deleteAll()
      },

      async progressReplay(index, event) {
        await withReplaying(() => withHydrating(() => apply(event)))
        if (
          !checkpointApi.relatedStreamEvents ||
          checkpointApi.relatedStreamEvents.some((streamEvent) =>
            streamEvent.events.includes(event.type),
          )
        ) {
          params.onReplay?.progress?.(name, index, event)
        }
      },

      endReplay() {
        params.onReplay?.end?.(name)
        resolveHydrated()
      },

      async replayUntil(id, position, filterEvents = () => true) {
        let startPosition = 0
        if (snapshotting) {
          await params.snapshot?.delete({ id, projection: name }, position)
          const snapshot = await params.snapshot?.get({ id, projection: name })
          if (snapshot) {
            const result = snapshotSchema.safeParse(snapshot)

            if (!result.success) {
              throw new ValidationError(
                'loaded projection snapshot does not comply with schema',
                result.error,
              )
            }
            snapshotting.internalApply(result.data)
            startPosition = result.data.lastEventPosition + 1
          }
        }

        const events = params.loadEvents(
          relatedStreamEvents?.map((relatedStreamEvent) => ({
            ...relatedStreamEvent,
            id,
          })) ?? null,
          {
            from: startPosition,
            to: position,
          },
        )

        replaying = true
        await withReplaying(async () => {
          for await (const event of events) {
            if ((filterEvents as (event: BaseOutputEvent) => boolean)(event)) {
              await apply(event)
            }
          }
        })
        replaying = false
      },
      async init(catchUp) {
        idle = false
        relatedStreamEvents = globalEventHandler
          ? null
          : generateRelatedStreamEvents(handlers as EventHandlers<any>)
        checkpointApi = createCheckpointApi({
          ...params.checkpoint,
          name,
          relatedStreamEvents,
          type: 'projection',
          loadEvents: params.loadEvents,
          apply: (event) => withHydrating(() => apply(event)),
        })

        if (resolveReplay) {
          await replay()
          resolveReplay()
          return
        }

        if (resolveReplayOne && replayOneStreamId) {
          await replayOne(replayOneStreamId)
        }

        if (!catchUp) {
          return
        }

        await checkpointApi.rehydrated()
        resolveHydrated()
        resolveReplayOne?.()
      },
    } as ProjectionApi & InternalProjection & ReplayProjection<any>

    async function replayOne(id: string) {
      if (!replayConfig?.deleteOne) {
        throw new Error(
          'this projection does not support replay one. withReplay({ deleteOne }) not implemented.',
        )
      }
      await params.snapshot?.delete({ id, projection: name })
      await replayConfig.deleteOne(id)
      const streamIds = relatedStreamEvents
        ? relatedStreamEvents.map((relatedStreamEvent) => ({
            ...relatedStreamEvent,
            id,
          }))
        : null
      const events = params.loadEvents(streamIds)

      replaying = true
      await withReplaying(async () => {
        for await (const event of events) {
          await withHydrating(() => apply(event))
        }
      })
      replaying = false
    }

    async function replay() {
      await api.beginReplay()
      const events = params.loadEvents(relatedStreamEvents)
      let index = 0
      replaying = true
      await withReplaying(async () => {
        for await (const event of events) {
          await api.progressReplay(index, event)
          index++
        }
      })
      replaying = false
      api.endReplay()
    }

    return api
  }
}
