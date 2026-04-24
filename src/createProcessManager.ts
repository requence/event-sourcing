import { AsyncLocalStorage } from 'node:async_hooks'

import {
  type CheckpointApi,
  type CheckpointMethods,
  createCheckpointApi,
} from './createCheckpointApi.ts'
import type { LoadEvents, OnProgress } from './createEventStore.ts'
import {
  ProcessManagerEventHandlerExecutionError,
  RefreshingSkipError,
} from './errors.ts'
import { isRefreshing, withRefreshing } from './refresh.ts'
import { clone } from './superjson.ts'
import type {
  AfterEffectHandlers,
  BaseOutputEvent,
  EventHandlers,
  EventTemplate,
  Keys,
  Merge,
  StreamEvents,
} from './utilityTypes.js'

type ProcessManagerInfo = {
  name: string
  event: BaseOutputEvent
}
const processManagerInfoStore = new AsyncLocalStorage<ProcessManagerInfo>()

function withProcessManagerInfo(
  info: ProcessManagerInfo,
  handler: () => Promise<void>,
) {
  return processManagerInfoStore.run(info, handler)
}

export function getProcessManagerInfo() {
  return processManagerInfoStore.getStore()
}

export function isInsideProcessManager() {
  return Boolean(getProcessManagerInfo())
}

type ProcessManagerEventHandlers<Events extends EventTemplate> = EventHandlers<
  Events,
  { refreshing: boolean }
>

type ProcessManagerAfterEffectHandlers<Events extends EventTemplate> =
  AfterEffectHandlers<Events, { refreshing: boolean }>

export type InternalProcessManager = {
  apply: (event: BaseOutputEvent) => Promise<void>
  applyAfter: (event: BaseOutputEvent) => Promise<void>
  setRelatedStreamEvents: (
    generateRelatedStreamEvents: (
      eventHandlers: EventHandlers<EventTemplate>,
    ) => StreamEvents[],
  ) => void
  isReady(): Promise<void>
  beginRefresh(): Promise<void>
  progressRefresh(
    index: number,
    event: BaseOutputEvent,
    after: boolean,
  ): Promise<void>
  endRefresh(): void
  init(catchUp: boolean): Promise<void>
  hasState(): boolean
}

export type ProcessManagerApi<
  State = never,
  Events extends EventTemplate = never,
  Flags extends {
    withState: boolean
    withEventHandlers: boolean
    concurrent: boolean
    withAfterEffects: boolean
  } = {
    withState: false
    withEventHandlers: false
    concurrent: false
    withAfterEffects: false
  },
> = {
  withState: <NextState>(
    state: NextState,
  ) => Omit<
    ProcessManagerApi<NextState, Events, Merge<Flags, { withState: true }>>,
    Keys<Merge<Flags, { withState: true }>>
  >
  withEventHandlers: (
    handlers: [State] extends [never]
      ? ProcessManagerEventHandlers<Events>
      : (state: State) => ProcessManagerEventHandlers<Events>,
  ) => Omit<
    ProcessManagerApi<State, Events, Merge<Flags, { withEventHandlers: true }>>,
    Keys<Merge<Flags, { withEventHandlers: true }>>
  >
  withAfterEffects: (
    handlers: [State] extends [never]
      ? ProcessManagerAfterEffectHandlers<Events>
      : (state: State) => ProcessManagerAfterEffectHandlers<Events>,
  ) => Omit<
    ProcessManagerApi<State, Events, Merge<Flags, { withAfterEffects: true }>>,
    Keys<Merge<Flags, { withAfterEffects: true }>>
  >
  concurrent: () => Omit<
    ProcessManagerApi<State, Events, Merge<Flags, { concurrent: true }>>,
    Keys<Merge<Flags, { concurrent: true }>>
  >
  isReady(): Promise<void>
  $inferState: State
} & (Flags['withState'] extends true
  ? {
      refreshState: () => Promise<void>
      state: () => Promise<State>
    }
  : {})

export function buildProcessManagerCreator(params: {
  loadEvents: LoadEvents
  checkpoint: CheckpointMethods
  onRefresh?: OnProgress
}) {
  return (name: string): ProcessManagerApi => {
    let idle = true
    let createEventHandlers: (
      state: any,
    ) => ProcessManagerEventHandlers<any> = () => ({})
    let createAfterEffects: (
      state: any,
    ) => ProcessManagerAfterEffectHandlers<any> = () => ({})

    let hasState = false
    let state: any = null
    let initialState: any = null
    let exclusive = true
    let generateRelatedStreamEvents: (
      eventHandlers: EventHandlers<any>,
    ) => StreamEvents[]
    let checkpointApi: CheckpointApi
    let lastEventPosition = -1
    const { promise: hydrated, resolve: resolveHydrated } =
      Promise.withResolvers<void>()

    const locks = new Map<string, Promise<void>>()
    const apply = async (
      event: BaseOutputEvent,
      after: boolean,
      hydrating = false,
    ) => {
      const handler = after
        ? createAfterEffects(state)[`after${event.type}`]
        : createEventHandlers(state)[`on${event.type}`]

      if (!handler) {
        return Promise.resolve()
      }

      if (hydrating) {
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
        const refreshing = isRefreshing()
        await withProcessManagerInfo(
          {
            name,
            event,
          },
          async () => {
            try {
              await handler(event, { refreshing })
            } catch (error) {
              if (error instanceof RefreshingSkipError) {
                if (refreshing) {
                  return
                }

                throw new Error(
                  'RefreshingSkipError should never be thrown outside refresh',
                )
              }

              if (!(error instanceof Error)) {
                throw error
              }

              throw new ProcessManagerEventHandlerExecutionError(
                name,
                after ? `after${event.type}` : `on${event.type}`,
                event,
                error,
              )
            }
          },
        )
        await checkpointApi.upsert(event, { state })
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

    let resolveRefresh: null | (() => void) = null

    const api = {
      name,
      hasState() {
        return hasState
      },
      async isReady() {
        await hydrated
      },
      async apply(event) {
        await apply(event, false)
      },
      async applyAfter(event) {
        await apply(event, true)
      },
      withState(nextState) {
        state = clone(nextState)
        initialState = clone(nextState)
        hasState = true
        return api as any
      },
      concurrent() {
        exclusive = false
        return api
      },
      withEventHandlers(handlers) {
        createEventHandlers =
          typeof handlers === 'function' ? (handlers as any) : () => handlers

        return api
      },
      withAfterEffects(handlers) {
        createAfterEffects =
          typeof handlers === 'function' ? (handlers as any) : () => handlers

        return api
      },
      setRelatedStreamEvents(generate) {
        generateRelatedStreamEvents = generate
      },
      async beginRefresh() {
        params.onRefresh?.begin?.(name)
        await checkpointApi.clear()
        lastEventPosition = -1
        state = initialState
      },
      async progressRefresh(index, event, after) {
        await apply(event, after, true)

        if (
          !checkpointApi.relatedStreamEvents ||
          checkpointApi.relatedStreamEvents.some((streamEvent) =>
            streamEvent.events.includes(event.type),
          )
        ) {
          params.onRefresh?.progress?.(name, index, event)
        }
      },
      endRefresh() {
        params.onRefresh?.end?.(name)
        resolveHydrated()
      },
      async refreshState() {
        if (!idle) {
          await refresh()
          return
        }
        const { promise, resolve } = Promise.withResolvers<void>()
        resolveRefresh = resolve
        return promise
      },
      async state() {
        await hydrated
        return state
      },
      async init(catchUp) {
        idle = false
        const relatedStreamEvents = generateRelatedStreamEvents({
          ...createEventHandlers(null),
          ...createAfterEffects(null),
        })

        checkpointApi = createCheckpointApi({
          ...params.checkpoint,
          name,
          relatedStreamEvents,
          type: 'processManager',
          loadEvents: params.loadEvents,
          apply: (event) => apply(event, false, true),
          applyAfter: (event) => apply(event, true, true),
        })

        if (resolveRefresh) {
          await refresh()
          return
        }
        if (!catchUp) {
          return
        }

        await checkpointApi.rehydrated({
          getMetadata() {
            return { state }
          },
          onMetadata(metadata) {
            state = metadata.state ?? state
          },
        })

        resolveHydrated()
      },
      $inferState: null as never,
    } as ProcessManagerApi & InternalProcessManager

    async function refresh() {
      await withRefreshing(async () => {
        await api.beginRefresh()
        await checkpointApi.rehydrated({
          getMetadata() {
            return { state }
          },
          onProgress(index, event) {
            params.onRefresh?.progress?.(name, index, event)
          },
        })
      })
      api.endRefresh()
    }

    return api
  }
}
