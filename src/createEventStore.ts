import type {
  AggregateRootApi,
  AggregateRootSnapshotMethods,
  AnyAggregateRoot,
  InternalAggregateRootApi,
} from './createAggregateRoot.ts'
import type { CheckpointMethods } from './createCheckpointApi.ts'
import {
  type EventListenerApi,
  type InternalEventListener,
  createEventListener,
} from './createEventListener.ts'
import {
  type InternalProcessManager,
  type ProcessManagerApi,
  buildProcessManagerCreator,
} from './createProcessManager.ts'
import {
  type InternalProjection,
  type ProjectionApi,
  type ProjectionApiWithSnapshots,
  type ProjectionSnapshotMethods,
  buildProjectionCreator,
} from './createProjection.ts'
import lock, { type LockCreator } from './lock.ts'
import { withRefreshing } from './refresh.ts'
import createTransaction from './transaction.ts'
import type {
  BaseInputEvent,
  BaseOutputEvent,
  EventHandlers,
  EventTemplate,
  MaybePromise,
  Stream,
  StreamEvents,
  StreamId,
  StreamIdEvents,
} from './utilityTypes.js'
import {
  type WrappedEvent,
  unwrapEvent,
  wrappedEventBuilder,
} from './wrappedEventBuilder.ts'

export type LoadEvents = (
  select:
    | Stream
    | StreamId
    | Stream[]
    | StreamId[]
    | StreamEvents
    | StreamEvents[]
    | StreamIdEvents
    | StreamIdEvents[]
    | null,
  range?: { from: number; to?: number },
) => AsyncGenerator<BaseOutputEvent>

type AppendEvents = (
  streamId: StreamId,
  events: BaseInputEvent[],
  expectedVersion: number,
) => MaybePromise<BaseOutputEvent[]>

export type InternalAppendEvents = (
  streamId: StreamId,
  events: BaseInputEvent[],
  expectedVersion: number,
) => MaybePromise<BaseOutputEvent[]>

export type EmitEvents = (events: BaseOutputEvent[]) => Promise<void>

export type EventsFromRoot<Root> =
  Root extends Omit<AggregateRootApi<any, any, infer E>, any> ? E : never

export type OnProgress = {
  begin?: (projectionName: string) => void
  progress?: (
    projectionName: string,
    index: number,
    event: BaseOutputEvent,
  ) => void
  end?: (projectionName: string) => void
}

interface BaseEventStoreParams<Root extends AnyAggregateRoot> {
  loadEvents: LoadEvents
  appendEvents: AppendEvents
  aggregateRoots: Root[]
  checkpoint: CheckpointMethods
  lock?: LockCreator
  projectionSnapshot?: ProjectionSnapshotMethods
  onProjectionReplay?: OnProgress
  onProcessManagerRefresh?: OnProgress
  autoInit?: boolean
}

export interface EventStoreParamsWithAggregateRootSnapshots<
  Root extends AnyAggregateRoot,
> extends BaseEventStoreParams<Root> {
  aggregateRootSnapshot: AggregateRootSnapshotMethods
  postProcessEvent?: (
    event: WrappedEvent<EventsFromRoot<Root>>,
    aggregateRoot: Root,
  ) => WrappedEvent
}

export interface EventStoreParamsWithoutAggregateRootSnapshots<
  Root extends AnyAggregateRoot,
> extends BaseEventStoreParams<Root> {
  aggregateRoots: Root[]
  aggregateRootSnapshot?: never
  postProcessEvent?: (
    event: WrappedEvent<EventsFromRoot<Root>>,
    aggregateRoot: Root,
  ) => WrappedEvent
}

export type EventStore<
  Event extends EventTemplate,
  Root extends AnyAggregateRoot,
  SupportsProjectionSnapshots = boolean,
> = {
  createProjection(
    name: string,
  ): SupportsProjectionSnapshots extends true
    ? ProjectionApiWithSnapshots<Event>
    : ProjectionApi<Event>
  createProcessManager(name: string): ProcessManagerApi<never, Event>
  createEventListener(name: string): EventListenerApi<Event>
  rebuild: () => Promise<void>
  isReady: () => Promise<void>
  init: () => Promise<EventStore<Event, Root, SupportsProjectionSnapshots>>
  getAggregateRoot<S extends Root['type']>(
    type: S,
  ): Root extends { type: S } ? Root : never
  transaction<T extends () => any>(handler: T): Promise<Awaited<ReturnType<T>>>
}

export function createEventStore<const Root extends AnyAggregateRoot>(
  params: EventStoreParamsWithoutAggregateRootSnapshots<Root>,
): EventStore<EventsFromRoot<Root>, Root, false>
export function createEventStore<const Root extends AnyAggregateRoot>(
  params: EventStoreParamsWithAggregateRootSnapshots<Root>,
): EventStore<EventsFromRoot<Root>, Root, true>
export function createEventStore<const Root extends AnyAggregateRoot>(
  params:
    | EventStoreParamsWithoutAggregateRootSnapshots<Root>
    | EventStoreParamsWithAggregateRootSnapshots<Root>,
) {
  const transaction = createTransaction()
  const { promise: initialized, resolve: resolveInitialized } =
    Promise.withResolvers<void>()
  const projections = new Map<string, InternalProjection>()
  const processManagers = new Map<string, InternalProcessManager>()
  const eventListeners = new Map<string, InternalEventListener>()
  const aggregatRootsByEvents = new Map<
    string,
    { aggregateRoot: InternalAggregateRootApi; eventType: string }
  >()

  let allReplaying = false
  let initializing = false

  const assertInitializing = () => {
    if (!initializing && params.autoInit === false) {
      throw new Error(
        'Cannot emit events because the event store is not initialized. Set autoInit to true, or explicitly call .init() before emitting events.',
      )
    }
  }

  let exclusiveEmitEventsChain: Promise<any> = Promise.resolve()
  const emitEvents: EmitEvents = async (events) => {
    exclusiveEmitEventsChain = exclusiveEmitEventsChain.then(async () => {
      for (const event of events) {
        await Promise.all(
          [
            ...processManagers.values(),
            ...projections.values(),
            ...eventListeners.values(),
          ].map((listener) => listener.apply(event)),
        )
      }

      for (const event of events) {
        await Promise.all(
          [...processManagers.values(), ...eventListeners.values()].map(
            (processManager) => processManager.applyAfter(event),
          ),
        )
      }
    })

    return exclusiveEmitEventsChain
  }

  const loadEvents: LoadEvents = (select, range) => {
    assertInitializing()
    return params.loadEvents(select, range)
  }

  let exclusiveAppendEventsChain: Promise<any> = initialized
  const appendEvents: InternalAppendEvents = async (
    streamId,
    events,
    expectedVersion,
  ) => {
    assertInitializing()
    exclusiveAppendEventsChain = exclusiveAppendEventsChain.then(async () => {
      if (events.length === 0) {
        return []
      }
      await new Promise((resolve) => setImmediate(resolve)) // edge case for sync setup, see 'can execute after effect' test

      const processedEvents = events.map((rawEvent) => {
        const event = structuredClone(rawEvent)
        if (params.postProcessEvent) {
          const wrappedEvent = wrappedEventBuilder(event) as WrappedEvent<any>
          const { aggregateRoot } = aggregatRootsByEvents.get(event.type)!
          const processedEvent = unwrapEvent(
            params.postProcessEvent(
              wrappedEvent,
              aggregateRoot as unknown as Root,
            ),
          )
          Object.assign(event, processedEvent)
        }

        return event
      })

      return params.appendEvents(streamId, processedEvents, expectedVersion)
    })

    return exclusiveAppendEventsChain
  }

  for (const aggregateRoot of params.aggregateRoots as unknown as InternalAggregateRootApi[]) {
    aggregateRoot.eventTypes.forEach((eventType) => {
      aggregatRootsByEvents.set(eventType, {
        aggregateRoot,
        eventType,
      })
    })

    aggregateRoot.linkEventStore(
      loadEvents,
      appendEvents,
      emitEvents,
      transaction,
      params.lock ?? lock(5000),
      params.aggregateRootSnapshot,
    )
  }

  const baseCreateProjection = buildProjectionCreator({
    loadEvents,
    checkpoint: params.checkpoint,
    snapshot: params.projectionSnapshot,
    onReplay: params.onProjectionReplay,
  })

  const baseCreateProcessManager = buildProcessManagerCreator({
    loadEvents,
    checkpoint: params.checkpoint,
    onRefresh: params.onProcessManagerRefresh,
  })

  const getRelatedStreamEvents = (
    handlers: EventHandlers<any>,
  ): StreamEvents[] => {
    const relatedStreamEvents = new Map<string, Set<string>>()
    for (const handlerName of Object.keys(handlers)) {
      const sanitizedHandlerName = handlerName.replace(/^(on|after)/, '')
      if (!aggregatRootsByEvents.has(sanitizedHandlerName)) {
        throw new Error(
          `event handler ${handlerName} has no corresponding event in any aggregate root`,
        )
      }
      const {
        aggregateRoot: { type: aggregateRootName },
        eventType,
      } = aggregatRootsByEvents.get(sanitizedHandlerName)!
      if (!relatedStreamEvents.has(aggregateRootName)) {
        relatedStreamEvents.set(aggregateRootName, new Set())
      }
      relatedStreamEvents.get(aggregateRootName)!.add(eventType)
    }

    return Array.from(relatedStreamEvents, ([stream, events]) => ({
      stream,
      events: Array.from(events),
    }))
  }

  const initialize = async () => {
    initializing = true
    await Promise.all(
      projections.values().map((projection) => projection.init(true)),
    )
    await Promise.all(
      processManagers
        .values()
        .map((processManager) => processManager.init(true)),
    )
    resolveInitialized()
  }
  setImmediate(() => {
    if (allReplaying || params.autoInit === false) {
      return
    }

    initialize()
  })

  const api = {
    createProjection(name) {
      if (projections.has(name)) {
        throw new Error(`Projection ${name} is already registered`)
      }
      const api = baseCreateProjection(name)
      const internalApi = api as unknown as InternalProjection
      internalApi.setRelatedStreamEvents((handlers) =>
        getRelatedStreamEvents(handlers),
      )
      projections.set(name, internalApi)
      if (initializing) {
        internalApi.init(true)
      }
      return api
    },
    createProcessManager(name) {
      if (processManagers.has(name)) {
        throw new Error(`Process Manager ${name} is already registered`)
      }
      const api = baseCreateProcessManager(name)

      const internalApi = api as unknown as InternalProcessManager
      internalApi.setRelatedStreamEvents((handlers) =>
        getRelatedStreamEvents(handlers),
      )
      processManagers.set(name, internalApi)
      if (initializing) {
        internalApi.init(true)
      }
      return api
    },
    createEventListener(name) {
      if (eventListeners.has(name)) {
        throw new Error(`Event Listener ${name} is already registered`)
      }

      const api = createEventListener(name)

      const internalApi = api as unknown as InternalEventListener
      eventListeners.set(name, internalApi)
      return api
    },
    async isReady() {
      await initialized
      await Promise.all([
        ...projections.values().map((projection) => projection.isReady()),
        ...processManagers.values().map((projection) => projection.isReady()),
      ])
    },
    async rebuild() {
      allReplaying = true
      initializing = true
      const replayProjections = Array.from(projections.values()).filter(
        (projection) => projection.canReplay(),
      )
      const replayProcessManagers = Array.from(processManagers.values()).filter(
        (processManager) => processManager.hasState(),
      )

      await Promise.all([
        ...replayProjections.map((projection) => projection.init(false)),
        ...replayProcessManagers.map((projection) => projection.init(false)),
        ...replayProjections.map((projection) => projection.beginReplay()),
        ...replayProcessManagers.map((processManager) =>
          processManager.beginRefresh(),
        ),
      ])

      await withRefreshing(async () => {
        let index = 0
        for await (const event of loadEvents(null)) {
          index++
          await Promise.all([
            ...replayProjections.map((projection) =>
              projection.progressReplay(index, event),
            ),
            ...replayProcessManagers.map((processManager) =>
              processManager.progressRefresh(index, event, false),
            ),
            ...Array.from(eventListeners.values(), (listener) =>
              listener.apply(event),
            ),
          ])
          await Promise.all([
            ...replayProcessManagers.map((processManager) =>
              processManager.progressRefresh(index, event, true),
            ),
            ...Array.from(eventListeners.values(), (listener) =>
              listener.applyAfter(event),
            ),
          ])
        }
      })

      await Promise.all([
        ...replayProjections.map((projection) => projection.endReplay()),
        ...replayProcessManagers.map((processManager) =>
          processManager.endRefresh(),
        ),
      ])

      await Promise.all(
        projections.values().map(async (projection) => {
          if (!projection.canReplay()) {
            await projection.init(true)
          }
        }),
      )
      resolveInitialized()
    },
    async init() {
      if (initializing) {
        return api
      }
      await initialize()
      return api
    },
    getAggregateRoot(type) {
      return params.aggregateRoots.find(
        (aggregateRoot) => aggregateRoot.type === type,
      ) as any
    },
    async transaction(handler) {
      return transaction.run(handler)
    },
  } as EventStore<any, Root>

  return api
}
