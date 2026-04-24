import { AsyncLocalStorage } from 'node:async_hooks'

import { deepEqual } from 'fast-equals'
import { z } from 'zod/v4'

import type {
  EmitEvents,
  InternalAppendEvents,
  LoadEvents,
} from './createEventStore.ts'
import { getProcessManagerInfo } from './createProcessManager.ts'
import { getProjectionInfo } from './createProjection.ts'
import {
  CommandError,
  EventValidationError,
  InfiniteLoopError,
  ValidationError,
} from './errors.ts'
import type { LockCreator } from './lock.ts'
import { isRefreshing } from './refresh.ts'
import createSnapshotGenerator, {
  type CompleteSnapshotGenerator,
  type InternalSnapshotGenerator,
  type SnapshotGenerator,
} from './snapshotGenerator.ts'
import { clone } from './superjson.ts'
import type { Transaction, TransactionDelay } from './transaction.ts'
import type {
  BaseInputEvent,
  BaseOutputEvent,
  EventTemplate,
  Keys,
  MaybePromise,
  Merge,
  OutputEvents,
  Prettify,
} from './utilityTypes.ts'
import { awaitAll, normalizeArray } from './utils.ts'
import {
  type WrappedEvent,
  type WrappedEventBuilder,
  handleEventError,
  unwrapEvent,
  wrappedEventBuilder,
} from './wrappedEventBuilder.ts'

type UppercaseRecord<T extends Record<string, any>> = {
  [K in keyof T]: K extends Capitalize<K & string>
    ? any
    : `Error: Key '${K & string}' must be Capitalized`
}

type BaseEventMap = Record<
  string,
  z.ZodType | (z.ZodType | { schema: z.ZodType; version: number })[]
>

type DownstreamRootPromise = () => Promise<unknown>
const downstreamRootsPromiseStore = new AsyncLocalStorage<
  DownstreamRootPromise[]
>()

function registerRootPromiseToUpstream(rootPromise: DownstreamRootPromise) {
  downstreamRootsPromiseStore.getStore()?.push(rootPromise)
}

async function collectDownstreamRootPromise(handler: () => Promise<void>) {
  const rootPromises: DownstreamRootPromise[] = []
  await downstreamRootsPromiseStore.run(rootPromises, handler)
  return async () => {
    await Promise.all(rootPromises.map((promise) => promise()))
  }
}

type BaseInternalEvent<
  Type extends string,
  Version extends number,
  Payload = unknown,
> = BaseOutputEvent<Type, Version, Payload>

type InternalEvent<Event extends EventTemplate> = Prettify<
  {
    [K in Event as K['version']]: Omit<
      BaseInternalEvent<K['type'], K['version'], z.output<K['schema']>>,
      'createdAt' | 'position' | 'streamVersion'
    >
  }[Event['version']]
>

type InternalEventHandlers<Events extends EventTemplate> = {
  [K in Events as `on${K['type']}`]?: (
    event: InternalEvent<K>,
  ) => MaybePromise<void>
}

type EventsFromMap<EventMap extends BaseEventMap> = Prettify<
  {
    [K in keyof EventMap]: { type: K & string } & (EventMap[K] extends z.ZodType
      ? { schema: EventMap[K]; version: 0 }
      : EventMap[K] extends { schema: infer S; version: infer V }
        ? S extends z.ZodType
          ? { schema: S; version: V }
          : never
        : EventMap[K] extends Array<infer I>
          ? I extends z.ZodType
            ? { schema: I; version: 0 }
            : I extends { schema: infer S; version: infer V }
              ? { schema: S; version: V }
              : never
          : never)
  }[keyof EventMap]
>

type Constructor<T> = new (...args: any[]) => T

type Command = (
  ...args: any[]
) =>
  | MaybePromise<void | WrappedEvent | WrappedEvent[]>
  | Iterable<WrappedEvent>
  | AsyncIterable<WrappedEvent>

type Utils = {
  reset: () => void
  replayUntil: (streamVersion: number) => Promise<void>
}

type VersionBuilder = <V extends number, S extends z.ZodType>(
  version: V,
  schema: S,
) => { schema: S; version: V }

type Stream<Commands extends Record<string, Command>, State> = {
  [A in keyof Commands]: (
    ...args: Parameters<Commands[A]>
  ) => Stream<Commands, State>
} & {
  settled: () => Promise<Stream<Commands, State>>
  state: () => Promise<State>
  transformError: <T extends Error>(
    error: Constructor<T>,
    errorHandler: (commandError: CommandError<T>) => Error,
  ) => Stream<Commands, State>
  readonly streamId: string
}

export type AnyStream = {
  settled: () => Promise<AnyStream>
  state: () => Promise<unknown>
  transformError: <T extends Error>(
    error: Constructor<T>,
    errorHandler: (error: CommandError<T>) => Error,
  ) => AnyStream
}

export type AnyAggregateRoot = {
  type: string
  loadStream: (
    id: string,
    filter?: (event: BaseOutputEvent) => MaybePromise<boolean>,
  ) => AnyStream
  newStream: () => AnyStream
  $inferState: any
  $inferEvents: any
  $inferEvent: any
  $inferCommandParameters: any
}

type EventHandlerOptions = {
  streamId: string
  extendLock: (ms?: number) => Promise<boolean>
}

export type AggregateRootApi<
  Name,
  State = never,
  Events extends EventTemplate = never,
  Commands extends Record<string, Command> = Record<string, Command>,
  Flags extends {
    withInitialState: boolean
    withEvents: boolean
    withCommands: boolean
    withEventHandlers: boolean
    withSnapshots: boolean
  } = {
    withInitialState: false
    withEvents: false
    withCommands: true
    withEventHandlers: true
    withSnapshots: false
  },
> = {
  withInitialState: <NextState extends Record<string, unknown>>(
    state: NextState,
  ) => Omit<
    AggregateRootApi<
      Name,
      NextState,
      Events,
      Commands,
      Merge<Flags, { withInitialState: true; withEventHandlers: false }>
    >,
    Keys<Merge<Flags, { withInitialState: true; withEventHandlers: false }>>
  >
  withCommands: <
    NextCommands extends Record<
      Exclude<string, 'state' | 'settled' | 'transformError' | 'streamId'>,
      Command
    >,
  >(
    handlers: [State] extends [never]
      ? (
          event: WrappedEventBuilder<Events>,
          options: EventHandlerOptions,
        ) => NextCommands
      : (
          state: Readonly<State>,
          event: WrappedEventBuilder<Events>,
          options: EventHandlerOptions,
        ) => NextCommands,
  ) => Omit<
    AggregateRootApi<
      Name,
      State,
      Events,
      NextCommands,
      Merge<
        Flags,
        { withInitialState: true; withCommands: true; withEvents: true }
      >
    >,
    Keys<
      Merge<
        Flags,
        { withInitialState: true; withCommands: true; withEvents: true }
      >
    >
  >
  withEvents: {
    <EventMap extends BaseEventMap>(
      handler: EventMap & UppercaseRecord<EventMap>,
    ): Omit<
      AggregateRootApi<
        Name,
        State,
        EventsFromMap<EventMap>,
        Commands,
        Merge<
          Flags,
          {
            withInitialState: true
            withEvents: true
            withCommands: false
          }
        >
      >,
      Keys<
        Merge<
          Flags,
          {
            withInitialState: true
            withEvents: true
            withCommands: false
          }
        >
      >
    >
    <EventMap extends BaseEventMap>(
      handler: (utils: {
        version: VersionBuilder
        z: typeof z
      }) => EventMap & UppercaseRecord<EventMap>,
    ): Omit<
      AggregateRootApi<
        Name,
        State,
        EventsFromMap<EventMap>,
        Commands,
        Merge<
          Flags,
          {
            withInitialState: true
            withEvents: true
            withCommands: false
          }
        >
      >,
      Keys<
        Merge<
          Flags,
          {
            withInitialState: true
            withEvents: true
            withCommands: false
          }
        >
      >
    >
  }
  withEventHandlers: (
    handler: (state: State, utils: Utils) => InternalEventHandlers<Events>,
  ) => Omit<
    AggregateRootApi<
      Name,
      State,
      Events,
      Commands,
      Merge<
        Flags,
        { withInitialState: true; withEventHandlers: true; withEvents: true }
      >
    >,
    Keys<
      Merge<
        Flags,
        { withInitialState: true; withEventHandlers: true; withEvents: true }
      >
    >
  >
  withSnapshots: (
    handler: (
      state: State,
      generate: SnapshotGenerator<Omit<Snapshot, 'schemaVersion' | 'state'>>,
      utils: { z: typeof z },
    ) => CompleteSnapshotGenerator,
  ) => Omit<
    AggregateRootApi<
      Name,
      State,
      Events,
      Commands,
      Merge<Flags, { withSnapshots: true }>
    >,
    Keys<Merge<Flags, { withSnapshots: true }>>
  >
  loadStream: (
    id: string,
    filter?: (event: OutputEvents<Events>) => MaybePromise<boolean>,
  ) => Stream<Commands, State>
  newStream: (id?: string) => Stream<Commands, State>

  $inferEvents: OutputEvents<Events>
  $inferEvent: OutputEvents<Events> extends BaseOutputEvent
    ? { [E in OutputEvents<Events> as E['type']]: E }
    : never
  $inferState: State
  $inferCommandParameters: {
    [CommandName in keyof Commands]: Parameters<Commands[CommandName]>
  }
  readonly type: Name
}

export type InternalAggregateRootApi = {
  linkEventStore: (
    loadEvents: LoadEvents,
    appendEvents: InternalAppendEvents,
    emitEvents: EmitEvents,
    transaction: Transaction,
    lock: LockCreator,
    snapshots?: AggregateRootSnapshotMethods,
  ) => void
  eventTypes: string[]
  readonly type: string
}

const snapshotSchema = z.object({
  streamId: z.string(),
  schemaVersion: z.int(),
  state: z.unknown(),
  createdAt: z.date(),
  streamVersion: z.int(),
  streamType: z.string(),
  lastEventPosition: z.int(),
  metadata: z.record(z.string(), z.unknown()).optional(),
})

export type Snapshot = z.output<typeof snapshotSchema>

export type AggregateRootSnapshotMethods = {
  get: (select: {
    stream: string
    id: string
  }) => MaybePromise<Snapshot | null | undefined>
  put: (snapshot: Snapshot) => MaybePromise<void>
  delete: (
    target: {
      stream: string
      id: string
    },
    fromPosition: number,
  ) => MaybePromise<void>
}

export function createAggregateRoot<Name extends string>(type: Name) {
  let initialState: any = null
  let createEventHandlers: (
    state: any,
  ) => InternalEventHandlers<any> = () => ({})

  let loadEvents!: LoadEvents
  let appendEvents!: InternalAppendEvents
  let emitEvents!: EmitEvents
  let snapshots: AggregateRootSnapshotMethods | undefined
  let createLock!: LockCreator
  let transaction!: Transaction

  let createCommands: (
    state: any,
    eventBuilder: (name: string, payload: unknown) => WrappedEvent,
    options: EventHandlerOptions,
  ) => Record<string, Command> = () => ({})

  const registeredEvents = new Map<string, Map<number, z.ZodType>>()
  let createSnapshotApi:
    | ((
        state: any,
        generate: SnapshotGenerator<any>,
        utils: { z: typeof z },
      ) => CompleteSnapshotGenerator & InternalSnapshotGenerator)
    | null = null

  const loadUntil = async (
    state: any,
    snapshotApi:
      | (CompleteSnapshotGenerator & InternalSnapshotGenerator)
      | undefined,
    id: string,
    untilPosition?: number,
    filter?: (event: BaseOutputEvent) => MaybePromise<boolean>,
  ) => {
    let position = 0
    let version = 0
    if (snapshotApi) {
      const snapshot = await snapshots?.get({
        stream: type,
        id,
      })
      if (snapshot) {
        const result = snapshotSchema.safeParse(snapshot)

        if (!result.success) {
          throw new ValidationError(
            'loaded aggregate root snapshot does not comply with schema',
            result.error,
          )
        }

        snapshotApi.internalApply(result.data)
        position = snapshot.lastEventPosition
        version = snapshot.streamVersion
      }
    }

    const events = loadEvents(
      { stream: type, id },
      {
        from: position,
        to: untilPosition,
      },
    )
    const eventHandlers = createEventHandlers(state)
    for await (const event of events) {
      version += 1
      if ((await filter?.(event)) === false) {
        continue
      }
      await (eventHandlers as any)[`on${event.type}`]?.(event)
    }

    return version
  }

  const createStream = (state: any, streamId: string) => {
    let version = 0
    const snapshotApi = createSnapshotApi?.(
      state,
      createSnapshotGenerator() as any,
      { z },
    )
    const errorHandlers = new Map<
      Constructor<Error>,
      (error: CommandError<Error>) => Error
    >()
    let downstreamSettledPromise = () => Promise.resolve()
    let releaseLock!: () => Promise<void>
    let extendLock!: EventHandlerOptions['extendLock']

    const warnReleaseLock = () => {
      if (releasedAfterChain) {
        console.warn(`aggregate root for ${type}:${streamId} was open too long`)
      }
      releaseLock()
    }

    const processEvents = async (events: BaseInputEvent[]) => {
      if (!appendEvents) {
        throw new Error(
          `aggregate root ${type} is not linked to an event store`,
        )
      }

      const persistedEvents = await appendEvents(
        { stream: type, id: streamId },
        events,
        version,
      )

      warnReleaseLock()

      for (const persistedEvent of persistedEvents) {
        const snapshot = await snapshotApi?.internalCapture(
          persistedEvent,
          persistedEvent.streamVersion,
        )

        if (snapshot) {
          await snapshots?.put({
            state: snapshot.state,
            lastEventPosition: persistedEvent.position,
            streamId,
            streamVersion: version,
            streamType: type,
            schemaVersion: snapshot.version,
            createdAt: new Date(),
          })
        }
      }

      version += events.length

      downstreamSettledPromise = await collectDownstreamRootPromise(() =>
        emitEvents(persistedEvents),
      )
    }

    let commandChain = createLock([type, streamId]).then((lock) => {
      releaseLock = lock.release
      extendLock = lock.extend
      return [] as Error | BaseInputEvent[]
    })
    let commandChainTouched = false
    let releasedAfterChain = false
    let transactionDelay: TransactionDelay | undefined

    const stateReady = Promise.withResolvers<Error | undefined>()

    const rawCommands = createCommands(state, wrappedEventBuilder, {
      streamId,
      extendLock,
    })

    const commands = Object.fromEntries(
      Object.entries(rawCommands).map(([commandName, commandFn]) => [
        commandName,
        (...args: any[]) => {
          // don't create new events when refreshing the process manager
          if (isRefreshing()) {
            return api.commands
          }

          const commandId = crypto.randomUUID()
          transactionDelay ??= transaction.delay()
          commandChainTouched = true
          commandChain = commandChain
            .then(async (previousEvents) => {
              if (previousEvents instanceof Error) {
                return previousEvents
              }
              const events = (
                await normalizeArray(
                  commandFn.apply(rawCommands, args),
                  () => extendLock(),
                )
              ).filter((event) => Boolean(event))

              const baseInputEvents = await Promise.all(
                events.map(async (event) => {
                  event.withAdditionalMetadata({ commandName })
                  event.withDefaultCausationId(commandId)

                  const unwrappedEvent = unwrapEvent(event)

                  const eventVersions = registeredEvents.get(
                    unwrappedEvent.type,
                  )
                  if (!eventVersions) {
                    throw new Error(
                      `cannot append unknown event ${unwrappedEvent.type}`,
                    )
                  }
                  const latestVersion = Math.max(
                    ...Array.from(eventVersions.keys()),
                  )
                  const schema = eventVersions.get(latestVersion)!

                  const result = schema.safeParse(unwrappedEvent.payload)
                  if (!result.success) {
                    const error = new EventValidationError(
                      `payload does not comply with schema for event ${unwrappedEvent.type}@${latestVersion} at stream ${streamId}`,
                      result.error,
                    )

                    await handleEventError(event, error)
                    throw error
                  }

                  unwrappedEvent.payload = result.data

                  return {
                    ...unwrappedEvent,
                    schemaVersion: latestVersion,
                  }
                }),
              )

              const eventHandlers = createEventHandlers(state)
              for (const event of baseInputEvents) {
                const processManagerInfo = getProcessManagerInfo()
                if (processManagerInfo) {
                  if (
                    processManagerInfo.event.streamId === streamId &&
                    processManagerInfo.event.streamType === type &&
                    processManagerInfo.event.type === event.type &&
                    deepEqual(processManagerInfo.event.payload, event.payload)
                  ) {
                    return new InfiniteLoopError(
                      `Infinite loop detected. Process Manager "${processManagerInfo.name}" listens and emits same event "${event.type}" for stream ${type}:${streamId}, causing a recurring event cycle`,
                    )
                  }
                }

                await eventHandlers[`on${event.type}`]?.({
                  ...event,
                  streamId,
                  streamType: type,
                })
              }

              return [...previousEvents, ...baseInputEvents]
            })
            .catch((error) => {
              if (!(error instanceof Error)) {
                throw error
              }

              const commandError = new CommandError(
                type,
                commandName,
                streamId,
                error,
              )
              for (const [ErrorType, handler] of errorHandlers) {
                if (error instanceof ErrorType) {
                  return new CommandError(
                    type,
                    commandName,
                    streamId,
                    handler(commandError),
                  )
                }
              }

              return commandError
            })

          const currentCommandChain = commandChain
          currentCommandChain.then(async (allEvents) => {
            if (
              // we are the last command
              currentCommandChain === commandChain
            ) {
              stateReady.resolve(
                allEvents instanceof Error ? allEvents : undefined,
              )

              commandChain = commandChain.then(async () => {
                // no Error in chain
                if (Array.isArray(allEvents)) {
                  transactionDelay?.resolve()
                  try {
                    await transactionDelay?.commit()
                  } catch (error) {
                    if (error instanceof Error) {
                      warnReleaseLock()
                      return error
                    }
                  }

                  await processEvents(allEvents)
                  return []
                }

                warnReleaseLock()
                transactionDelay?.reject(allEvents)
                if (transaction.active) {
                  return []
                }

                throw allEvents
              })
            }
          })

          return api.commands
        },
      ]),
    )

    setImmediate(() => {
      if (!commandChainTouched) {
        releasedAfterChain = true
        commandChain = commandChain.then((v) => {
          releaseLock()
          return v
        })
      }
    })

    const api = {
      load(filter?: (event: BaseOutputEvent) => MaybePromise<boolean>) {
        // ensure commands are executed after load is finished
        commandChain = commandChain
          .then(() =>
            loadUntil(state, snapshotApi, streamId, undefined, filter),
          )
          .then((loadedVersion) => {
            version = loadedVersion
            return []
          })
      },
      commands: {
        ...commands,
        async state() {
          const maybeError = await Promise.race([
            stateReady.promise,
            awaitAll(() => commandChain),
          ])

          if (maybeError) {
            throw maybeError
          }

          return state
        },
        async settled() {
          const parentProcessManager = getProcessManagerInfo()
          if (parentProcessManager) {
            const error = new Error(
              `aggregateRoot.settled() cannot be called inside a process manager. Attempted on aggregate stream "${type}" within Process Manager ${parentProcessManager.name}.`,
            )
            commandChain = commandChain.then(() => error)
            throw error
          }

          if (transaction.active) {
            const error = new Error(
              `aggregateRoot.settled() cannot be called inside a transaction. Attempted on aggregate stream "${type}".`,
            )
            commandChain = commandChain.then(() => error)
            throw error
          }

          await awaitAll(() => commandChain)
          await downstreamSettledPromise()
          return api.commands
        },
        transformError(
          error: Constructor<Error>,
          handler: (error: Error) => never,
        ) {
          errorHandlers.set(error, handler)
          return api.commands
        },
        streamId,
      },
    }

    registerRootPromiseToUpstream(() => api.commands.settled())
    transaction.after(() => api.commands.settled())
    return api
  }

  const api: AggregateRootApi<Name> & InternalAggregateRootApi = {
    type,
    withInitialState: (state) => {
      initialState = clone(state)
      return api as any
    },

    withCommands(handlers) {
      createCommands = (state, eventBuilder, options) => {
        if (state === null) {
          return (handlers as any)(eventBuilder, options)
        }

        return (handlers as any)(state, eventBuilder, options)
      }

      return api as any
    },

    withEvents(handler) {
      const eventMap: BaseEventMap =
        typeof handler === 'function'
          ? handler({ version: (version, schema) => ({ version, schema }), z })
          : handler

      for (const [type, config] of Object.entries(eventMap)) {
        const versions = Array.isArray(config) ? config : [config]
        for (const v of versions) {
          let schema: z.ZodType
          let version: number
          if (typeof v === 'object' && 'version' in v) {
            schema = v.schema
            version = v.version
          } else {
            schema = v
            version = 0
          }

          if (registeredEvents.get(type)?.has(version)) {
            throw new Error(
              `event ${type} is already registered in version ${version}`,
            )
          }

          if (!registeredEvents.has(type)) {
            registeredEvents.set(type, new Map())
          }
          registeredEvents.get(type)!.set(version, schema)
        }
      }
      return api as any
    },

    withEventHandlers(handler) {
      createEventHandlers = handler as any
      return api as any
    },

    withSnapshots: (handler) => {
      createSnapshotApi = handler as any
      return api as any
    },

    newStream(id) {
      if (!loadEvents) {
        throw new Error(
          `aggregate root ${type} is not linked to an event store`,
        )
      }

      const parentProjectionName = getProjectionInfo()?.name
      if (parentProjectionName) {
        throw new Error(
          `aggregateRoot.newStream() cannot be called inside a projection. Attempted on aggregate stream ${type} within Projection ${parentProjectionName}.`,
        )
      }
      return createStream(clone(initialState), id ?? crypto.randomUUID())
        .commands as any
    },

    loadStream(id, filter) {
      if (!loadEvents) {
        throw new Error(
          `aggregate root ${type} is not linked to an event store`,
        )
      }

      const parentProjectionName = getProjectionInfo()?.name
      if (parentProjectionName) {
        throw new Error(
          `aggregateRoot.loadStream("${id}") cannot be called inside a projection. Attempted on aggregate stream ${type} within Projection ${parentProjectionName}.`,
        )
      }
      const root = createStream(clone(initialState), id)
      root.load(filter as any)
      return root.commands as any
    },

    linkEventStore(load, append, emit, trans, lock, snap) {
      loadEvents = load
      appendEvents = append
      emitEvents = emit
      transaction = trans
      createLock = lock
      snapshots = snap
    },
    get eventTypes() {
      return Array.from(registeredEvents.keys())
    },
    $inferEvents: null as never,
    $inferEvent: null as never,
    $inferState: null as never,
    $inferCommandParameters: null as never,
  }

  return api as Omit<
    AggregateRootApi<Name>,
    'withCommands' | 'withEventHandlers'
  >
}
