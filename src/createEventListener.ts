import { AsyncLocalStorage } from 'node:async_hooks'

import type {
  AfterEffectHandlers,
  AllEvents,
  BaseOutputEvent,
  EventHandlers,
  EventTemplate,
  Keys,
  MaybePromise,
  Merge,
} from './utilityTypes.js'

export type InternalEventListener = {
  apply: (event: BaseOutputEvent) => Promise<void>
  applyAfter: (event: BaseOutputEvent) => Promise<void>
}

type EventListenerInfo = {
  name: string | null
  event: BaseOutputEvent
}
const eventListenerInfoStore = new AsyncLocalStorage<EventListenerInfo>()

function withEventListenerInfo(
  info: EventListenerInfo,
  handler: () => Promise<void>,
) {
  return eventListenerInfoStore.run(info, handler)
}

export function getEventListenerInfo() {
  return eventListenerInfoStore.getStore()
}

export function isInsideEventListener() {
  return Boolean(getEventListenerInfo())
}

export type EventListenerApi<
  Events extends EventTemplate = never,
  Flags extends {
    withEventHandlers: boolean
    withGlobalEventHandler: boolean
    withAfterEffects: boolean
    withGlobalAfterEffect: boolean
  } = {
    withEventHandlers: false
    withGlobalEventHandler: false
    withAfterEffects: false
    withGlobalAfterEffect: false
  },
> = {
  withEventHandlers: (
    handlers: EventHandlers<Events>,
  ) => Omit<
    EventListenerApi<Events, Merge<Flags, { withEventHandlers: true }>>,
    Keys<Merge<Flags, { withEventHandlers: true }>>
  >
  withGlobalEventHandler: (
    handler: (event: AllEvents<Events>) => MaybePromise<void>,
  ) => Omit<
    EventListenerApi<Events, Merge<Flags, { withGlobalEventHandler: true }>>,
    Keys<Merge<Flags, { withGlobalEventHandler: true }>>
  >
  withAfterEffects: (
    handlers: AfterEffectHandlers<Events>,
  ) => Omit<
    EventListenerApi<Events, Merge<Flags, { withAfterEffects: true }>>,
    Keys<Merge<Flags, { withAfterEffects: true }>>
  >
  withGlobalAfterEffect: (
    handler: (event: AllEvents<Events>) => MaybePromise<void>,
  ) => Omit<
    EventListenerApi<Events, Merge<Flags, { withGlobalAfterEffect: true }>>,
    Keys<Merge<Flags, { withGlobalAfterEffect: true }>>
  >
}

export function createEventListener(
  name: string | null = null,
): EventListenerApi {
  let eventHandlers: EventHandlers<any> = {}
  let afterEffectHandlers: AfterEffectHandlers<any> = {}
  let globalEventHandler: (event: BaseOutputEvent) => MaybePromise<void>
  let globalAfterEffect: (event: BaseOutputEvent) => MaybePromise<void>
  const api = {
    async apply(event) {
      withEventListenerInfo({ name, event }, async () => {
        await eventHandlers[`on${event.type}`]?.(event)
        await globalEventHandler?.(event)
      })
    },

    async applyAfter(event) {
      await afterEffectHandlers[`after${event.type}`]?.(event)
      await globalAfterEffect?.(event)
    },
    withEventHandlers(handlers) {
      eventHandlers = handlers
      return api
    },
    withGlobalEventHandler(handler) {
      globalEventHandler = handler as any
      return api
    },
    withAfterEffects(handlers) {
      afterEffectHandlers = handlers
      return api
    },
    withGlobalAfterEffect(handler) {
      globalAfterEffect = handler as any
      return api
    },
  } as EventListenerApi & InternalEventListener

  return api
}
