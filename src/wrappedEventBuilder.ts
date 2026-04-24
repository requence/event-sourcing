import type { z } from 'zod/v4'

import type { EventValidationError } from './errors.ts'
import type {
  BaseInputEvent,
  EventTemplate,
  Max,
  MaybePromise,
} from './utilityTypes.js'

export type WrappedEvent<E extends EventTemplate = EventTemplate> = {
  withCausationId(causationId: string | null | undefined): WrappedEvent<E>
  withDefaultCausationId(
    causationId: string | null | undefined,
  ): WrappedEvent<E>
  withCorrelationId(correlationId: string | null | undefined): WrappedEvent<E>
  withDefaultCorrelationId(
    correlationId: string | null | undefined,
  ): WrappedEvent<E>
  withActorId(actorId: string | null | undefined): WrappedEvent<E>
  withDefaultActorId(actorId: string | null | undefined): WrappedEvent<E>
  withMetadata(
    metadata: Record<string, any> | null | undefined,
  ): WrappedEvent<E>
  withDefaultMetadata(
    metadata: Record<string, any> | null | undefined,
  ): WrappedEvent<E>
  withAdditionalMetadata(
    metadata: Record<string, any> | null | undefined,
  ): WrappedEvent<E>
  onValidationError(
    errorHandler: (error: EventValidationError) => MaybePromise<void>,
  ): WrappedEvent<E>
  readonly type: string
}

const eventInternalsSymbol = Symbol.for('event-internals')

function getInternals(event: WrappedEvent) {
  return (event as any)[eventInternalsSymbol] as {
    event: Omit<BaseInputEvent, 'schemaVersion'>
    errorHandler?: (error: EventValidationError) => MaybePromise<void>
  }
}

export function unwrapEvent<E extends EventTemplate = EventTemplate>(
  event: WrappedEvent<E>,
) {
  return getInternals(event).event as {
    [K in E as K['type']]: Omit<
      BaseInputEvent<K['type'], z.output<K['schema']>>,
      'schemaVersion'
    >
  }[E['type']]
}

export async function handleEventError(
  event: WrappedEvent,
  error: EventValidationError,
) {
  await getInternals(event).errorHandler?.(error)
}

type LatestSchema<
  Event extends EventTemplate,
  LatestVersion = Max<Event['version']>,
  Schema = Event extends { version: LatestVersion } ? Event['schema'] : never,
> = LatestVersion extends number ? z.input<Schema> : never

type EventMap<T extends EventTemplate> = {
  [K in T as K['type']]: LatestSchema<K>
}

export type WrappedEventBuilder<Event extends EventTemplate> = {
  <
    Type extends Event['type'],
    LatestVersion = Max<
      Event extends { type: Type } ? Event['version'] : never
    >,
    Schema = Event extends { type: Type; version: LatestVersion }
      ? Event['schema']
      : never,
  >(
    ...args: z.input<Schema> extends null
      ? [type: Type, payload?: null]
      : [type: Type, payload: z.input<Schema>]
  ): WrappedEvent
  $inferPayload: EventMap<Event>
}

export function wrappedEventBuilder(
  type: string,
  payload: unknown,
): WrappedEvent
export function wrappedEventBuilder(event: BaseInputEvent): WrappedEvent
export function wrappedEventBuilder(
  type: string | BaseInputEvent,
  payload?: unknown,
): WrappedEvent {
  let errorHandler: (error: EventValidationError) => MaybePromise<void>
  const event: Omit<BaseInputEvent, 'schemaVersion'> =
    typeof type === 'string'
      ? {
          type,
          payload: payload ?? null,
        }
      : type

  return {
    withCausationId(causationId) {
      if (causationId) {
        event.causationId = causationId
      }
      return this
    },
    withDefaultCausationId(causationId) {
      if (causationId && !event.causationId) {
        event.causationId = causationId
      }
      return this
    },
    withCorrelationId(correlationId) {
      if (correlationId) {
        event.correlationId = correlationId
      }
      return this
    },
    withDefaultCorrelationId(correlationId) {
      if (correlationId && !event.correlationId) {
        event.correlationId = correlationId
      }
      return this
    },
    withActorId(actorId) {
      if (actorId) {
        event.actorId = actorId
      }
      return this
    },
    withDefaultActorId(actorId) {
      if (actorId && !event.actorId) {
        event.actorId = actorId
      }
      return this
    },
    withMetadata(metadata) {
      if (metadata) {
        event.metadata = metadata
      }
      return this
    },
    withDefaultMetadata(metadata) {
      if (metadata && !event.metadata) {
        event.metadata = metadata
      }
      return this
    },
    withAdditionalMetadata(metadata) {
      if (!metadata) {
        return this
      }
      if (!event.metadata) {
        event.metadata = metadata
      } else {
        event.metadata = Object.assign(event.metadata, metadata)
      }

      return this
    },
    onValidationError(handler) {
      errorHandler = handler
      return this
    },

    get [eventInternalsSymbol]() {
      return { event, errorHandler }
    },

    type,
  } as WrappedEvent
}
