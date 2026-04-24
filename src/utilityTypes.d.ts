import type z from 'zod/v4'

export type Prettify<T> = {
  [K in keyof T]: T[K]
} & {}

interface Stream {
  stream: string
}

interface StreamEvents extends Stream {
  events: string[]
}

interface StreamId extends Stream {
  id: string
}

interface StreamIdEvents extends StreamEvents {
  id: string
}

export type BaseInputEvent<Type extends string = string, Payload = unknown> = {
  type: Type
  payload: Payload
  metadata?: Record<string, unknown>
  causationId?: string
  correlationId?: string
  actorId?: string
  schemaVersion: number
}

type LatestEvent<
  Event extends EventTemplate,
  LatestVersion = Max<Event['version']>,
  Schema = Event extends { version: LatestVersion } ? Event['schema'] : never,
> = LatestVersion extends number
  ? BaseOutputEvent<Event['type'], LatestVersion, z.output<Schema>>
  : never

export type AllEvents<Event extends EventTemplate> = {
  [E in Event as E['type']]: LatestEvent<E>
}[Event['type']]

export type BaseOutputEvent<
  Type extends string = string,
  Version extends number = number,
  Payload = unknown,
> = {
  position: number
  type: Type
  schemaVersion: Version
  payload: Payload
  metadata?: Record<string, any>
  causationId?: string | null
  actorId?: string | null
  correlationId?: string | null
  streamId: string
  streamType: string
  streamVersion: number
  createdAt: Date
}

export type EventTemplate = { type: string; schema: z.ZodType; version: number }
export type OutputEvent<Event extends EventTemplate> = Prettify<
  {
    [K in Event as K['version']]: BaseOutputEvent<
      K['type'],
      K['version'],
      z.output<K['schema']>
    >
  }[Event['version']]
>

export type OutputEvents<Event extends EventTemplate> = Prettify<
  {
    [E in Event as E['type']]: OutputEvent<E>
  }[Event['type']]
>

export type Max<N extends number, A extends any[] = []> = [N] extends [
  Partial<A>['length'],
]
  ? A['length']
  : Max<N, [0, ...A]>

export type EventHandlers<Events extends EventTemplate, Helpers = never> = {
  [K in Events as `on${K['type']}`]?: (
    ...args: [Helpers] extends [never]
      ? [event: OutputEvent<K>]
      : [event: OutputEvent<K>, helpers: Helpers]
  ) => MaybePromise<void>
}

export type AfterEffectHandlers<
  Events extends EventTemplate,
  Helpers = never,
> = {
  [K in Events as `after${K['type']}`]?: (
    ...args: [Helpers] extends [never]
      ? [event: OutputEvent<K>]
      : [event: OutputEvent<K>, helpers: Helpers]
  ) => MaybePromise<void>
}

export type Merge<A, B> = Prettify<Omit<A, keyof B> & B>
export type Keys<T> = {
  [K in keyof T]: T[K] extends true ? K : never
}[keyof T]
export type MaybePromise<T> = T | Promise<T>
