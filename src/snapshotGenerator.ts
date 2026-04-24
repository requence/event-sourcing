import { z } from 'zod/v4'

import { ValidationError } from './errors.ts'
import type {
  BaseOutputEvent,
  Keys,
  Max,
  MaybePromise,
  Merge,
} from './utilityTypes.js'

type RegisteredSnapshotSchemas = { schemaVersion: number; schema: z.ZodType }

type LatestSnapshotSchemaInput<
  T extends RegisteredSnapshotSchemas,
  M = Max<T['schemaVersion']>,
> = T extends { schemaVersion: M } ? z.input<T['schema']> : never

type OutputSnapshot<T extends RegisteredSnapshotSchemas> = T extends {
  schemaVersion: infer V
  schema: infer S
}
  ? {
      schemaVersion: V
      state: z.output<S>
      createdAt: Date
    }
  : never

type SnapshotApplier<Base, T extends RegisteredSnapshotSchemas> = (
  snapshot: OutputSnapshot<T> & Base,
) => MaybePromise<void>
type SnapshotCapturer<T extends RegisteredSnapshotSchemas> = (
  event: BaseOutputEvent,
) => MaybePromise<LatestSnapshotSchemaInput<T> | undefined | null | void>

export type SnapshotGenerator<
  Base,
  Schemas extends RegisteredSnapshotSchemas = never,
  Flags extends {
    every: boolean
    apply: boolean
    capture: boolean
    schema: boolean
  } = {
    every: false
    apply: true
    capture: true
    schema: false
  },
> = {
  every(
    count: number,
  ): Omit<
    SnapshotGenerator<Base, Schemas, Merge<Flags, { every: true }>>,
    Keys<Merge<Flags, { every: true }>>
  >
  schema<Schema extends z.ZodType, V extends number = 0>(
    schema: Schema,
    schemaVersion?: V,
  ): Omit<
    SnapshotGenerator<
      Base,
      Schemas | { schemaVersion: V; schema: Schema },
      Merge<Flags, { apply: false; capture: false }>
    >,
    Keys<Merge<Flags, { apply: false; capture: false }>>
  >
  apply(
    applier: SnapshotApplier<Base, Schemas>,
  ): Omit<
    SnapshotGenerator<
      Base,
      Schemas,
      Merge<Flags, { apply: true; schema: true }>
    >,
    Keys<Merge<Flags, { apply: true; schema: true }>>
  >
  capture(
    capturer: SnapshotCapturer<Schemas>,
  ): Omit<
    SnapshotGenerator<
      Base,
      Schemas,
      Merge<Flags, { capture: true; schema: true }>
    >,
    Keys<Merge<Flags, { capture: true; schema: true }>>
  >
  readonly complete: Flags['schema'] extends true
    ? Flags['capture'] extends true
      ? Flags['apply'] extends true
        ? true
        : false
      : false
    : false
}

export type InternalSnapshotGenerator = {
  internalApply: (snapshot: OutputSnapshot<any>) => Promise<void>
  internalCapture: (
    event: BaseOutputEvent,
    applied: number,
  ) => Promise<null | { state: any; version: number }>
}

export type CompleteSnapshotGenerator = Omit<
  SnapshotGenerator<
    any,
    RegisteredSnapshotSchemas,
    { every: true; apply: true; capture: true; schema: true }
  >,
  'schema' | 'capture' | 'apply' | 'every'
>

export default function createSnapshotGenerator<Base>(
  schemas: Array<RegisteredSnapshotSchemas> = [],
  config: { every: number } | null = null,
  applier:
    | ((snapshot: OutputSnapshot<any>) => MaybePromise<void>)
    | null = null,
  capturer: ((event: BaseOutputEvent) => any) | null = null,
): CompleteSnapshotGenerator {
  return {
    async internalApply(snapshot) {
      const result = z
        .union(schemas.map(({ schema }) => schema))
        .safeParse(snapshot.state)

      if (!result.success) {
        throw new ValidationError(
          `loaded snapshot version ${snapshot.schemaVersion} does not comply with registered schema`,
          result.error,
        )
      }

      await applier!(snapshot)
    },
    async internalCapture(event, applied) {
      if (!config || applied % config.every !== 0) {
        return null
      }

      const latestSchema = schemas
        .slice(1)
        .reduce(
          (latest, schema) =>
            schema.schemaVersion > latest.schemaVersion ? schema : latest,
          schemas[0],
        )

      const result = latestSchema.schema.safeParse(await capturer!(event))

      if (!result.success) {
        throw new ValidationError(
          `generated snapshot does not comply with schema for version ${latestSchema.schemaVersion}`,
          result.error,
        )
      }

      return {
        state: result.data,
        version: latestSchema.schemaVersion,
      }
    },
    every(count) {
      return createSnapshotGenerator(
        schemas,
        { every: count },
        applier,
        capturer,
      )
    },
    schema(schema, schemaVersion) {
      schemas.push({ schemaVersion: schemaVersion ?? 0, schema })
      return createSnapshotGenerator(schemas, config, null, null) as any
    },
    apply(applier) {
      return createSnapshotGenerator(schemas, config, applier as any, capturer)
    },
    capture(capturer) {
      return createSnapshotGenerator(schemas, config, applier, capturer as any)
    },
    complete: true,
  } as SnapshotGenerator<
    Base,
    RegisteredSnapshotSchemas,
    { capture: true; apply: true; every: true; schema: true }
  > &
    InternalSnapshotGenerator
}
