import z from 'zod/v4'

import type { LoadEvents } from './createEventStore.ts'
import { CheckpointConcurrencyError, ValidationError } from './errors.ts'
import type {
  BaseOutputEvent,
  MaybePromise,
  StreamEvents,
} from './utilityTypes.ts'

const checkpointSchema = z.object({
  lastEventPosition: z.int(),
  type: z.enum(['projection', 'processManager']),
  name: z.string(),
  metadata: z.record(z.string(), z.any()),
  version: z.int(),
})

export type Checkpoint = z.output<typeof checkpointSchema>

export type CheckpointMethods = {
  get: (
    type: 'projection' | 'processManager',
    name: string,
  ) => MaybePromise<Checkpoint | null | undefined>
  delete: (
    type: 'projection' | 'processManager',
    name: string,
  ) => MaybePromise<void>
  /**
   * Persist a checkpoint using optimistic concurrency control.
   *
   * The write must only succeed if the currently stored version equals
   * `expectedVersion` (or, when `expectedVersion` is `null`, if no checkpoint
   * exists yet). `checkpoint.version` holds the version to store on success.
   * Return `true` when the write was applied and `false` when it was rejected
   * because another writer had advanced the checkpoint in the meantime.
   */
  upsert: (
    checkpoint: Checkpoint,
    expectedVersion: number | null,
  ) => MaybePromise<boolean>
}

export function createCheckpointApi(
  params: CheckpointMethods & {
    loadEvents: LoadEvents
    relatedStreamEvents: StreamEvents[] | null
    name: string
    type: 'processManager' | 'projection'
    apply: (event: BaseOutputEvent) => MaybePromise<void>
    applyAfter?: (event: BaseOutputEvent) => MaybePromise<void>
  },
) {
  // In-memory view of the persisted checkpoint version. `null` means "no
  // checkpoint has been written yet". It is kept in sync with the store on
  // every load/upsert so optimistic-concurrency writes can detect when another
  // instance has advanced the same checkpoint.
  let version: number | null = null

  const api = {
    relatedStreamEvents: params.relatedStreamEvents,
    async clear() {
      await params.delete(params.type, params.name)
      version = null
    },
    /**
     * Load the persisted checkpoint and sync the local version. Returns the
     * raw checkpoint (or `null`), so callers can rehydrate their own state
     * (e.g. a process manager reloading its folded state) from it.
     */
    async load() {
      const checkpoint = await params.get(params.type, params.name)
      version = checkpoint?.version ?? null
      return checkpoint ?? null
    },
    /**
     * Persist the checkpoint position (and optional metadata) for the given
     * event.
     *
     * - `'cas'` (compare-and-swap): the write must land on top of the exact
     *   version we last observed. If another writer advanced the checkpoint,
     *   the local version is refreshed and a {@link CheckpointConcurrencyError}
     *   is thrown so the caller can re-fold its state and retry. Used for
     *   process managers that fold state into the checkpoint.
     * - `'monotonic'` (default): retries on conflict, skipping the write once
     *   the stored position already covers this event. Safe for cursors with
     *   no derived state (projections, stateless/after-effect writes).
     */
    async upsert(
      event: BaseOutputEvent,
      metadata: Record<string, any> = {},
      mode: 'cas' | 'monotonic' = 'monotonic',
    ) {
      while (true) {
        const expectedVersion = version
        const nextVersion = (expectedVersion ?? -1) + 1
        const committed = await params.upsert(
          {
            lastEventPosition: event.position,
            type: params.type,
            name: params.name,
            metadata,
            version: nextVersion,
          },
          expectedVersion,
        )

        if (committed) {
          version = nextVersion
          return
        }

        // Another writer advanced this checkpoint. Refresh our view of it.
        const current = await api.load()

        if (mode === 'cas') {
          throw new CheckpointConcurrencyError(
            params.type,
            params.name,
            expectedVersion,
          )
        }

        // monotonic: the stored cursor already covers this event, nothing to do.
        if (current && current.lastEventPosition >= event.position) {
          return
        }
      }
    },
    async rehydrated(
      options: {
        onMetadata?: (metadata: Record<string, any>) => unknown
        getMetadata?: () => Record<string, any>
        onProgress?: (index: number, event: BaseOutputEvent) => void
      } = {},
    ) {
      const checkpoint = await api.load()
      let startPosition = 0
      if (checkpoint) {
        const result = checkpointSchema.safeParse(checkpoint)
        if (!result.success) {
          throw new ValidationError(
            `loaded checkpoint for ${params.type} ${params.name} does not comply with schema`,
            result.error,
          )
        }
        startPosition = checkpoint.lastEventPosition + 1
        options.onMetadata?.(checkpoint.metadata)
      }

      const events = params.loadEvents(params.relatedStreamEvents, {
        from: startPosition,
      })

      let index = 0
      let lastEvent: BaseOutputEvent | null = null
      for await (const event of events) {
        lastEvent = event
        await params.apply(event)
        index++
        options.onProgress?.(index, event)
      }

      if (!lastEvent) {
        return // up to date
      }

      if (params.applyAfter) {
        for await (const event of events) {
          await params.applyAfter(event)
        }
      }

      await api.upsert(lastEvent, options.getMetadata?.())
    },
  }

  return api
}

export type CheckpointApi = ReturnType<typeof createCheckpointApi>
