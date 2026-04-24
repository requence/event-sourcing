import z from 'zod/v4'

import type { LoadEvents } from './createEventStore.ts'
import { ValidationError } from './errors.ts'
import type {
  BaseOutputEvent,
  MaybePromise,
  StreamEvents,
} from './utilityTypes.js'

const checkpointSchema = z.object({
  lastEventPosition: z.int(),
  type: z.enum(['projection', 'processManager']),
  name: z.string(),
  metadata: z.record(z.string(), z.any()),
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
  upsert: (checkpoint: Checkpoint) => MaybePromise<void>
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
  const api = {
    relatedStreamEvents: params.relatedStreamEvents,
    async clear() {
      await params.delete(params.type, params.name)
    },
    async upsert(event: BaseOutputEvent, metadata: Record<string, any> = {}) {
      await params.upsert({
        lastEventPosition: event.position,
        type: params.type,
        name: params.name,
        metadata,
      })
    },
    async rehydrated(
      options: {
        onMetadata?: (metadata: Record<string, any>) => unknown
        getMetadata?: () => Record<string, any>
        onProgress?: (index: number, event: BaseOutputEvent) => void
      } = {},
    ) {
      const checkpoint = await params.get(params.type, params.name)
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
