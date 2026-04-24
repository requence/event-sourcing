import type {
  Stream,
  StreamEvents,
  StreamId,
  StreamIdEvents,
} from './utilityTypes.d.ts'

export { createEventStore } from './createEventStore.ts'
export type { BaseOutputEvent as Event } from './utilityTypes.d.ts'
export {
  type Snapshot as ProjectionSnapshot,
  isInsideProjection,
  getProjectionInfo,
} from './createProjection.ts'
export type { Checkpoint } from './createCheckpointApi.ts'
export {
  isInsideProcessManager,
  getProcessManagerInfo,
} from './createProcessManager.ts'
export {
  isInsideEventListener,
  getEventListenerInfo,
} from './createEventListener.ts'
export * from './errors.ts'
export {
  type Snapshot as AggregateRootSnapshot,
  type AnyAggregateRoot,
  type AnyStream,
  createAggregateRoot,
} from './createAggregateRoot.ts'

export { skipRefreshing } from './refresh.ts'
export { skipReplay } from './replay.ts'

export function isStreamId(
  stream: Stream,
): stream is StreamId | StreamIdEvents {
  return 'id' in stream
}
export function isStreamEvents(
  stream: Stream,
): stream is StreamEvents | StreamIdEvents {
  return 'events' in stream && Array.isArray(stream.events)
}
export type { WrappedEvent } from './wrappedEventBuilder.ts'
