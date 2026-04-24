import z from 'zod/v4'

import type { BaseOutputEvent, OutputEvent } from './utilityTypes.js'

export class ConcurrencyError extends Error {
  constructor(
    streamId: { stream: string; id: string },
    expectedVersion: number,
    actualVersion: number,
  ) {
    super(
      `Concurrency error on ${streamId.stream}(${streamId.id}): expected v${expectedVersion}, actual v${actualVersion}`,
    )
  }
}

export class ValidationError extends Error {
  constructor(
    message: string,
    public readonly zodError: z.ZodError,
  ) {
    super(`${message}: ${z.prettifyError(zodError)}`)
  }
}

export class EventValidationError extends ValidationError {}

export class InfiniteLoopError extends Error {}

export class CommandError<T extends Error> extends Error {
  constructor(
    public readonly aggregateRootType: string,
    public readonly commandName: string,
    public readonly streamId: string,
    public readonly originalError: T,
  ) {
    super(
      `Error executing command "${commandName}" on aggregate root "${aggregateRootType}" for stream id "${streamId}": ${originalError.message}`,
    )
  }
}

export class ProjectionReplayLoopError extends Error {
  constructor(
    message: string,
    public readonly projection: { name: string; eventHandler: string },
    public readonly event: OutputEvent<any>,
  ) {
    super(message)
  }
}

export class RefreshingSkipError extends Error {}

export class ReplaySkipError extends Error {}

export class ProjectionEventHandlerExecutionError<
  T extends Error,
> extends Error {
  constructor(
    public readonly projection: string,
    public readonly eventHandler: string,
    public readonly event: BaseOutputEvent,
    public readonly originalError: T,
  ) {
    super(
      `Error execution event handler "${eventHandler}" in projection "${projection}": ${originalError.message}`,
    )
  }
}

export class ProcessManagerEventHandlerExecutionError<
  T extends Error,
> extends Error {
  constructor(
    public readonly processManager: string,
    public readonly eventHandler: string,
    public readonly event: BaseOutputEvent,
    public readonly originalError: T,
  ) {
    super(
      `Error execution event handler "${eventHandler}" in process manager "${processManager}": ${originalError.message}`,
    )
  }
}
