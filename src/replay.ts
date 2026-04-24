import { AsyncLocalStorage } from 'node:async_hooks'

import { ReplaySkipError } from './errors.ts'

const replayStore = new AsyncLocalStorage<boolean>()

export function isReplaying() {
  return replayStore.getStore() ?? false
}

export function withReplaying<T extends () => any>(handler: T) {
  return replayStore.run(true, handler)
}

export function skipReplay() {
  if (isReplaying()) {
    throw new ReplaySkipError()
  }
}
