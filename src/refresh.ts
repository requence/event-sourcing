import { AsyncLocalStorage } from 'node:async_hooks'

import { RefreshingSkipError } from './errors.ts'

const refreshStore = new AsyncLocalStorage<boolean>()

export function isRefreshing() {
  return refreshStore.getStore() ?? false
}

export function withRefreshing<T extends () => any>(handler: T) {
  return refreshStore.run(true, handler)
}

export function skipRefreshing() {
  if (isRefreshing()) {
    throw new RefreshingSkipError()
  }
}
