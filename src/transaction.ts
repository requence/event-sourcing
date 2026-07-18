import { AsyncLocalStorage } from 'node:async_hooks'

import type { SettledOptions } from './createAggregateRoot.ts'

async function awaitAll(getPromises: () => Promise<any>[]) {
  let promises = getPromises()

  while (true) {
    await Promise.all(promises)
    const nextPromises = getPromises()
    if (nextPromises.length > promises.length) {
      promises = nextPromises
    } else {
      break
    }
  }
}

export type TransactionDelay = {
  resolve: () => void
  reject: (error: Error) => void
  commit: () => Promise<void>
}

/**
 * Options for a transaction run.
 *
 * `settled` — default {@link SettledOptions} applied when the transaction
 * auto-settles the streams whose commands were dispatched inside the handler
 * (explicit `settled()` calls are not allowed there).
 */
export type TransactionOptions = {
  settled?: SettledOptions
}

export default function createTransaction() {
  const transactionStore = new AsyncLocalStorage<{
    delayPromises: Promise<void>[]
    commitPromises: Promise<void>[]
    afterHandlers: Array<() => Promise<any>>
    settledDefaults?: SettledOptions
  }>()

  return {
    get active() {
      return Boolean(transactionStore.getStore())
    },

    get settledDefaults() {
      return transactionStore.getStore()?.settledDefaults
    },

    /**
     * Runs the handler outside the transaction scope. Used by the settle
     * retry: a retried append already passed the transaction gate on its
     * first attempt and must not re-enter the (committed) transaction.
     */
    exit<T>(handler: () => T): T {
      return transactionStore.exit(handler)
    },

    delay(): TransactionDelay {
      const store = transactionStore.getStore()
      if (!store) {
        return {
          resolve: () => {},
          reject: () => {},
          commit: () => Promise.resolve(),
        }
      }

      const idx = store.commitPromises.length
      const { resolve, reject, promise } = Promise.withResolvers<void>()
      store.delayPromises.push(promise)

      const { promise: commitPromise, resolve: resolveCommitPromise } =
        Promise.withResolvers<void>()
      store.commitPromises.push(commitPromise)

      return {
        resolve,
        reject,
        async commit() {
          if (idx > 0) {
            await store.commitPromises.at(idx - 1)
          }

          resolveCommitPromise()
          await awaitAll(() => store.delayPromises)
        },
      }
    },

    after(onAfter: () => Promise<any>) {
      transactionStore.getStore()?.afterHandlers.push(onAfter)
    },

    async run<T extends () => any>(handler: T, options?: TransactionOptions) {
      const { resolve, reject, promise } = Promise.withResolvers<void>()
      const delayPromises = [promise]
      const afterHandlers: Array<() => Promise<any>> = []
      let result
      try {
        result = await transactionStore.run(
          {
            delayPromises,
            afterHandlers,
            commitPromises: [],
            settledDefaults: options?.settled,
          },
          handler,
        )
        resolve()
      } catch (error) {
        reject(error)
      }
      await awaitAll(() => delayPromises)
      await Promise.all(afterHandlers.map((handler) => handler()))

      return result
    },
  }
}

export type Transaction = ReturnType<typeof createTransaction>
