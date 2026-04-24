import type { MaybePromise } from './utilityTypes.js'

export async function normalizeArray<T>(
  result: MaybePromise<void | T | T[]> | Iterable<T> | AsyncIterable<T>,
  onItem?: (item: T) => void,
): Promise<T[]> {
  const awaited = (await result) as any

  // If it's explicitly void/undefined, normalize to empty array
  if (typeof awaited === 'undefined') {
    return []
  }

  // Async iterable
  if (typeof awaited[Symbol.asyncIterator] === 'function') {
    const out: T[] = []
    for await (const item of awaited as AsyncIterable<T>) {
      out.push(item)
      onItem?.(item)
    }
    return out
  }

  // Sync iterable (this also covers arrays)
  if (typeof awaited[Symbol.iterator] === 'function') {
    return Array.from(awaited as Iterable<T>)
  }

  // Plain single value
  return [awaited as T]
}

export async function awaitAll(getPromise: () => Promise<any>) {
  let currentPromise = getPromise()
  while (true) {
    await currentPromise
    const nextPromise = getPromise()
    if (nextPromise === currentPromise) {
      break
    }
    currentPromise = nextPromise
  }
}
