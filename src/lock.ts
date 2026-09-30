import { Mutex } from 'async-mutex'

export const DEFAULT_LOCK_TTL = 5000

export type Lock = {
  /** Moves the expiry to `ttl` from now (default: half the lock's TTL). */
  extend(ttl?: number): Promise<boolean>
  release(): Promise<void>
  /**
   * The TTL the lock was taken with. The aggregate root keeps a replay's lock
   * alive at a quarter of it, and assumes {@link DEFAULT_LOCK_TTL} without it.
   */
  ttl?: number
}

export type LockCreator = (key: string | string[]) => Promise<Lock>

export default function lock(defaultTtl = DEFAULT_LOCK_TTL): LockCreator {
  const locks = new Map<string, Mutex>()
  return async (key: string | string[]) => {
    const k = Array.isArray(key) ? key.join(':') : key
    if (!locks.has(k)) {
      locks.set(k, new Mutex())
    }

    const mutex = locks.get(k)!

    const releaseMutex = await mutex.acquire()
    let isReleased = false

    let timer: ReturnType<typeof setTimeout>
    const startTimer = (ttl: number) => {
      timer = setTimeout(() => {
        if (!isReleased) {
          console.warn('lock ttl expired for key', k)
          isReleased = true
          releaseMutex()
          if (!mutex.isLocked()) {
            locks.delete(k)
          }
        }
      }, ttl)
    }

    startTimer(defaultTtl)

    return {
      ttl: defaultTtl,
      async extend(ttl?: number) {
        if (isReleased) {
          return false
        }
        clearTimeout(timer)
        startTimer(ttl ?? defaultTtl / 2)
        return true
      },
      async release() {
        if (isReleased) {
          return
        }

        isReleased = true
        clearTimeout(timer)
        releaseMutex()

        if (!mutex.isLocked()) {
          locks.delete(k)
        }
      },
    }
  }
}
