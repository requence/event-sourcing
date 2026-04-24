import { Mutex } from 'async-mutex'

export default function lock(defaultTtl = 5000) {
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

export type LockCreator = ReturnType<typeof lock>
