import type { Redis } from 'ioredis'

import { DEFAULT_LOCK_TTL, type LockCreator } from '../lock.ts'

const extendScript = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("pexpire", KEYS[1], ARGV[2])
  else
      return 0
  end
`

const deleteScript = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
`

export function redisLock(
  client: Redis,
  defaultTtl = DEFAULT_LOCK_TTL,
): LockCreator {
  return async (rawKey) => {
    const key = Array.isArray(rawKey) ? rawKey.join(':') : rawKey
    const token = crypto.randomUUID()

    while (true) {
      const acquired = await client.set(key, token, 'PX', defaultTtl, 'NX')

      if (acquired !== 'OK') {
        await new Promise((resolve) => setTimeout(resolve, 100))
        continue
      }

      let isReleased = false
      return {
        ttl: defaultTtl,
        async extend(ttl) {
          const extended = await client.eval(
            extendScript,
            1,
            key,
            token,
            ttl ?? defaultTtl / 2,
          )
          if (extended === 0) {
            isReleased = true
            return false
          }
          return true
        },
        async release() {
          if (isReleased) {
            return
          }

          isReleased = true
          await client.eval(deleteScript, 1, key, token)
        },
      }
    }
  }
}
