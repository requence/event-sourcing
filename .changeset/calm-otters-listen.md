---
'@requence/event-sourcing': patch
---

Forward the `lock` option through the drizzle and memory adapter wrappers to
the core event store. The option was documented (e.g. `redisLock` for
multi-instance deployments) but both adapters silently dropped it, so custom
locking strategies never took effect and the default in-memory lock was always
used.
