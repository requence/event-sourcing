---
'@requence/event-sourcing': patch
---

Loading a stream no longer loses its lock when the replay outlasts the lock's TTL. The lock was taken before the existing events were folded into the state and extended only by commands, so a long stream — or a slow database under load — let it expire mid-replay; a second writer then took it, and one of the two failed with a `ConcurrencyError` although neither command was slow. The lock is now kept alive on a timer while the replay runs, at a quarter of its TTL, and never beyond it, so a lock nobody releases still expires. Lock handles report their TTL through a new optional `ttl` field; a custom `LockCreator` without it is assumed to use the default 5 s.
