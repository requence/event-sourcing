---
'@requence/event-sourcing': minor
---

Expose the global log `position` on events passed to aggregate root event
handlers. The runtime always delivered it when folding persisted events during
stream loading and replay, but the handler type omitted it. It is typed as
optional because events yielded by a command fold before they are appended and
therefore have no position yet. This lets aggregates capture log positions in
their state (e.g. the position of a delete event, to later reconstruct the
dependency picture as it was just before the deletion).
