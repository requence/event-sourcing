---
'@requence/event-sourcing': patch
---

Projections no longer apply the same event twice when several instances share one backing store. Each instance decided what it had already applied from a cursor only it could see, so two instances catching up over the same backlog — or one catching up while the other applied an event it had just appended — both ran the same handler, which showed up in consumers as a duplicate-key error or, where a handler applies a delta, as a silently wrong number. An event is now claimed through the checkpoint's compare-and-swap before its handler runs, so only one instance applies it. The claim is released again if that handler fails, so a failing handler still leaves its event to be retried rather than silently skipped.
