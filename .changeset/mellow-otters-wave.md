---
'@requence/event-sourcing': patch
---

Fix a typo in the drizzle adapter's `projectionSnapshot.get` that called `super.parse` instead of `superjson.parse` when deserializing snapshot metadata. Reading back a projection snapshot previously threw a `TypeError` at runtime.
