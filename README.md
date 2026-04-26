# @requence/event-sourcing

A high-performance, type-safe event sourcing library for TypeScript.

Build resilient, auditable systems by storing every state change as an immutable event — then derive your current state by replaying them.

## Features

- **Aggregate Roots** — define your domain model with events, business rules, and commands using a fluent builder API.
- **Projections** — build query-optimized read models from events, with full replay support.
- **Process Managers** — coordinate workflows across multiple aggregates.
- **Event Listeners** — react to events with lightweight, stateless side effects.
- **Storage Adapters** — swap between an in-memory store (great for testing), PostgreSQL via Drizzle, or Redis.
- **Type-Safe Events** — leverage Zod schemas and TypeScript for compile-time and runtime guarantees on every event shape.

## Install

```bash
npm install @requence/event-sourcing zod
```

## Quick Start

```typescript
import { createAggregateRoot } from '@requence/event-sourcing'
import { createEventStore } from '@requence/event-sourcing/memory'

// 1. Define an aggregate root
const counter = createAggregateRoot('counter')
  .withInitialState({ count: 0 })
  .withEvents(({ z }) => ({
    Incremented: z.object({ amount: z.number() }),
  }))
  .withEventHandlers((state) => ({
    onIncremented({ payload }) {
      state.count += payload.amount
    },
  }))
  .withCommands((state, event) => ({
    increment(amount: number) {
      return event('Incremented', { amount })
    },
  }))

// 2. Create an event store with the aggregate root
const eventStore = createEventStore({
  aggregateRoots: [counter],
})

// 3. Build a read model with a projection
const totals = new Map<string, number>()

eventStore.createProjection('counter-totals').withEventHandlers({
  onIncremented({ streamId, payload }) {
    totals.set(streamId, (totals.get(streamId) ?? 0) + payload.amount)
  },
})

// 4. Execute commands on a new stream
const stream = await counter.newStream().increment(1).increment(5).settled()
console.log(totals.get(stream.streamId)) // 6
```

## Storage Adapters

| Adapter | Import Path | Use Case |
|---------|------------|----------|
| In-Memory | `@requence/event-sourcing/memory` | Testing & prototyping |
| PostgreSQL (Drizzle) | `@requence/event-sourcing/drizzle/postgres` | Production |

## Documentation

Full documentation — including concept guides, a step-by-step tutorial, and API reference — is available at:

**[https://event-sourcing.docs.requence.cloud](https://event-sourcing.docs.requence.cloud)**

## License

[MIT](./LICENSE)
