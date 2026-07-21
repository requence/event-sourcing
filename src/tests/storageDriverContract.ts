import { describe, expect, it } from 'bun:test'

import type {
  Snapshot as AggregateRootSnapshot,
  AggregateRootSnapshotMethods,
} from '../createAggregateRoot.ts'
import type { Checkpoint, CheckpointMethods } from '../createCheckpointApi.ts'
import type { LoadEvents } from '../createEventStore.ts'
import type {
  Snapshot as ProjectionSnapshot,
  ProjectionSnapshotMethods,
} from '../createProjection.ts'
import { ConcurrencyError } from '../errors.ts'
import type {
  BaseInputEvent,
  BaseOutputEvent,
  MaybePromise,
  StreamId,
} from '../utilityTypes.ts'

export type StorageDriver = {
  loadEvents: LoadEvents
  appendEvents: (
    streamId: StreamId,
    events: BaseInputEvent[],
    expectedVersion: number,
  ) => MaybePromise<BaseOutputEvent[]>
  checkpoint: CheckpointMethods
  projectionSnapshot: ProjectionSnapshotMethods
  aggregateRootSnapshot: AggregateRootSnapshotMethods
}

function inputEvent(
  type: string,
  payload: unknown,
  overrides?: Partial<BaseInputEvent>,
): BaseInputEvent {
  return {
    type,
    payload,
    metadata: {},
    schemaVersion: 1,
    ...overrides,
  } as BaseInputEvent
}

// Works for drivers that throw synchronously (memory) and asynchronously.
async function expectConcurrencyError(run: () => MaybePromise<unknown>) {
  let error: unknown
  try {
    await run()
  } catch (err) {
    error = err
  }
  expect(error).toBeInstanceOf(ConcurrencyError)
}

async function collect(events: AsyncGenerator<BaseOutputEvent>) {
  const resolved: BaseOutputEvent[] = []
  for await (const event of events) {
    resolved.push(event)
  }
  return resolved
}

function checkpointFixture(overrides?: Partial<Checkpoint>): Checkpoint {
  return {
    type: 'projection',
    name: 'test',
    lastEventPosition: 1,
    metadata: {},
    version: 1,
    ...overrides,
  }
}

/**
 * Reusable contract suite exercising the raw storage methods every driver
 * must supply to the core `createEventStore`. Call it once per driver with a
 * factory producing a fresh, empty storage instance.
 */
export function runStorageDriverContract(
  name: string,
  makeStorage: () => MaybePromise<StorageDriver>,
) {
  describe(`storage driver contract: ${name}`, () => {
    describe('appendEvents', () => {
      it('persists events with stream versions, positions and createdAt', async () => {
        const storage = await makeStorage()
        const persisted = await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [
            inputEvent('created', { total: 1 }),
            inputEvent('updated', { total: 2 }),
          ],
          0,
        )

        expect(persisted).toHaveLength(2)
        expect(persisted[0].streamVersion).toBe(1)
        expect(persisted[1].streamVersion).toBe(2)
        expect(persisted[0].streamId).toBe('o1')
        expect(persisted[0].streamType).toBe('order')
        expect(persisted[1].position).toBeGreaterThan(persisted[0].position)
        expect(persisted[0].createdAt).toBeInstanceOf(Date)
        expect(persisted[0].payload).toEqual({ total: 1 })
      })

      it('continues stream versions across appends', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', {})],
          0,
        )
        const second = await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('updated', {})],
          1,
        )

        expect(second[0].streamVersion).toBe(2)
      })

      it('throws ConcurrencyError when the expected version is stale', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', {})],
          0,
        )

        await expectConcurrencyError(() =>
          storage.appendEvents(
            { stream: 'order', id: 'o1' },
            [inputEvent('updated', {})],
            0,
          ),
        )
      })

      it('throws ConcurrencyError when expecting events on a fresh stream', async () => {
        const storage = await makeStorage()

        await expectConcurrencyError(() =>
          storage.appendEvents(
            { stream: 'order', id: 'missing' },
            [inputEvent('updated', {})],
            3,
          ),
        )
      })

      it('tracks versions per stream independently', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', {})],
          0,
        )
        const other = await storage.appendEvents(
          { stream: 'order', id: 'o2' },
          [inputEvent('created', {})],
          0,
        )

        expect(other[0].streamVersion).toBe(1)
      })

      it('round-trips complex payloads and metadata through superjson types', async () => {
        const storage = await makeStorage()
        const createdAt = new Date('2024-05-04T03:02:01.000Z')
        const payload = {
          when: createdAt,
          tags: new Map([['a', 1]]),
          missing: undefined,
          nested: { deep: [1, 2, 3] },
        }

        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', payload, { metadata: { at: createdAt } })],
          0,
        )

        const [event] = await collect(
          storage.loadEvents({ stream: 'order', id: 'o1' }),
        )
        expect(event.payload).toEqual(payload)
        expect((event.metadata as any).at).toEqual(createdAt)
      })
    })

    describe('loadEvents', () => {
      it('yields all events globally ordered by position', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', { n: 1 })],
          0,
        )
        await storage.appendEvents(
          { stream: 'invoice', id: 'i1' },
          [inputEvent('issued', { n: 2 })],
          0,
        )
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('updated', { n: 3 })],
          1,
        )

        const events = await collect(storage.loadEvents(null))
        expect(events).toHaveLength(3)
        expect(events.map((event) => (event.payload as any).n)).toEqual([
          1, 2, 3,
        ])
        for (let index = 1; index < events.length; index++) {
          expect(events[index].position).toBeGreaterThan(
            events[index - 1].position,
          )
        }
      })

      it('paginates beyond the chunk size without gaps or duplicates', async () => {
        const storage = await makeStorage()
        const total = 120
        for (let index = 0; index < total; index += 20) {
          await storage.appendEvents(
            { stream: 'order', id: 'o1' },
            Array.from({ length: 20 }, (_, offset) =>
              inputEvent('created', { n: index + offset }),
            ),
            index,
          )
        }

        const events = await collect(storage.loadEvents(null))
        expect(events).toHaveLength(total)
        expect(events.map((event) => (event.payload as any).n)).toEqual(
          Array.from({ length: total }, (_, n) => n),
        )
      })

      it('filters by stream type', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', {})],
          0,
        )
        await storage.appendEvents(
          { stream: 'invoice', id: 'i1' },
          [inputEvent('issued', {})],
          0,
        )

        const events = await collect(storage.loadEvents({ stream: 'order' }))
        expect(events).toHaveLength(1)
        expect(events[0].streamType).toBe('order')
      })

      it('filters by stream type and id', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [inputEvent('created', {})],
          0,
        )
        await storage.appendEvents(
          { stream: 'order', id: 'o2' },
          [inputEvent('created', {})],
          0,
        )

        const events = await collect(
          storage.loadEvents({ stream: 'order', id: 'o2' }),
        )
        expect(events).toHaveLength(1)
        expect(events[0].streamId).toBe('o2')
      })

      it('filters by stream events', async () => {
        const storage = await makeStorage()
        await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          [
            inputEvent('created', {}),
            inputEvent('updated', {}),
            inputEvent('shipped', {}),
          ],
          0,
        )

        const events = await collect(
          storage.loadEvents({
            stream: 'order',
            events: ['created', 'shipped'],
          }),
        )
        expect(events.map((event) => event.type)).toEqual([
          'created',
          'shipped',
        ])
      })

      it('honors range.from and range.to inclusively', async () => {
        const storage = await makeStorage()
        const persisted = await storage.appendEvents(
          { stream: 'order', id: 'o1' },
          Array.from({ length: 5 }, (_, n) => inputEvent('created', { n })),
          0,
        )

        const from = persisted[1].position
        const to = persisted[3].position
        const events = await collect(storage.loadEvents(null, { from, to }))

        expect(events.map((event) => event.position)).toEqual([
          persisted[1].position,
          persisted[2].position,
          persisted[3].position,
        ])
      })
    })

    describe('checkpoint', () => {
      it('inserts with a null expected version and reads it back', async () => {
        const storage = await makeStorage()
        const checkpoint = checkpointFixture({ metadata: { cursor: 'abc' } })

        expect(await storage.checkpoint.upsert(checkpoint, null)).toBe(true)

        const stored = await storage.checkpoint.get('projection', 'test')
        expect(stored).toBeTruthy()
        expect(stored!.lastEventPosition).toBe(1)
        expect(stored!.version).toBe(1)
        expect(stored!.metadata).toEqual({ cursor: 'abc' })
      })

      it('rejects an insert-only upsert when the checkpoint already exists', async () => {
        const storage = await makeStorage()
        await storage.checkpoint.upsert(checkpointFixture(), null)

        expect(await storage.checkpoint.upsert(checkpointFixture(), null)).toBe(
          false,
        )
      })

      it('applies the write when the expected version matches', async () => {
        const storage = await makeStorage()
        await storage.checkpoint.upsert(checkpointFixture({ version: 1 }), null)

        expect(
          await storage.checkpoint.upsert(
            checkpointFixture({ lastEventPosition: 9, version: 2 }),
            1,
          ),
        ).toBe(true)

        const stored = await storage.checkpoint.get('projection', 'test')
        expect(stored!.lastEventPosition).toBe(9)
        expect(stored!.version).toBe(2)
      })

      it('rejects the write when the expected version is stale', async () => {
        const storage = await makeStorage()
        await storage.checkpoint.upsert(checkpointFixture({ version: 2 }), null)

        expect(
          await storage.checkpoint.upsert(
            checkpointFixture({ lastEventPosition: 9, version: 3 }),
            1,
          ),
        ).toBe(false)

        const stored = await storage.checkpoint.get('projection', 'test')
        expect(stored!.lastEventPosition).toBe(1)
        expect(stored!.version).toBe(2)
      })

      it('inserts when expecting a version on a missing checkpoint', async () => {
        const storage = await makeStorage()

        expect(
          await storage.checkpoint.upsert(checkpointFixture({ version: 5 }), 4),
        ).toBe(true)

        const stored = await storage.checkpoint.get('projection', 'test')
        expect(stored!.version).toBe(5)
      })

      it('keeps checkpoints of different types and names separate', async () => {
        const storage = await makeStorage()
        await storage.checkpoint.upsert(
          checkpointFixture({ type: 'projection', lastEventPosition: 1 }),
          null,
        )
        await storage.checkpoint.upsert(
          checkpointFixture({ type: 'processManager', lastEventPosition: 2 }),
          null,
        )

        const projection = await storage.checkpoint.get('projection', 'test')
        const processManager = await storage.checkpoint.get(
          'processManager',
          'test',
        )
        expect(projection!.lastEventPosition).toBe(1)
        expect(processManager!.lastEventPosition).toBe(2)
        expect(await storage.checkpoint.get('projection', 'other')).toBeFalsy()
      })

      it('deletes a checkpoint', async () => {
        const storage = await makeStorage()
        await storage.checkpoint.upsert(checkpointFixture(), null)
        await storage.checkpoint.delete('projection', 'test')

        expect(await storage.checkpoint.get('projection', 'test')).toBeFalsy()
      })
    })

    describe('projectionSnapshot', () => {
      const snapshotFixture = (
        overrides?: Partial<ProjectionSnapshot>,
      ): ProjectionSnapshot => ({
        projectionId: 'p1',
        projectionType: 'orders',
        lastEventPosition: 1,
        state: { count: 1 },
        metadata: { origin: 'test' },
        schemaVersion: 1,
        createdAt: new Date(),
        ...overrides,
      })

      it('returns the snapshot with the highest event position', async () => {
        const storage = await makeStorage()
        await storage.projectionSnapshot.put(snapshotFixture())
        await storage.projectionSnapshot.put(
          snapshotFixture({ lastEventPosition: 7, state: { count: 7 } }),
        )

        const snapshot = await storage.projectionSnapshot.get({
          projection: 'orders',
          id: 'p1',
        })
        expect(snapshot!.lastEventPosition).toBe(7)
        expect(snapshot!.state).toEqual({ count: 7 })
        expect(snapshot!.metadata).toEqual({ origin: 'test' })
        expect(snapshot!.createdAt).toBeInstanceOf(Date)
      })

      it('returns nothing for unknown projections', async () => {
        const storage = await makeStorage()

        expect(
          await storage.projectionSnapshot.get({
            projection: 'orders',
            id: 'missing',
          }),
        ).toBeFalsy()
      })

      it('deletes snapshots above a position', async () => {
        const storage = await makeStorage()
        await storage.projectionSnapshot.put(
          snapshotFixture({ lastEventPosition: 2 }),
        )
        await storage.projectionSnapshot.put(
          snapshotFixture({ lastEventPosition: 9 }),
        )

        await storage.projectionSnapshot.delete(
          { projection: 'orders', id: 'p1' },
          5,
        )

        const snapshot = await storage.projectionSnapshot.get({
          projection: 'orders',
          id: 'p1',
        })
        expect(snapshot!.lastEventPosition).toBe(2)
      })

      it('deletes all snapshots of a projection', async () => {
        const storage = await makeStorage()
        await storage.projectionSnapshot.put(snapshotFixture())
        await storage.projectionSnapshot.put(
          snapshotFixture({ projectionId: 'p2' }),
        )

        await storage.projectionSnapshot.delete({ projection: 'orders' })

        expect(
          await storage.projectionSnapshot.get({
            projection: 'orders',
            id: 'p1',
          }),
        ).toBeFalsy()
        expect(
          await storage.projectionSnapshot.get({
            projection: 'orders',
            id: 'p2',
          }),
        ).toBeFalsy()
      })

      it('increments applied counts per projection instance', async () => {
        const storage = await makeStorage()
        const target = { projection: 'orders', id: 'p1' }

        expect(
          await storage.projectionSnapshot.incrementAppliedCount(target),
        ).toBe(1)
        expect(
          await storage.projectionSnapshot.incrementAppliedCount(target),
        ).toBe(2)
        expect(
          await storage.projectionSnapshot.incrementAppliedCount({
            projection: 'orders',
            id: 'p2',
          }),
        ).toBe(1)
      })
    })

    describe('aggregateRootSnapshot', () => {
      const snapshotFixture = (
        overrides?: Partial<AggregateRootSnapshot>,
      ): AggregateRootSnapshot => ({
        streamId: 's1',
        streamType: 'order',
        streamVersion: 1,
        lastEventPosition: 1,
        state: { total: 1 },
        metadata: { origin: 'test' },
        schemaVersion: 1,
        createdAt: new Date(),
        ...overrides,
      })

      it('returns the snapshot with the highest stream version', async () => {
        const storage = await makeStorage()
        await storage.aggregateRootSnapshot.put(snapshotFixture())
        await storage.aggregateRootSnapshot.put(
          snapshotFixture({
            streamVersion: 4,
            lastEventPosition: 9,
            state: { total: 4 },
          }),
        )

        const snapshot = await storage.aggregateRootSnapshot.get({
          stream: 'order',
          id: 's1',
        })
        expect(snapshot!.streamVersion).toBe(4)
        expect(snapshot!.state).toEqual({ total: 4 })
        expect(snapshot!.createdAt).toBeInstanceOf(Date)
      })

      it('returns nothing for unknown streams', async () => {
        const storage = await makeStorage()

        expect(
          await storage.aggregateRootSnapshot.get({
            stream: 'order',
            id: 'missing',
          }),
        ).toBeFalsy()
      })

      it('deletes snapshots above a stream version', async () => {
        const storage = await makeStorage()
        await storage.aggregateRootSnapshot.put(
          snapshotFixture({ streamVersion: 4 }),
        )

        await storage.aggregateRootSnapshot.delete(
          { stream: 'order', id: 's1' },
          2,
        )

        expect(
          await storage.aggregateRootSnapshot.get({
            stream: 'order',
            id: 's1',
          }),
        ).toBeFalsy()
      })

      it('keeps snapshots at or below the deletion version', async () => {
        const storage = await makeStorage()
        await storage.aggregateRootSnapshot.put(
          snapshotFixture({ streamVersion: 2 }),
        )

        await storage.aggregateRootSnapshot.delete(
          { stream: 'order', id: 's1' },
          2,
        )

        const snapshot = await storage.aggregateRootSnapshot.get({
          stream: 'order',
          id: 's1',
        })
        expect(snapshot!.streamVersion).toBe(2)
      })
    })
  })
}
