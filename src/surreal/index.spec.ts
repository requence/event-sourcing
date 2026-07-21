import { afterAll, describe, expect, it } from 'bun:test'
import { createNodeEngines } from '@surrealdb/node'
import { Surreal } from 'surrealdb'

import { ConcurrencyError } from '../errors.ts'
import { runStorageDriverContract } from '../tests/storageDriverContract.ts'
import { createSurrealStorage, initSchema } from './index.ts'

// One shared embedded engine for the whole file; isolation comes from a
// fresh logical database per test (tests run sequentially).
let connection: Surreal | undefined

async function makeDatabase() {
  if (!connection) {
    connection = new Surreal({ engines: { ...createNodeEngines() } })
    await connection.connect('mem://')
  }
  await connection.use({ namespace: 'test', database: crypto.randomUUID() })
  return connection
}

afterAll(async () => {
  await connection?.close()
})

runStorageDriverContract('surreal', async () => {
  const database = await makeDatabase()
  return createSurrealStorage({ database })
})

describe('surreal driver specifics', () => {
  it('initializes the schema idempotently', async () => {
    const database = await makeDatabase()
    await initSchema(database)
    await initSchema(database)

    const storage = createSurrealStorage({ database })
    const persisted = await storage.appendEvents(
      { stream: 'order', id: 'o1' },
      [{ type: 'created', payload: {}, metadata: {}, schemaVersion: 1 }],
      0,
    )
    expect(persisted[0].position).toBe(1)
  })

  it('skips schema creation when initSchema is disabled', async () => {
    const database = await makeDatabase()
    const storage = createSurrealStorage({ database, initSchema: false })

    let error: unknown
    try {
      await storage.appendEvents(
        { stream: 'order', id: 'o1' },
        [{ type: 'created', payload: {}, metadata: {}, schemaVersion: 1 }],
        0,
      )
    } catch (err) {
      error = err
    }
    // Without the SCHEMAFULL tables the counter arithmetic cannot resolve.
    expect(error).toBeTruthy()
  })

  it('assigns gapless, monotonically increasing global positions', async () => {
    const database = await makeDatabase()
    const storage = createSurrealStorage({ database })

    await storage.appendEvents(
      { stream: 'order', id: 'o1' },
      [
        { type: 'created', payload: {}, metadata: {}, schemaVersion: 1 },
        { type: 'updated', payload: {}, metadata: {}, schemaVersion: 1 },
      ],
      0,
    )
    const second = await storage.appendEvents(
      { stream: 'order', id: 'o2' },
      [{ type: 'created', payload: {}, metadata: {}, schemaVersion: 1 }],
      0,
    )

    expect(second[0].position).toBe(3)
  })

  it('serializes concurrent appends to different streams without losing positions', async () => {
    const database = await makeDatabase()
    const storage = createSurrealStorage({ database })

    const results = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        storage.appendEvents(
          { stream: 'order', id: `o${index}` },
          [{ type: 'created', payload: {}, metadata: {}, schemaVersion: 1 }],
          0,
        ),
      ),
    )

    const positions = results
      .map(([event]) => event.position)
      .toSorted((a, b) => a - b)
    expect(positions).toEqual([1, 2, 3, 4, 5, 6, 7, 8])
  })

  it('reports the current version in ConcurrencyError', async () => {
    const database = await makeDatabase()
    const storage = createSurrealStorage({ database })

    await storage.appendEvents(
      { stream: 'order', id: 'o1' },
      [
        { type: 'created', payload: {}, metadata: {}, schemaVersion: 1 },
        { type: 'updated', payload: {}, metadata: {}, schemaVersion: 1 },
      ],
      0,
    )

    let error: unknown
    try {
      await storage.appendEvents(
        { stream: 'order', id: 'o1' },
        [{ type: 'updated', payload: {}, metadata: {}, schemaVersion: 1 }],
        1,
      )
    } catch (err) {
      error = err
    }

    expect(error).toBeInstanceOf(ConcurrencyError)
    expect((error as Error).message).toContain('expected v1, actual v2')
  })

  it('notifies onEventsAppended with the persisted events', async () => {
    const database = await makeDatabase()
    let appended: unknown[] = []
    const storage = createSurrealStorage({
      database,
      onEventsAppended: (events) => {
        appended = events
      },
    })

    await storage.appendEvents(
      { stream: 'order', id: 'o1' },
      [{ type: 'created', payload: { n: 1 }, metadata: {}, schemaVersion: 1 }],
      0,
    )

    expect(appended).toHaveLength(1)
    expect((appended[0] as any).position).toBe(1)
  })
})
