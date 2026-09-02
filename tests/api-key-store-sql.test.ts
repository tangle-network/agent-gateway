import { DatabaseSync } from 'node:sqlite'
import { describe, expect, it } from 'vitest'

import { SqlApiKeyStore, sqlApiKeyStoreSchemaStatements } from '../src/api-key-store-sql'
import type { SqlAdapter } from '../src/a2a/task-store-sql'

function sqliteAdapter(db: DatabaseSync): SqlAdapter {
  return {
    async exec(sql, params = []) {
      const result = db.prepare(sql).run(...params as never[])
      return { rowsAffected: Number(result.changes) }
    },
    async query<TRow>(sql, params = []) {
      return db.prepare(sql).all(...params as never[]) as TRow[]
    },
  }
}

async function createStore(db: DatabaseSync, table?: string): Promise<SqlApiKeyStore> {
  const store = new SqlApiKeyStore(sqliteAdapter(db), table ? { table } : {})
  await store.migrate()
  return store
}

async function createKey(
  store: SqlApiKeyStore,
  userId = 'user-1',
  keyHash = 'hash-1',
  spendingLimitCents = 500,
  limits: { rateLimit?: number; dailyLimit?: number } = {},
) {
  return store.create(userId, {
    name: 'Production',
    keyHash,
    keyPrefix: 'ak_12345678',
    scopes: ['chat'],
    rateLimit: limits.rateLimit ?? 60,
    dailyLimit: limits.dailyLimit ?? 1_000,
    spendingLimitCents,
    expiresAt: new Date('2030-01-01T00:00:00.999Z'),
  })
}

describe('SqlApiKeyStore', () => {
  it('migrates idempotently and preserves the API key contract', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      await store.migrate()
      const created = await createKey(store)
      const indexes = db.prepare("PRAGMA index_list('agent_api_key')").all() as Array<{
        name: string
        unique: number
      }>

      expect(created.id).toMatch(/^[0-9a-f]{32}$/)
      expect(created.spentCents).toBe(0)
      expect(created.lastUsedAt).toBeNull()
      expect(created.expiresAt?.toISOString()).toBe('2030-01-01T00:00:00.000Z')

      expect(await store.findByHash('hash-1')).toEqual(created)
      expect(await store.list('user-1')).toEqual([
        expect.objectContaining({
          id: created.id,
          scopes: ['chat'],
          spendingLimitCents: 500,
        }),
      ])
      expect(await store.list('user-1')).not.toEqual([
        expect.objectContaining({ keyHash: expect.anything() }),
      ])
      expect(indexes).toContainEqual(expect.objectContaining({
        name: 'idx_agent_api_key_hash_unique',
        unique: 1,
      }))
    } finally {
      db.close()
    }
  })

  it('deletes only a key owned by the requesting user', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(store)

      expect(await store.delete('user-2', key.id)).toBe(false)
      expect(await store.findByHash('hash-1')).not.toBeNull()
      expect(await store.delete('user-1', key.id)).toBe(true)
      expect(await store.findByHash('hash-1')).toBeNull()
    } finally {
      db.close()
    }
  })

  it('records concurrent usage atomically', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      await createKey(store)

      const key = await store.findByHash('hash-1')
      await Promise.all(Array.from(
        { length: 20 },
        (_, index) => store.recordUsage(key!.id, 3, `request-${index}`),
      ))

      const updated = await store.findByHash('hash-1')
      expect(updated?.spentCents).toBe(60)
      expect(updated?.lastUsedAt).toBeInstanceOf(Date)
    } finally {
      db.close()
    }
  })

  it('claims minute and daily request slots before work starts', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(
        store,
        'user-1',
        'limited-hash',
        500,
        { rateLimit: 2, dailyLimit: 3 },
      )
      const minuteOne = new Date('2026-09-02T12:00:10.000Z')
      const minuteTwo = new Date('2026-09-02T12:01:10.000Z')
      const nextDay = new Date('2026-09-03T00:00:10.000Z')

      await expect(store.claimRequest(key.id, 'request-1', minuteOne))
        .resolves.toMatchObject({ allowed: true, minuteRemaining: 1, dailyRemaining: 2 })
      await expect(store.claimRequest(key.id, 'request-2', minuteOne))
        .resolves.toMatchObject({ allowed: true, minuteRemaining: 0, dailyRemaining: 1 })
      await expect(store.claimRequest(key.id, 'request-3', minuteOne))
        .resolves.toMatchObject({ allowed: false, reason: 'minute' })

      await expect(store.claimRequest(key.id, 'request-3', minuteTwo))
        .resolves.toMatchObject({ allowed: true, minuteRemaining: 1, dailyRemaining: 0 })
      await expect(store.claimRequest(key.id, 'request-4', minuteTwo))
        .resolves.toMatchObject({ allowed: false, reason: 'daily' })
      await expect(store.claimRequest(key.id, 'request-4', nextDay))
        .resolves.toMatchObject({ allowed: true, dailyRemaining: 2 })
    } finally {
      db.close()
    }
  })

  it('deduplicates request claims and refuses every slot above the limit', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(
        store,
        'user-1',
        'request-hash',
        500,
        { rateLimit: 5, dailyLimit: 5 },
      )
      const now = new Date('2026-09-02T12:00:10.000Z')
      const claims = await Promise.all(Array.from(
        { length: 20 },
        (_, index) => store.claimRequest(key.id, `request-${index}`, now),
      ))

      expect(claims.filter((claim) => claim.allowed)).toHaveLength(5)
      expect(claims.filter((claim) => !claim.allowed)).toHaveLength(15)
      await expect(store.claimRequest(key.id, 'request-0', now))
        .resolves.toMatchObject({ allowed: true, dailyRemaining: 0 })

      const other = await createKey(store, 'user-2', 'other-hash')
      await expect(store.claimRequest(other.id, 'request-0', now))
        .rejects.toThrow(/another key/)
    } finally {
      db.close()
    }
  })

  it('bounds stored request claims while retaining the current and prior UTC day', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(
        store,
        'user-1',
        'retention-hash',
        500,
        { rateLimit: 300, dailyLimit: 300 },
      )
      await store.claimRequest(key.id, 'old-request', new Date('2026-09-01T12:00:00.000Z'))
      await Promise.all(Array.from(
        { length: 255 },
        (_, index) => store.claimRequest(
          key.id,
          `current-request-${index}`,
          new Date('2026-09-03T12:00:00.000Z'),
        ),
      ))

      const old = db.prepare(
        'SELECT request_id FROM agent_api_key_request WHERE request_id = ?',
      ).all('old-request')
      const current = db.prepare(
        'SELECT COUNT(*) AS count FROM agent_api_key_request WHERE key_id = ?',
      ).get(key.id) as { count: number }

      expect(old).toEqual([])
      expect(current.count).toBe(255)
    } finally {
      db.close()
    }
  })

  it('deduplicates retries by request id and rejects changed retry data', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(store)

      await store.recordUsage(key.id, 7, 'request-1')
      await store.recordUsage(key.id, 7, 'request-1')
      expect((await store.findByHash('hash-1'))?.spentCents).toBe(7)

      await expect(store.recordUsage(key.id, 8, 'request-1'))
        .rejects.toThrow(/reused with different usage/)
    } finally {
      db.close()
    }
  })

  it('atomically refuses sequential and concurrent spending above the key cap', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const sequential = await createStore(db)
      const first = await createKey(sequential, 'user-1', 'sequential-hash', 100)
      await sequential.recordUsage(first.id, 90, 'sequential-1')
      await expect(sequential.recordUsage(first.id, 50, 'sequential-2'))
        .rejects.toMatchObject({ code: 'api_key.spending_limit_exceeded' })
      expect((await sequential.findByHash('sequential-hash'))?.spentCents).toBe(90)

      const concurrent = await createKey(sequential, 'user-1', 'concurrent-hash', 100)
      const results = await Promise.allSettled([
        sequential.recordUsage(concurrent.id, 80, 'concurrent-1'),
        sequential.recordUsage(concurrent.id, 80, 'concurrent-2'),
      ])
      expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
      expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1)
      expect((await sequential.findByHash('concurrent-hash'))?.spentCents).toBe(80)
    } finally {
      db.close()
    }
  })

  it('adds a distinct unique hash index when a fleet table has a legacy non-unique index', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      db.exec(`CREATE TABLE agent_api_key (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        key_hash TEXT NOT NULL,
        key_prefix TEXT NOT NULL,
        scopes TEXT NOT NULL,
        rate_limit INTEGER NOT NULL,
        daily_limit INTEGER NOT NULL,
        spending_limit_cents INTEGER,
        spent_cents INTEGER NOT NULL DEFAULT 0,
        last_used_at INTEGER,
        expires_at INTEGER,
        created_at INTEGER NOT NULL
      )`)
      db.exec('CREATE INDEX idx_agent_api_key_hash ON agent_api_key (key_hash)')
      const store = await createStore(db)
      await createKey(store, 'user-1', 'duplicate-hash')

      await expect(createKey(store, 'user-2', 'duplicate-hash')).rejects.toThrow()
      const indexes = db.prepare("PRAGMA index_list('agent_api_key')").all() as Array<{
        name: string
        unique: number
      }>
      expect(indexes).toContainEqual(expect.objectContaining({
        name: 'idx_agent_api_key_hash_unique',
        unique: 1,
      }))
    } finally {
      db.close()
    }
  })

  it('rejects usage for a key deleted while its request was running', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(store)
      await store.delete('user-1', key.id)

      await expect(store.recordUsage(key.id, 1)).rejects.toThrow(/no longer exists/)
    } finally {
      db.close()
    }
  })

  it('rejects negative or fractional usage', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      const key = await createKey(store)

      await expect(store.recordUsage(key.id, -1)).rejects.toThrow(/non-negative integer/)
      await expect(store.recordUsage(key.id, 0.5)).rejects.toThrow(/non-negative integer/)
    } finally {
      db.close()
    }
  })

  it('isolates custom tables and rejects unsafe identifiers', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const first = await createStore(db)
      const second = await createStore(db, 'tenant_api_keys')
      await createKey(first, 'user-1', 'first-hash')
      await createKey(second, 'user-1', 'second-hash')

      expect(await first.findByHash('second-hash')).toBeNull()
      expect(await second.findByHash('first-hash')).toBeNull()
      expect(() => new SqlApiKeyStore(sqliteAdapter(db), { table: 'keys; DROP TABLE keys' }))
        .toThrow(/table name/)
      expect(() => sqlApiKeyStoreSchemaStatements({ table: 'keys; DROP TABLE keys' }))
        .toThrow(/table name/)
      expect(() => new SqlApiKeyStore(sqliteAdapter(db), {
        table: 'keys',
        usageTable: 'keys',
      })).toThrow(/table names must differ/)
    } finally {
      db.close()
    }
  })
})
