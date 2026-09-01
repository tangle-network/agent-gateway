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
) {
  return store.create(userId, {
    name: 'Production',
    keyHash,
    keyPrefix: 'ak_12345678',
    scopes: ['chat'],
    rateLimit: 60,
    dailyLimit: 1_000,
    spendingLimitCents: 500,
    expiresAt: new Date('2030-01-01T00:00:00.000Z'),
  })
}

describe('SqlApiKeyStore', () => {
  it('migrates idempotently and preserves the API key contract', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = await createStore(db)
      await store.migrate()
      const created = await createKey(store)

      expect(created.id).toMatch(/^[0-9a-f]{32}$/)
      expect(created.spentCents).toBe(0)
      expect(created.lastUsedAt).toBeNull()

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

      await Promise.all(Array.from({ length: 20 }, () => store.recordUsage('missing', 1)))
      const key = await store.findByHash('hash-1')
      await Promise.all(Array.from({ length: 20 }, () => store.recordUsage(key!.id, 3)))

      const updated = await store.findByHash('hash-1')
      expect(updated?.spentCents).toBe(60)
      expect(updated?.lastUsedAt).toBeInstanceOf(Date)
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
    } finally {
      db.close()
    }
  })
})
