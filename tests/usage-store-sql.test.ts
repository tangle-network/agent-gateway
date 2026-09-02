import { DatabaseSync } from 'node:sqlite'
import { describe, expect, it } from 'vitest'

import type { GatewayUsageEvent } from '../src/payment-types'
import type { SqlAdapter } from '../src/a2a/task-store-sql'
import {
  SqlGatewayUsageStore,
  sqlGatewayUsageStoreSchemaStatements,
} from '../src/usage-store-sql'

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

function usage(overrides: Partial<GatewayUsageEvent> = {}): GatewayUsageEvent {
  return {
    requestId: 'request-1',
    agentId: 'agent-1',
    agentSlug: 'workspace',
    consumerId: 'apikey:key-1',
    paymentMethod: 'apikey',
    inputTokens: 10,
    outputTokens: 20,
    reasoningTokens: 3,
    toolTokens: 4,
    toolCallCount: 2,
    providerCostUsd: 0.01,
    totalCostUsd: 0.01,
    ownerEarnedUsd: 0.01,
    platformFeeUsd: 0,
    durationMs: 250,
    settlementBasis: 'usage-receipt',
    ...overrides,
  }
}

describe('SqlGatewayUsageStore', () => {
  it('records once by request id and updates a retry atomically', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = new SqlGatewayUsageStore(sqliteAdapter(db))
      await store.migrate()
      await store.migrate()

      const recordUsage = store.recordUsage
      await recordUsage(usage())
      const createdAt = db.prepare(
        'SELECT created_at FROM agent_gateway_usage WHERE request_id = ?',
      ).get('request-1') as { created_at: number }

      await recordUsage(usage({ outputTokens: 30, durationMs: 500 }))
      const rows = db.prepare(
        'SELECT request_id, output_tokens, duration_ms, created_at FROM agent_gateway_usage',
      ).all() as Array<{
        request_id: string
        output_tokens: number
        duration_ms: number
        created_at: number
      }>

      expect(rows).toEqual([{
        request_id: 'request-1',
        output_tokens: 30,
        duration_ms: 500,
        created_at: createdAt.created_at,
      }])
    } finally {
      db.close()
    }
  })

  it('stores money as exact integer nanodollars instead of lossy SQL REAL values', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = new SqlGatewayUsageStore(sqliteAdapter(db))
      await store.migrate()
      await store.recordUsage(usage({
        providerCostUsd: 123_456.789_123,
        totalCostUsd: 123_456.789_123,
        ownerEarnedUsd: 123_456.789_123,
        platformFeeUsd: 0,
      }))

      const row = db.prepare(`SELECT
        provider_cost_nanodollars,
        total_cost_nanodollars,
        owner_earned_nanodollars,
        platform_fee_nanodollars
        FROM agent_gateway_usage WHERE request_id = ?`).get('request-1') as Record<string, number>
      expect(row).toEqual({
        provider_cost_nanodollars: 123_456_789_123_000,
        total_cost_nanodollars: 123_456_789_123_000,
        owner_earned_nanodollars: 123_456_789_123_000,
        platform_fee_nanodollars: 0,
      })
    } finally {
      db.close()
    }
  })

  it('isolates custom tables and rejects unsafe identifiers', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const store = new SqlGatewayUsageStore(sqliteAdapter(db), { table: 'private_usage' })
      await store.migrate()
      await store.recordUsage(usage())

      const count = db.prepare('SELECT COUNT(*) AS count FROM private_usage').get() as { count: number }
      expect(count.count).toBe(1)
      expect(() => sqlGatewayUsageStoreSchemaStatements({ table: 'usage; DROP TABLE usage' }))
        .toThrow(/table name/)
      expect(() => new SqlGatewayUsageStore(sqliteAdapter(db), {
        table: 'usage; DROP TABLE usage',
      })).toThrow(/table name/)
    } finally {
      db.close()
    }
  })
})
