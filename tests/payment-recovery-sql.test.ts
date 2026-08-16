import { describe, expect, it } from 'vitest'

import type { SqlAdapter } from '../src/a2a/task-store-sql'
import {
  PaymentRecoveryFenceError,
  updateOwnedPaymentRecovery,
  type PaymentRecoveryRecord,
} from '../src/payment-recovery'
import { SqlPaymentRecoveryStore } from '../src/payment-recovery-sql'

interface Row {
  id: string
  state: string
  next_attempt_at: number
  revision: number
  payload: string
  updated_at: number
}

function adapter(): SqlAdapter & { rows: Map<string, Row> } {
  const rows = new Map<string, Row>()
  return {
    rows,
    async exec(sql, params = []) {
      const statement = sql.trim()
      if (statement.startsWith('CREATE TABLE') || statement.startsWith('CREATE INDEX')) {
        return { rowsAffected: 0 }
      }
      if (statement.startsWith('INSERT INTO')) {
        const [id, state, nextAttemptAt, revision, payload, updatedAt] = params as [
          string,
          string,
          number,
          number,
          string,
          number,
        ]
        if (rows.has(id)) throw new Error('duplicate primary key')
        rows.set(id, {
          id,
          state,
          next_attempt_at: nextAttemptAt,
          revision,
          payload,
          updated_at: updatedAt,
        })
        return { rowsAffected: 1 }
      }
      if (statement.startsWith('UPDATE')) {
        const [state, nextAttemptAt, revision, payload, updatedAt, id, expectedRevision] = params as [
          string,
          number,
          number,
          string,
          number,
          string,
          number,
        ]
        const row = rows.get(id)
        if (!row || row.revision !== expectedRevision) return { rowsAffected: 0 }
        rows.set(id, {
          id,
          state,
          next_attempt_at: nextAttemptAt,
          revision,
          payload,
          updated_at: updatedAt,
        })
        return { rowsAffected: 1 }
      }
      throw new Error(`unrecognized SQL: ${statement}`)
    },
    async query<TRow>(sql: string, params: readonly unknown[] = []): Promise<TRow[]> {
      const statement = sql.trim()
      if (statement.includes('WHERE id =')) {
        const row = rows.get(params[0] as string)
        return (row ? [{ payload: row.payload }] : []) as TRow[]
      }
      if (statement.includes('next_attempt_at <=')) {
        const [, now, limit] = params as [string, number, number]
        return [...rows.values()]
          .filter((row) => row.state !== 'reconciled' && row.next_attempt_at <= now)
          .sort((left, right) => left.next_attempt_at - right.next_attempt_at)
          .slice(0, limit)
          .map((row) => ({ payload: row.payload })) as TRow[]
      }
      throw new Error(`unrecognized SQL: ${statement}`)
    },
  }
}

function record(id = 'payment-1'): PaymentRecoveryRecord {
  return {
    version: 1,
    id,
    revision: 0,
    state: 'claimed',
    payment: { kind: 'x402', operationId: id },
    attribution: {
      requestId: 'request-1',
      agentId: 'agent-1',
      agentSlug: 'agent',
      consumerId: 'consumer-1',
      paymentMethod: 'x402',
      startMs: 1,
      pricePerTokenUsd: 0.000001,
      platformFeePercent: 0.2,
      requiredAmount: '100',
      currencyDecimals: 6,
      maxOutputTokens: 1,
      executionBudget: {
        maxInputTokens: 1,
        maxOutputTokens: 1,
        maxReasoningTokens: 0,
        maxToolTokens: 0,
        maxToolCalls: 0,
        maxProviderCostUsd: 0.0001,
      },
    },
    workStarted: false,
    usageRecorded: false,
    attempts: 0,
    nextAttemptAt: 10,
    createdAt: 1,
    updatedAt: 1,
  }
}

describe('SqlPaymentRecoveryStore', () => {
  it('persists rows, rejects duplicate payment identities, and uses revision CAS', async () => {
    const db = adapter()
    const store = new SqlPaymentRecoveryStore(db)
    await store.migrate()
    const initial = record()
    expect(await store.createIfAbsent(initial)).toBe(true)
    expect(await store.createIfAbsent(initial)).toBe(false)
    expect(await store.get(initial.id)).toEqual(initial)

    const next = { ...initial, revision: 1, state: 'executing' as const, updatedAt: 2 }
    expect(await store.compareAndSet(initial, next)).toBe(true)
    expect(await store.compareAndSet(initial, { ...next, revision: 2 })).toBe(false)
    expect((await store.get(initial.id))?.state).toBe('executing')
  })

  it('scans only due unresolved rows and keeps reconciled tombstones', async () => {
    const store = new SqlPaymentRecoveryStore(adapter())
    const due = record('due')
    const later = { ...record('later'), nextAttemptAt: 100 }
    const reconciled = {
      ...record('done'),
      state: 'reconciled' as const,
      reconciledAt: 5,
      nextAttemptAt: Number.MAX_SAFE_INTEGER,
    }
    await store.createIfAbsent(due)
    await store.createIfAbsent(later)
    await store.createIfAbsent(reconciled)

    expect((await store.listDue(10, 10)).map((value) => value.id)).toEqual(['due'])
    expect(await store.get('done')).toEqual(reconciled)
  })

  it('rejects an unsafe interpolated table name', () => {
    expect(() => new SqlPaymentRecoveryStore(adapter(), { table: 'payments; DROP TABLE users' }))
      .toThrow('table name is invalid')
  })

  it('rejects a stale owner after another lease takes the durable row', async () => {
    const store = new SqlPaymentRecoveryStore(adapter())
    const first = {
      ...record('fenced'),
      lease: { id: 'request-fence', expiresAt: 10 },
    }
    await store.createIfAbsent(first)
    const replacement: PaymentRecoveryRecord = {
      ...first,
      revision: 1,
      lease: { id: 'worker-fence', expiresAt: 20 },
      updatedAt: 11,
    }
    expect(await store.compareAndSet(first, replacement)).toBe(true)

    await expect(updateOwnedPaymentRecovery(
      store,
      first.id,
      'request-fence',
      (current) => ({ ...current, state: 'executing' }),
    )).rejects.toBeInstanceOf(PaymentRecoveryFenceError)
    expect((await updateOwnedPaymentRecovery(
      store,
      first.id,
      'worker-fence',
      (current) => ({ ...current, state: 'executing' }),
    )).state).toBe('executing')
  })
})
