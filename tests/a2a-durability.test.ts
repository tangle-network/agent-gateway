/**
 * SqlTaskStore + SqlPushNotificationStore — exercises both stores
 * end-to-end against a fake-but-honest SqlAdapter that interprets the exact
 * statements the stores issue, plus real SQLite migration and lease-race tests.
 *
 * The fake adapter is intentionally minimal — if a store's SQL drifts, these
 * tests break loudly, which is the point.
 */
import { DatabaseSync } from 'node:sqlite'
import { describe, expect, it } from 'vitest'

import {
  InMemoryPushNotificationStore,
  type PushNotificationConfig,
  SqlPushNotificationStore,
} from '../src/a2a/push-notifications'
import { claimTaskExecution, renewTaskExecution } from '../src/a2a/execution-fence'
import { type SqlAdapter, SqlTaskStore } from '../src/a2a/task-store-sql'
import type { Task } from '../src/a2a/types'

interface FakeTaskRow {
  id: string
  context_id: string
  state: string
  payload: string
  updated_at: number
  execution_request_id: string | null
  execution_lease_expires_at: number | null
}
interface FakePushRow {
  task_id: string
  config_id: string
  url: string
  token: string | null
  authentication: string | null
}

function makeTaskAdapter(
  beforeDelete?: () => Promise<void>,
): SqlAdapter & { rows: Map<string, FakeTaskRow> } {
  const rows = new Map<string, FakeTaskRow>()
  return {
    rows,
    async exec(sql, params = []) {
      const s = sql.trim()
      if (
        s.startsWith('CREATE TABLE') ||
        s.startsWith('CREATE INDEX') ||
        s.startsWith('ALTER TABLE')
      ) {
        return { rowsAffected: 0 }
      }
      if (s.startsWith('UPDATE')) {
        const [contextId, state, payload, updatedAt, executionRequestId, executionLeaseExpiresAt, id] = params as [
          string,
          string,
          string,
          number,
          string | null,
          number | null,
          string,
        ]
        const row = rows.get(id)
        if (!row) return { rowsAffected: 0 }
        if (s.includes('AND payload = ?') && row.payload !== params[7]) {
          return { rowsAffected: 0 }
        }
        if (
          s.includes('AND execution_request_id = ?') &&
          (row.state !== 'working' ||
            row.execution_request_id !== params[8] ||
            row.execution_lease_expires_at === null ||
            row.execution_lease_expires_at <= Number(params[9]))
        ) {
          return { rowsAffected: 0 }
        }
        row.context_id = contextId
        row.state = state
        row.payload = payload
        row.updated_at = updatedAt
        row.execution_request_id = executionRequestId
        row.execution_lease_expires_at = executionLeaseExpiresAt
        return { rowsAffected: 1 }
      }
      if (s.startsWith('INSERT INTO')) {
        const [id, contextId, state, payload, updatedAt, executionRequestId, executionLeaseExpiresAt] = params as [
          string,
          string,
          string,
          string,
          number,
          string | null,
          number | null,
        ]
        rows.set(id, {
          id,
          context_id: contextId,
          state,
          payload,
          updated_at: updatedAt,
          execution_request_id: executionRequestId,
          execution_lease_expires_at: executionLeaseExpiresAt,
        })
        return { rowsAffected: 1 }
      }
      if (s.startsWith('DELETE')) {
        await beforeDelete?.()
        const [id, expectedPayload, expectedUpdatedAt] = params as [string, string | undefined, number | undefined]
        if (expectedPayload !== undefined && rows.get(id)?.payload !== expectedPayload) {
          return { rowsAffected: 0 }
        }
        if (expectedUpdatedAt !== undefined && rows.get(id)?.updated_at !== expectedUpdatedAt) {
          return { rowsAffected: 0 }
        }
        const had = rows.delete(id)
        return { rowsAffected: had ? 1 : 0 }
      }
      throw new Error(`unrecognised exec SQL: ${s}`)
    },
    async query<TRow>(sql: string, params: readonly unknown[] = []): Promise<TRow[]> {
      const s = sql.trim()
      if (s.includes('WHERE id =')) {
        const [id] = params as [string]
        const row = rows.get(id)
        return (row
          ? [{
              payload: row.payload,
              updated_at: row.updated_at,
              execution_request_id: row.execution_request_id,
              execution_lease_expires_at: row.execution_lease_expires_at,
            }]
          : []) as TRow[]
      }
      if (s.includes('WHERE context_id =')) {
        const [ctx] = params as [string]
        return [...rows.values()]
          .filter((r) => r.context_id === ctx)
          .sort((a, b) => b.updated_at - a.updated_at)
          .map((r) => ({
            payload: r.payload,
            updated_at: r.updated_at,
            execution_request_id: r.execution_request_id,
            execution_lease_expires_at: r.execution_lease_expires_at,
          })) as TRow[]
      }
      throw new Error(`unrecognised query SQL: ${s}`)
    },
  }
}

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

function makeTask(id: string, state: Task['status']['state'] = 'submitted'): Task {
  return {
    kind: 'task',
    id,
    contextId: `ctx-${id}`,
    status: { state, timestamp: new Date().toISOString() },
    history: [
      {
        kind: 'message',
        role: 'user',
        parts: [{ kind: 'text', text: 'hello' }],
        messageId: `msg-${id}`,
      },
    ],
  }
}

describe('SqlTaskStore', () => {
  it('rejects a stale SQL renewal while expiry recovery claims the same row', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      const adapter = sqliteAdapter(db)
      const staleWorker = new SqlTaskStore(adapter)
      const recoveryWorker = new SqlTaskStore(adapter)
      await staleWorker.migrate()

      const task: Task = {
        ...makeTask('sql-execution-race', 'working'),
        metadata: {
          gatewayExecution: {
            version: 1,
            requestId: 'worker-a',
            lease: { id: 'worker-a', expiresAt: 1_000 },
          },
        },
      }
      await staleWorker.put(task)
      const observed = await staleWorker.get(task.id)
      expect(observed).toEqual(task)
      await expect(renewTaskExecution(staleWorker, task.id, 'worker-a', 1_001))
        .rejects.toThrow("was canceled before sandbox execution")

      const staleRenewal = {
        ...task,
        metadata: {
          ...task.metadata,
          gatewayExecution: {
            ...task.metadata!.gatewayExecution as Record<string, unknown>,
            lease: { id: 'worker-a', expiresAt: 5_000 },
          },
        },
      }
      const [staleWon, recovered] = await Promise.all([
        staleWorker.compareAndSetExecution(
          observed!,
          staleRenewal,
          'worker-a',
          1_001,
        ),
        claimTaskExecution(
          recoveryWorker,
          observed!,
          'worker-b',
          1_000,
        ),
      ])
      expect(staleWon).toBe(false)
      expect(recovered.metadata?.gatewayExecution).toMatchObject({ requestId: 'worker-b' })

      expect(await staleWorker.compareAndSetExecution(
        observed!,
        staleRenewal,
        'worker-a',
        1_001,
      )).toBe(false)
      expect((await staleWorker.get(task.id))?.metadata?.gatewayExecution)
        .toMatchObject({ requestId: 'worker-b' })
    } finally {
      db.close()
    }
  })

  it('seeds NULL execution columns on the first renewal after an old-schema migration', async () => {
    const db = new DatabaseSync(':memory:')
    try {
      db.exec(`
        CREATE TABLE a2a_tasks (
          id TEXT PRIMARY KEY,
          context_id TEXT NOT NULL,
          state TEXT NOT NULL,
          payload TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `)
      const now = Date.now()
      const task: Task = {
        ...makeTask('legacy-execution-renewal', 'working'),
        metadata: {
          gatewayExecution: {
            version: 1,
            requestId: 'legacy-worker',
            lease: { id: 'legacy-worker', expiresAt: now + 60_000 },
          },
        },
      }
      db.prepare(
        `INSERT INTO a2a_tasks (id, context_id, state, payload, updated_at)
         VALUES (?, ?, ?, ?, ?)`,
      ).run(
        task.id,
        task.contextId,
        task.status.state,
        JSON.stringify(task),
        now,
      )

      const store = new SqlTaskStore(sqliteAdapter(db))
      await store.migrate()
      expect(db.prepare(
        `SELECT execution_request_id, execution_lease_expires_at
         FROM a2a_tasks WHERE id = ?`,
      ).get(task.id)).toEqual({
        execution_request_id: null,
        execution_lease_expires_at: null,
      })

      const renewed = await renewTaskExecution(store, task.id, 'legacy-worker', now)

      expect(renewed.metadata?.gatewayExecution).toMatchObject({
        requestId: 'legacy-worker',
        lease: { expiresAt: now + 5 * 60 * 1000 },
      })
      expect(db.prepare(
        `SELECT execution_request_id, execution_lease_expires_at
         FROM a2a_tasks WHERE id = ?`,
      ).get(task.id)).toEqual({
        execution_request_id: 'legacy-worker',
        execution_lease_expires_at: now + 5 * 60 * 1000,
      })
    } finally {
      db.close()
    }
  })

  it('round-trips a task through put → get with state + history preserved', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db)
    await store.migrate()
    const task = makeTask('t1', 'completed')
    await store.put(task)
    const fetched = await store.get('t1')
    expect(fetched?.id).toBe('t1')
    expect(fetched?.status.state).toBe('completed')
    expect(fetched?.history?.[0]?.parts[0]).toEqual({ kind: 'text', text: 'hello' })
  })

  it('upserts on repeated put (UPDATE-then-INSERT-on-miss pattern is portable)', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db)
    await store.put(makeTask('t1', 'submitted'))
    await store.put(makeTask('t1', 'working'))
    await store.put(makeTask('t1', 'completed'))
    expect(db.rows.size).toBe(1)
    const fetched = await store.get('t1')
    expect(fetched?.status.state).toBe('completed')
  })

  it('deep-clones on get so callers cannot mutate stored state', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db)
    await store.put(makeTask('t1'))
    const a = await store.get('t1')
    expect(a).toBeDefined()
    if (!a) return
    a.history = []
    const b = await store.get('t1')
    expect(b?.history?.length).toBe(1)
  })

  it('returns undefined and lazily deletes for tasks past TTL', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db, { ttlMs: 1 })
    await store.put(makeTask('t1'))
    await new Promise((r) => setTimeout(r, 5))
    expect(await store.get('t1')).toBeUndefined()
  })

  it('does not let lazy GC delete a row refreshed with the same payload', async () => {
    let deleteStarted!: () => void
    const deleteStartedPromise = new Promise<void>((resolve) => { deleteStarted = resolve })
    let releaseDelete!: () => void
    const deleteGate = new Promise<void>((resolve) => { releaseDelete = resolve })
    const db = makeTaskAdapter(async () => {
      deleteStarted()
      await deleteGate
    })
    const store = new SqlTaskStore(db, { ttlMs: 1 })
    const task = makeTask('t1')
    await store.put(task)
    db.rows.get('t1')!.updated_at = Date.now() - 100

    const staleRead = store.get('t1')
    await deleteStartedPromise
    await store.put(task)
    releaseDelete()
    expect(await staleRead).toBeUndefined()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(db.rows.has('t1')).toBe(true)
  })

  it.each([
    'gatewayFinalizing',
    'gatewayPaymentRelease',
    'gatewayPaymentRecovery',
  ])('does not expire a task while %s reconciliation is pending', async (key) => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db, { ttlMs: 1 })
    const task = { ...makeTask('t1'), metadata: { [key]: { version: 1 } } }
    await store.put(task)
    db.rows.get('t1')!.updated_at = Date.now() - 100

    expect(await store.get('t1')).toEqual(task)
    expect(db.rows.has('t1')).toBe(true)

    await store.put({ ...task, metadata: undefined })
    db.rows.get('t1')!.updated_at = Date.now() - 100
    expect(await store.get('t1')).toBeUndefined()
  })

  it('delete removes the row', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db)
    await store.put(makeTask('t1'))
    await store.delete('t1')
    expect(await store.get('t1')).toBeUndefined()
  })

  it('does not delete a task while payment reconciliation is pending', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db)
    const task = {
      ...makeTask('t1'),
      metadata: { gatewayPaymentRecovery: { version: 1, id: 'payment-1' } },
    }
    await store.put(task)
    await store.delete(task.id)
    expect(await store.get(task.id)).toEqual(task)
  })

  it('listByContext returns most-recent-first for tasks sharing a context', async () => {
    const db = makeTaskAdapter()
    const store = new SqlTaskStore(db)
    const t1 = { ...makeTask('t1'), contextId: 'shared' }
    const t2 = { ...makeTask('t2'), contextId: 'shared' }
    const t3 = { ...makeTask('t3'), contextId: 'other' }
    await store.put(t1)
    await new Promise((r) => setTimeout(r, 2))
    await store.put(t2)
    await store.put(t3)
    const ctxTasks = await store.listByContext('shared')
    expect(ctxTasks.map((t) => t.id)).toEqual(['t2', 't1'])
  })

  it('honors custom table prefix so multiple stores can share a database', async () => {
    const db = makeTaskAdapter()
    const a = new SqlTaskStore(db, { table: 'agent_a_tasks' })
    const b = new SqlTaskStore(db, { table: 'agent_b_tasks' })
    await a.migrate()
    await b.migrate()
    // The fake adapter just accepts both DDLs; the assertion is that no error
    // was thrown and both stores can put/get without colliding when keyed by
    // the same id. (Real DBs use distinct tables; the fake adapter shares a
    // map but task ids in this test are deliberately distinct.)
    await a.put(makeTask('aa'))
    await b.put(makeTask('bb'))
    expect((await a.get('aa'))?.id).toBe('aa')
    expect((await b.get('bb'))?.id).toBe('bb')
  })
})

// ── Push store ──────────────────────────────────────────────────────────────

function makePushAdapter(
  beforeUpsert?: () => Promise<void>,
): SqlAdapter & { rows: Map<string, FakePushRow>; readonly upsertCalls: number } {
  const rows = new Map<string, FakePushRow>()
  const key = (taskId: string, configId: string) => `${taskId}::${configId}`
  let upsertCalls = 0
  return {
    rows,
    get upsertCalls() {
      return upsertCalls
    },
    async exec(sql, params = []) {
      const s = sql.trim()
      if (s.startsWith('CREATE TABLE')) return { rowsAffected: 0 }
      if (s.startsWith('UPDATE')) {
        throw new Error('push config writes must use an atomic upsert')
      }
      if (s.startsWith('INSERT INTO') && s.includes('ON CONFLICT')) {
        upsertCalls += 1
        const [taskId, configId, url, token, auth] = params as [
          string,
          string,
          string,
          string | null,
          string | null,
        ]
        await beforeUpsert?.()
        rows.set(key(taskId, configId), {
          task_id: taskId,
          config_id: configId,
          url,
          token,
          authentication: auth,
        })
        return { rowsAffected: 1 }
      }
      if (s.startsWith('DELETE')) {
        const [taskId, configId] = params as [string, string]
        const had = rows.delete(key(taskId, configId))
        return { rowsAffected: had ? 1 : 0 }
      }
      throw new Error(`unrecognised exec SQL: ${s}`)
    },
    async query<TRow>(sql: string, params: readonly unknown[] = []): Promise<TRow[]> {
      const s = sql.trim()
      if (s.includes('task_id = ? AND config_id =')) {
        const [taskId, configId] = params as [string, string]
        const row = rows.get(key(taskId, configId))
        return (row
          ? [
              {
                config_id: row.config_id,
                url: row.url,
                token: row.token,
                authentication: row.authentication,
              },
            ]
          : []) as TRow[]
      }
      if (s.includes('WHERE task_id =')) {
        const [taskId] = params as [string]
        return [...rows.values()]
          .filter((r) => r.task_id === taskId)
          .map((r) => ({
            config_id: r.config_id,
            url: r.url,
            token: r.token,
            authentication: r.authentication,
          })) as TRow[]
      }
      throw new Error(`unrecognised query SQL: ${s}`)
    },
  }
}

const cfg = (overrides: Partial<PushNotificationConfig> = {}): PushNotificationConfig => ({
  id: 'cfg1',
  url: 'https://example.com/hook',
  token: 'tkn',
  ...overrides,
})

describe('SqlPushNotificationStore', () => {
  it('round-trips a config and preserves token + authentication', async () => {
    const db = makePushAdapter()
    const store = new SqlPushNotificationStore(db)
    await store.migrate()
    await store.set('t1', cfg({ authentication: { schemes: ['Bearer'], credentials: 'abc' } }))
    const fetched = await store.get('t1', 'cfg1')
    expect(fetched?.url).toBe('https://example.com/hook')
    expect(fetched?.token).toBe('tkn')
    expect(fetched?.authentication?.credentials).toBe('abc')
  })

  it('upserts on repeated set (URL change replaces, does not duplicate)', async () => {
    const db = makePushAdapter()
    const store = new SqlPushNotificationStore(db)
    await store.set('t1', cfg({ url: 'https://a.example/h' }))
    await store.set('t1', cfg({ url: 'https://b.example/h' }))
    expect(db.rows.size).toBe(1)
    expect((await store.get('t1', 'cfg1'))?.url).toBe('https://b.example/h')
  })

  it('uses one atomic upsert when two workers register the same config concurrently', async () => {
    let entered = 0
    let release!: () => void
    const bothEntered = new Promise<void>((resolve) => { release = resolve })
    const db = makePushAdapter(async () => {
      entered += 1
      if (entered === 2) release()
      await bothEntered
    })
    const first = new SqlPushNotificationStore(db)
    const second = new SqlPushNotificationStore(db)

    await Promise.all([
      first.set('t1', cfg({ url: 'https://a.example/h' })),
      second.set('t1', cfg({ url: 'https://b.example/h' })),
    ])

    expect(db.upsertCalls).toBe(2)
    expect(db.rows.size).toBe(1)
    expect(['https://a.example/h', 'https://b.example/h'])
      .toContain((await first.get('t1', 'cfg1'))?.url)
  })

  it('list returns all configs for one task; other tasks isolated', async () => {
    const db = makePushAdapter()
    const store = new SqlPushNotificationStore(db)
    await store.set('t1', cfg({ id: 'cfg1' }))
    await store.set('t1', cfg({ id: 'cfg2', url: 'https://x.example/h' }))
    await store.set('t2', cfg({ id: 'cfg3' }))
    expect((await store.list('t1')).map((c) => c.id).sort()).toEqual(['cfg1', 'cfg2'])
    expect((await store.list('t2')).map((c) => c.id)).toEqual(['cfg3'])
  })

  it('delete removes only the targeted config', async () => {
    const db = makePushAdapter()
    const store = new SqlPushNotificationStore(db)
    await store.set('t1', cfg({ id: 'cfg1' }))
    await store.set('t1', cfg({ id: 'cfg2' }))
    await store.delete('t1', 'cfg1')
    expect((await store.list('t1')).map((c) => c.id)).toEqual(['cfg2'])
  })

  it('matches the InMemoryPushNotificationStore contract for the same operations', async () => {
    // Behavioral parity test — both stores should respond identically to the
    // same call sequence. Catches drift if either implementation diverges.
    const sequences: Array<(s: import('../src/a2a/push-notifications').PushNotificationStore) => Promise<unknown>> = [
      (s) => s.set('t1', cfg({ id: 'a' })),
      (s) => s.set('t1', cfg({ id: 'b', url: 'https://b.example/h' })),
      (s) => s.get('t1', 'b'),
      (s) => s.list('t1'),
      (s) => s.delete('t1', 'a'),
      (s) => s.list('t1'),
    ]
    const mem = new InMemoryPushNotificationStore()
    const sql = new SqlPushNotificationStore(makePushAdapter())
    const memResults: unknown[] = []
    const sqlResults: unknown[] = []
    for (const seq of sequences) {
      memResults.push(JSON.parse(JSON.stringify((await seq(mem)) ?? null)))
      sqlResults.push(JSON.parse(JSON.stringify((await seq(sql)) ?? null)))
    }
    expect(memResults).toEqual(sqlResults)
  })
})
