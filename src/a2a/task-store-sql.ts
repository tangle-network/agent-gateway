/**
 * @stable
 *
 * Durable `TaskStore` against any SQL store. Adapter-agnostic: callers wire a
 * `SqlAdapter` against their driver (D1, postgres, sqlite, libSQL, Turso) and
 * the same store survives gateway restarts so an in-flight task (and its
 * artifacts) is recoverable after a Worker recycle.
 *
 * Schema is one table: tasks keyed by id with the full JSON payload, plus a
 * secondary index on `context_id` so `tasks/resubscribe` and conversational
 * lookups by context are O(log n). TTL is enforced at read time the same way
 * `InMemoryTaskStore` does — `createIfAbsent` and `compareAndSet` make task
 * ownership safe when multiple gateway workers share the database.
 *
 * Why not bake in a specific driver? Hono workers run on Cloudflare (D1),
 * Node (pg / sqlite), Bun, Deno. Burning a hard dependency on one client
 * limits the gateway's reach. The adapter indirection costs ~5 lines per
 * driver in the consumer's code and keeps the package free of native deps.
 *
 * @example D1
 *   import { SqlTaskStore, d1ToSqlAdapter } from '@tangle-network/agent-gateway'
 *   const store = new SqlTaskStore(d1ToSqlAdapter(env.DB))
 *   await store.migrate()
 *   const gw = createAgentGateway({ ..., a2a: { taskStore: store } })
 *
 * @example libSQL / Turso
 *   import { createClient } from '@libsql/client'
 *   const client = createClient({ url: process.env.TURSO_URL!, authToken: process.env.TURSO_TOKEN! })
 *   const libsql: SqlAdapter = {
 *     exec: async (sql, params = []) => {
 *       const r = await client.execute({ sql, args: params as never[] })
 *       return { rowsAffected: Number(r.rowsAffected ?? 0) }
 *     },
 *     query: async (sql, params = []) => {
 *       const r = await client.execute({ sql, args: params as never[] })
 *       return r.rows as unknown as Record<string, unknown>[]
 *     },
 *   }
 *   const store = new SqlTaskStore(libsql)
 *   await store.migrate()
 */

import { hasPendingPaymentRecovery, type TaskStore } from './task-store'
import type { Task } from './types'

/**
 * Minimal SQL driver shape — identical to agent-runtime's `SqlAdapter` so the
 * same wrapper code works for both packages. Parameter placeholders MUST be
 * `?` (positional); driver wrappers that use `$1`, `$2`, … should rewrite at
 * the adapter boundary (see node-postgres example in the durability docs).
 */
export interface SqlAdapter {
  exec(sql: string, params?: readonly unknown[]): Promise<{ rowsAffected: number }>
  query<TRow = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
  ): Promise<TRow[]>
}

/**
 * Adapt a Cloudflare D1 binding to `SqlAdapter`. The package never imports
 * `@cloudflare/workers-types`; the binding's structural shape lines up via
 * TypeScript structural compatibility.
 */
export function d1ToSqlAdapter(db: D1DatabaseLike): SqlAdapter {
  return {
    async exec(sql, params = []) {
      const stmt = db.prepare(sql)
      const bound = params.length > 0 ? stmt.bind(...params) : stmt
      const result = await bound.run()
      const meta = (result as { meta?: { rows_written?: number; changes?: number } }).meta
      return { rowsAffected: meta?.rows_written ?? meta?.changes ?? 0 }
    },
    async query<TRow>(sql: string, params: readonly unknown[] = []): Promise<TRow[]> {
      const stmt = db.prepare(sql)
      const bound = params.length > 0 ? stmt.bind(...params) : stmt
      const result = await bound.all<TRow>()
      return result.results ?? []
    },
  }
}

export interface D1DatabaseLike {
  prepare(sql: string): D1StmtLike
}
export interface D1StmtLike {
  bind(...params: unknown[]): D1StmtLike
  run(): Promise<unknown>
  all<TRow = unknown>(): Promise<{ results?: TRow[] }>
}

const DEFAULT_TTL_MS = 60 * 60 * 1000

const TASKS_TABLE_DDL = (table: string) => `
  CREATE TABLE IF NOT EXISTS ${table} (
    id TEXT PRIMARY KEY,
    context_id TEXT NOT NULL,
    state TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  )
`
const CTX_INDEX_DDL = (table: string) => `
  CREATE INDEX IF NOT EXISTS idx_${table}_context ON ${table} (context_id, updated_at)
`

/**
 * SQL-backed TaskStore. Stores the full Task JSON; reads return a deep clone
 * so callers never observe shared references. TTL is enforced at read time:
 * expired rows are filtered out and (best-effort) deleted, matching the
 * in-memory store's semantics so behavior is portable across both adapters.
 */
export class SqlTaskStore implements TaskStore {
  constructor(
    private readonly db: SqlAdapter,
    private readonly opts: { ttlMs?: number; table?: string } = {},
  ) {}

  private get ttlMs(): number {
    return this.opts.ttlMs ?? DEFAULT_TTL_MS
  }
  private get table(): string {
    return this.opts.table ?? 'a2a_tasks'
  }

  private async readRow(id: string): Promise<{
    payload: string
    updatedAt: number
  } | undefined> {
    const rows = await this.db.query<{ payload: string; updated_at: number }>(
      `SELECT payload, updated_at FROM ${this.table} WHERE id = ?`,
      [id],
    )
    const row = rows[0]
    return row ? { payload: row.payload, updatedAt: row.updated_at } : undefined
  }

  private isExpired(updatedAt: number, task: Task): boolean {
    return Date.now() - updatedAt > this.ttlMs && !hasPendingPaymentRecovery(task)
  }

  private async deleteObservedRow(
    id: string,
    payload: string,
    updatedAt: number,
  ): Promise<number> {
    const result = await this.db.exec(
      `DELETE FROM ${this.table} WHERE id = ? AND payload = ? AND updated_at = ?`,
      [id, payload, updatedAt],
    )
    return result.rowsAffected
  }

  /** Idempotent. Call once at deploy. */
  async migrate(): Promise<void> {
    await this.db.exec(TASKS_TABLE_DDL(this.table))
    await this.db.exec(CTX_INDEX_DDL(this.table))
  }

  async get(id: string): Promise<Task | undefined> {
    const row = await this.readRow(id)
    if (!row) return undefined
    const task = JSON.parse(row.payload) as Task
    if (this.isExpired(row.updatedAt, task)) {
      // Delete only the version that was observed as stale. A refresh can reuse
      // the same payload, so payload equality and updated_at both fence the row.
      await this.deleteObservedRow(id, row.payload, row.updatedAt)
      return undefined
    }
    return task
  }

  private async insert(task: Task): Promise<number> {
    const payload = JSON.stringify(task)
    const result = await this.db.exec(
      `INSERT INTO ${this.table} (id, context_id, state, payload, updated_at) VALUES (?, ?, ?, ?, ?)`,
      [task.id, task.contextId, task.status.state, payload, Date.now()],
    )
    return result.rowsAffected
  }

  async put(task: Task): Promise<void> {
    const payload = JSON.stringify(task)
    const updatedAt = Date.now()
    // Adapter-agnostic upsert: try update, fall back to insert if no row
    // existed. Avoids needing ON CONFLICT (postgres) vs INSERT OR REPLACE
    // (sqlite/libSQL) divergence at the SQL layer.
    const updated = await this.db.exec(
      `UPDATE ${this.table} SET context_id = ?, state = ?, payload = ?, updated_at = ? WHERE id = ?`,
      [task.contextId, task.status.state, payload, updatedAt, task.id],
    )
    if (updated.rowsAffected === 0) {
      await this.db.exec(
        `INSERT INTO ${this.table} (id, context_id, state, payload, updated_at) VALUES (?, ?, ?, ?, ?)`,
        [task.id, task.contextId, task.status.state, payload, updatedAt],
      )
    }
  }

  async createIfAbsent(task: Task): Promise<boolean> {
    try {
      return (await this.insert(task)) === 1
    } catch (error) {
      // SQL dialects report duplicate primary keys as errors. Inspect the raw
      // row so an expired row can be removed and retried in the same call.
      const row = await this.readRow(task.id)
      if (!row) throw error
      const existing = JSON.parse(row.payload) as Task
      if (!this.isExpired(row.updatedAt, existing)) return false

      await this.deleteObservedRow(task.id, row.payload, row.updatedAt)
      try {
        return (await this.insert(task)) === 1
      } catch (retryError) {
        // Another writer may have won the retry after the stale row was
        // removed. Return the normal idempotency result in that case.
        if (await this.get(task.id)) return false
        throw retryError
      }
    }
  }

  async compareAndSet(expected: Task, next: Task): Promise<boolean> {
    const expectedPayload = JSON.stringify(expected)
    const payload = JSON.stringify(next)
    const result = await this.db.exec(
      `UPDATE ${this.table} SET context_id = ?, state = ?, payload = ?, updated_at = ? WHERE id = ? AND payload = ?`,
      [next.contextId, next.status.state, payload, Date.now(), expected.id, expectedPayload],
    )
    return result.rowsAffected === 1
  }

  async delete(id: string): Promise<void> {
    const task = await this.get(id)
    if (!task || hasPendingPaymentRecovery(task)) return
    await this.db.exec(
      `DELETE FROM ${this.table} WHERE id = ? AND payload = ?`,
      [id, JSON.stringify(task)],
    )
  }

  /**
   * Lookup tasks by contextId — used by `tasks/resubscribe` and the multi-turn
   * dispatcher. Returns most-recent-first. Not part of the base TaskStore
   * interface since the in-memory store doesn't expose it; consumers that
   * specifically wire SqlTaskStore can use it for richer queries.
   */
  async listByContext(contextId: string): Promise<Task[]> {
    const rows = await this.db.query<{ payload: string; updated_at: number }>(
      `SELECT payload, updated_at FROM ${this.table} WHERE context_id = ? ORDER BY updated_at DESC`,
      [contextId],
    )
    const now = Date.now()
    return rows
      .map((r) => ({ task: JSON.parse(r.payload) as Task, updatedAt: r.updated_at }))
      .filter(({ task, updatedAt }) =>
        now - updatedAt <= this.ttlMs || hasPendingPaymentRecovery(task),
      )
      .map(({ task }) => task)
  }
}
