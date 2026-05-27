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
 * `InMemoryTaskStore` does — the gateway is single-writer per task id so a
 * stale row is invisible to callers regardless of when the row is physically
 * deleted.
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

import type { TaskStore } from './task-store'
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

  /** Idempotent. Call once at deploy. */
  async migrate(): Promise<void> {
    await this.db.exec(TASKS_TABLE_DDL(this.table))
    await this.db.exec(CTX_INDEX_DDL(this.table))
  }

  async get(id: string): Promise<Task | undefined> {
    const rows = await this.db.query<{ payload: string; updated_at: number }>(
      `SELECT payload, updated_at FROM ${this.table} WHERE id = ?`,
      [id],
    )
    const row = rows[0]
    if (!row) return undefined
    if (Date.now() - row.updated_at > this.ttlMs) {
      // Lazy GC. If the delete loses a race with another reader, that reader
      // observes either the stale-then-deleted task (returning undefined here)
      // or, after this delete commits, observes undefined directly — either
      // way callers see consistent "expired" semantics.
      void this.db.exec(`DELETE FROM ${this.table} WHERE id = ?`, [id])
      return undefined
    }
    return JSON.parse(row.payload) as Task
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

  async delete(id: string): Promise<void> {
    await this.db.exec(`DELETE FROM ${this.table} WHERE id = ?`, [id])
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
      .filter((r) => now - r.updated_at <= this.ttlMs)
      .map((r) => JSON.parse(r.payload) as Task)
  }
}
