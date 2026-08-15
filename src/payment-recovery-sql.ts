import type { SqlAdapter } from './a2a/task-store-sql'
import type {
  PaymentRecoveryRecord,
  PaymentRecoveryStore,
} from './payment-recovery'

const TABLE_DDL = (table: string) => `
  CREATE TABLE IF NOT EXISTS ${table} (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    next_attempt_at INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  )
`

const DUE_INDEX_DDL = (table: string) => `
  CREATE INDEX IF NOT EXISTS idx_${table}_due
  ON ${table} (state, next_attempt_at)
`

/** Durable recovery outbox for D1, sqlite, libSQL, or an adapted SQL driver. */
export class SqlPaymentRecoveryStore implements PaymentRecoveryStore {
  private readonly table: string

  constructor(
    private readonly db: SqlAdapter,
    options: { table?: string } = {},
  ) {
    this.table = options.table ?? 'payment_recovery'
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(this.table)) {
      throw new Error('payment recovery table name is invalid')
    }
  }

  /** Idempotent. Run this before the gateway starts accepting traffic. */
  async migrate(): Promise<void> {
    await this.db.exec(TABLE_DDL(this.table))
    await this.db.exec(DUE_INDEX_DDL(this.table))
  }

  async createIfAbsent(record: PaymentRecoveryRecord): Promise<boolean> {
    try {
      const result = await this.db.exec(
        `INSERT INTO ${this.table} (id, state, next_attempt_at, revision, payload, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
        [
          record.id,
          record.state,
          record.nextAttemptAt,
          record.revision,
          JSON.stringify(record),
          record.updatedAt,
        ],
      )
      return result.rowsAffected === 1
    } catch (error) {
      if (await this.get(record.id)) return false
      throw error
    }
  }

  async get(id: string): Promise<PaymentRecoveryRecord | undefined> {
    const rows = await this.db.query<{ payload: string }>(
      `SELECT payload FROM ${this.table} WHERE id = ?`,
      [id],
    )
    return rows[0] ? parseRecord(rows[0].payload) : undefined
  }

  async compareAndSet(
    expected: PaymentRecoveryRecord,
    next: PaymentRecoveryRecord,
  ): Promise<boolean> {
    const result = await this.db.exec(
      `UPDATE ${this.table} SET state = ?, next_attempt_at = ?, revision = ?, payload = ?, updated_at = ? WHERE id = ? AND revision = ?`,
      [
        next.state,
        next.nextAttemptAt,
        next.revision,
        JSON.stringify(next),
        next.updatedAt,
        expected.id,
        expected.revision,
      ],
    )
    return result.rowsAffected === 1
  }

  async listDue(now: number, limit: number): Promise<PaymentRecoveryRecord[]> {
    if (!Number.isSafeInteger(limit) || limit <= 0) {
      throw new Error('payment recovery scan limit must be a positive safe integer')
    }
    const rows = await this.db.query<{ payload: string }>(
      `SELECT payload FROM ${this.table} WHERE state <> ? AND next_attempt_at <= ? ORDER BY next_attempt_at ASC LIMIT ?`,
      ['reconciled', now, limit],
    )
    return rows.map((row) => parseRecord(row.payload))
  }
}

function parseRecord(payload: string): PaymentRecoveryRecord {
  const value = JSON.parse(payload) as PaymentRecoveryRecord
  if (value.version !== 1 || !value.id || !Number.isSafeInteger(value.revision)) {
    throw new Error('stored payment recovery record is invalid')
  }
  return value
}
