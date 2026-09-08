import type { SqlAdapter } from './a2a/task-store-sql'

/** Reservations outlive rate-counter pruning. A usage row atomically closes its matching hold. */
export class ApiKeyReservationsSql {
  constructor(
    private readonly db: SqlAdapter,
    readonly table: string,
    private readonly keys: string,
    private readonly usage: string,
  ) {}

  outstanding(excludeRequest = false): string {
    return `COALESCE((SELECT SUM(r.reserved_cents) FROM ${this.table} r
      WHERE r.key_id = k.id AND r.state <> 'released'
      ${excludeRequest ? 'AND r.request_id <> ?' : ''}
      AND NOT EXISTS (SELECT 1 FROM ${this.usage} u WHERE u.request_id = r.request_id)), 0)`
  }

  async reserve(keyId: string, requestId: string, cents: number): Promise<boolean> {
    if (!Number.isSafeInteger(cents) || cents < 0) throw new TypeError('Reservation cents must be a non-negative safe integer')
    await this.db.exec(`INSERT INTO ${this.table} (request_id, key_id, reserved_cents, state, created_at)
      SELECT ?, k.id, ?, 'reserved', ? FROM ${this.keys} k
      WHERE k.id = ? AND (k.expires_at IS NULL OR k.expires_at > ?)
      AND (k.spending_limit_cents IS NULL OR k.spent_cents
        + COALESCE((SELECT SUM(u.cost_cents) FROM ${this.usage} u WHERE u.key_id = k.id), 0)
        + ${this.outstanding()} + ? <= k.spending_limit_cents)
      ON CONFLICT(request_id) DO NOTHING`, [requestId, cents, Math.floor(Date.now() / 1000), keyId, Math.floor(Date.now() / 1000), cents])
    const row = (await this.db.query<{ key_id: string; reserved_cents: number; state: string }>(
      `SELECT key_id, reserved_cents, state FROM ${this.table} WHERE request_id = ?`, [requestId],
    ))[0]
    if (!row) return false
    if (row.key_id !== keyId || Number(row.reserved_cents) !== cents) throw new Error('API key reservation id was reused with different terms')
    if (row.state !== 'reserved') throw new Error('API key reservation is no longer available for execution')
    if ((await this.db.query(`SELECT request_id FROM ${this.usage} WHERE request_id = ?`, [requestId])).length) {
      throw new Error('API key reservation is already settled')
    }
    return true
  }

  async begin(keyId: string, requestId: string): Promise<void> {
    const result = await this.db.exec(`UPDATE ${this.table} SET state = 'executing'
      WHERE request_id = ? AND key_id = ? AND state = 'reserved'
      AND EXISTS (SELECT 1 FROM ${this.keys} k WHERE k.id = ? AND (k.expires_at IS NULL OR k.expires_at > ?)
        AND (k.spending_limit_cents IS NULL OR k.spent_cents
          + COALESCE((SELECT SUM(u.cost_cents) FROM ${this.usage} u WHERE u.key_id = k.id), 0)
          + ${this.outstanding()} <= k.spending_limit_cents))
      AND NOT EXISTS (SELECT 1 FROM ${this.usage} WHERE request_id = ?)`,
    [requestId, keyId, keyId, Math.floor(Date.now() / 1000), requestId])
    if (result.rowsAffected !== 1) throw new Error('API key reservation cannot begin execution')
  }

  async release(keyId: string, requestId: string): Promise<void> {
    // An execution handoff is uncertain even when it produced no visible output.
    await this.db.exec(`UPDATE ${this.table} SET state = 'released'
      WHERE request_id = ? AND key_id = ? AND state = 'reserved'`, [requestId, keyId])
  }
}
