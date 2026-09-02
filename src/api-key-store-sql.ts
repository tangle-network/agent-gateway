import type { ApiKey, ApiKeyStore } from './api-keys'
import type { SqlAdapter } from './a2a/task-store-sql'
import { requireSqlIdentifier } from './sql'
import type { ApiKeyRequestClaimResult } from './types'

interface ApiKeyRow {
  id: string
  user_id: string
  name: string
  key_hash: string
  key_prefix: string
  scopes: string | null
  rate_limit: number
  daily_limit: number
  spending_limit_cents: number | null
  spent_cents: number
  last_used_at: number | null
  expires_at: number | null
  created_at: number
}

interface ApiKeyUsageRow {
  request_id: string
  key_id: string
  cost_cents: number | string | bigint
}

interface ApiKeyRequestStateRow {
  key_id: string
  reference_at: number | string | bigint
  day_bucket: number | string | bigint
  rate_limit: number | string | bigint
  daily_limit: number | string | bigint
  minute_count: number | string | bigint
  day_count: number | string | bigint
  oldest_minute_at: number | string | bigint
}

type PublicApiKeyRow = Omit<ApiKeyRow, 'key_hash'>

export interface SqlApiKeyStoreOptions {
  table?: string
  usageTable?: string
  requestTable?: string
}

export function sqlApiKeyStoreSchemaStatements(
  options: SqlApiKeyStoreOptions = {},
): readonly string[] {
  const table = requireSqlIdentifier(options.table ?? 'agent_api_key')
  const usageTable = requireSqlIdentifier(options.usageTable ?? `${table}_usage`)
  const requestTable = requireSqlIdentifier(options.requestTable ?? `${table}_request`)
  return [
    `CREATE TABLE IF NOT EXISTS ${table} (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      name TEXT NOT NULL,
      key_hash TEXT NOT NULL,
      key_prefix TEXT NOT NULL,
      scopes TEXT NOT NULL,
      rate_limit INTEGER NOT NULL,
      daily_limit INTEGER NOT NULL,
      spending_limit_cents BIGINT,
      spent_cents BIGINT NOT NULL DEFAULT 0,
      last_used_at BIGINT,
      expires_at BIGINT,
      created_at BIGINT NOT NULL
    )`,
    `CREATE INDEX IF NOT EXISTS idx_${table}_user ON ${table} (user_id, created_at)`,
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_${table}_hash_unique ON ${table} (key_hash)`,
    `CREATE TABLE IF NOT EXISTS ${usageTable} (
      request_id TEXT PRIMARY KEY,
      key_id TEXT NOT NULL,
      cost_cents BIGINT NOT NULL,
      created_at BIGINT NOT NULL,
      FOREIGN KEY (key_id) REFERENCES ${table}(id) ON DELETE CASCADE
    )`,
    `CREATE INDEX IF NOT EXISTS idx_${usageTable}_key ON ${usageTable} (key_id, created_at)`,
    `CREATE TABLE IF NOT EXISTS ${requestTable} (
      request_id TEXT PRIMARY KEY,
      key_id TEXT NOT NULL,
      claim_sequence BIGINT NOT NULL,
      day_bucket BIGINT NOT NULL,
      created_at BIGINT NOT NULL,
      FOREIGN KEY (key_id) REFERENCES ${table}(id) ON DELETE CASCADE
    )`,
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_${requestTable}_sequence
      ON ${requestTable} (key_id, claim_sequence)`,
    `CREATE INDEX IF NOT EXISTS idx_${requestTable}_window
      ON ${requestTable} (key_id, created_at)`,
    `CREATE INDEX IF NOT EXISTS idx_${requestTable}_day
      ON ${requestTable} (key_id, day_bucket)`,
  ]
}

function parseScopes(value: string | null): string[] {
  if (value === null) return ['chat']
  const parsed = JSON.parse(value) as unknown
  if (!Array.isArray(parsed) || !parsed.every((scope) => typeof scope === 'string')) {
    throw new TypeError('Stored API key scopes must be a string array')
  }
  return parsed
}

function fromSqlTimestamp(value: number | null): Date | null {
  return value === null ? null : new Date(value * 1_000)
}

function toSqlTimestamp(value: Date): number {
  return Math.floor(value.getTime() / 1_000)
}

function rowToApiKey(row: ApiKeyRow): ApiKey {
  return {
    id: row.id,
    userId: row.user_id,
    name: row.name,
    keyHash: row.key_hash,
    keyPrefix: row.key_prefix,
    scopes: parseScopes(row.scopes),
    rateLimit: row.rate_limit,
    dailyLimit: row.daily_limit,
    spendingLimitCents: row.spending_limit_cents,
    spentCents: row.spent_cents,
    lastUsedAt: fromSqlTimestamp(row.last_used_at),
    expiresAt: fromSqlTimestamp(row.expires_at),
    createdAt: fromSqlTimestamp(row.created_at)!,
  }
}

function publicRowToApiKey(row: PublicApiKeyRow): Omit<ApiKey, 'keyHash'> {
  const { keyHash: _keyHash, ...key } = rowToApiKey({ ...row, key_hash: '' })
  return key
}

/** Durable API-key storage for D1, SQLite, libSQL, and compatible SQL drivers. */
export class SqlApiKeyStore implements ApiKeyStore {
  private readonly table: string
  private readonly usageTable: string
  private readonly requestTable: string

  constructor(
    private readonly db: SqlAdapter,
    options: SqlApiKeyStoreOptions = {},
  ) {
    this.table = requireSqlIdentifier(options.table ?? 'agent_api_key')
    this.usageTable = requireSqlIdentifier(options.usageTable ?? `${this.table}_usage`)
    this.requestTable = requireSqlIdentifier(options.requestTable ?? `${this.table}_request`)
    if (new Set([this.table, this.usageTable, this.requestTable]).size !== 3) {
      throw new TypeError('API key, usage, and request table names must differ')
    }
  }

  /** Idempotent. Call once during deployment. */
  async migrate(): Promise<void> {
    for (const statement of sqlApiKeyStoreSchemaStatements({
      table: this.table,
      usageTable: this.usageTable,
      requestTable: this.requestTable,
    })) {
      await this.db.exec(statement)
    }
  }

  async create(
    userId: string,
    data: {
      name: string
      keyHash: string
      keyPrefix: string
      scopes: string[]
      rateLimit: number
      dailyLimit: number
      spendingLimitCents: number | null
      expiresAt: Date | null
    },
  ): Promise<ApiKey> {
    const id = crypto.randomUUID().replaceAll('-', '')
    const createdAt = Math.floor(Date.now() / 1_000)
    await this.db.exec(
      `INSERT INTO ${this.table} (
        id, user_id, name, key_hash, key_prefix, scopes, rate_limit, daily_limit,
        spending_limit_cents, spent_cents, last_used_at, expires_at, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?)`,
      [
        id,
        userId,
        data.name,
        data.keyHash,
        data.keyPrefix,
        JSON.stringify(data.scopes),
        data.rateLimit,
        data.dailyLimit,
        data.spendingLimitCents,
        data.expiresAt ? toSqlTimestamp(data.expiresAt) : null,
        createdAt,
      ],
    )
    return {
      id,
      userId,
      ...data,
      expiresAt: data.expiresAt ? fromSqlTimestamp(toSqlTimestamp(data.expiresAt)) : null,
      spentCents: 0,
      lastUsedAt: null,
      createdAt: fromSqlTimestamp(createdAt)!,
    }
  }

  async list(userId: string): Promise<Omit<ApiKey, 'keyHash'>[]> {
    const rows = await this.db.query<PublicApiKeyRow>(
      `SELECT k.id, k.user_id, k.name, k.key_prefix, k.scopes, k.rate_limit, k.daily_limit,
        k.spending_limit_cents,
        k.spent_cents + COALESCE((
          SELECT SUM(u.cost_cents) FROM ${this.usageTable} AS u WHERE u.key_id = k.id
        ), 0) AS spent_cents,
        COALESCE((
          SELECT MAX(u.created_at) FROM ${this.usageTable} AS u WHERE u.key_id = k.id
        ), k.last_used_at) AS last_used_at,
        k.expires_at, k.created_at
       FROM ${this.table} AS k
       WHERE k.user_id = ?
       ORDER BY k.created_at DESC`,
      [userId],
    )
    return rows.map(publicRowToApiKey)
  }

  async findByHash(keyHash: string): Promise<ApiKey | null> {
    const rows = await this.db.query<ApiKeyRow>(
      `SELECT k.id, k.user_id, k.name, k.key_hash, k.key_prefix, k.scopes,
        k.rate_limit, k.daily_limit, k.spending_limit_cents,
        k.spent_cents + COALESCE((
          SELECT SUM(u.cost_cents) FROM ${this.usageTable} AS u WHERE u.key_id = k.id
        ), 0) AS spent_cents,
        COALESCE((
          SELECT MAX(u.created_at) FROM ${this.usageTable} AS u WHERE u.key_id = k.id
        ), k.last_used_at) AS last_used_at,
        k.expires_at, k.created_at
       FROM ${this.table} AS k
       WHERE k.key_hash = ?
       LIMIT 1`,
      [keyHash],
    )
    return rows[0] ? rowToApiKey(rows[0]) : null
  }

  async delete(userId: string, keyId: string): Promise<boolean> {
    const result = await this.db.exec(
      `DELETE FROM ${this.table} WHERE id = ? AND user_id = ?`,
      [keyId, userId],
    )
    return result.rowsAffected > 0
  }

  async claimRequest(
    keyId: string,
    requestId: string,
    requestedAt = new Date(),
  ): Promise<ApiKeyRequestClaimResult> {
    if (!keyId) throw new TypeError('API key request key id is required')
    if (!requestId || requestId.length > 128) {
      throw new TypeError('API key request id must contain 1 to 128 characters')
    }
    const requestedAtMs = requestedAt.getTime()
    if (!Number.isFinite(requestedAtMs)) {
      throw new TypeError('API key request time must be valid')
    }
    const createdAt = Math.floor(requestedAtMs / 1_000)
    const minuteCutoff = createdAt - 60
    const dayBucket = Math.floor(createdAt / 86_400)

    for (let attempt = 0; attempt < 64; attempt += 1) {
      const existing = await this.readRequestClaim(requestId)
      if (existing) {
        if (existing.key_id !== keyId) {
          throw new Error('API key request id was reused by another key')
        }
        return requestClaimResult(existing, true)
      }

      const inserted = await this.db.exec(
        `INSERT INTO ${this.requestTable} (
          request_id, key_id, claim_sequence, day_bucket, created_at
        )
        SELECT ?, k.id,
          COALESCE((
            SELECT MAX(previous.claim_sequence) FROM ${this.requestTable} AS previous
            WHERE previous.key_id = k.id
          ), 0) + 1,
          ?,
          ?
        FROM ${this.table} AS k
        WHERE k.id = ?
          AND COALESCE((
            SELECT COUNT(*) FROM ${this.requestTable} AS recent
            WHERE recent.key_id = k.id AND recent.created_at > ?
          ), 0) < k.rate_limit
          AND COALESCE((
            SELECT COUNT(*) FROM ${this.requestTable} AS today
            WHERE today.key_id = k.id AND today.day_bucket = ?
          ), 0) < k.daily_limit
        ON CONFLICT DO NOTHING`,
        [
          requestId,
          dayBucket,
          createdAt,
          keyId,
          minuteCutoff,
          dayBucket,
        ],
      )
      if (inserted.rowsAffected === 1) {
        const claim = await this.readRequestClaim(requestId)
        if (!claim) throw new Error('API key request claim was not readable after insert')
        return requestClaimResult(claim, true)
      }

      const state = await this.readRequestState(keyId, createdAt, dayBucket)
      if (!state) throw new Error('API key no longer exists')
      const rateLimit = sqlSafeInteger(state.rate_limit, 'rate_limit')
      const dailyLimit = sqlSafeInteger(state.daily_limit, 'daily_limit')
      const minuteCount = sqlSafeInteger(state.minute_count, 'minute_count')
      const dayCount = sqlSafeInteger(state.day_count, 'day_count')
      if (minuteCount >= rateLimit || dayCount >= dailyLimit) {
        return requestClaimResult(state, false)
      }
    }

    throw new Error('API key request claim contention did not settle')
  }

  async recordUsage(keyId: string, costCents: number, requestId?: string): Promise<void> {
    if (!Number.isSafeInteger(costCents) || costCents < 0) {
      throw new TypeError('API key usage cost must be a non-negative integer number of cents')
    }
    const usageRequestId = requestId ?? crypto.randomUUID()
    if (!usageRequestId || usageRequestId.length > 128) {
      throw new TypeError('API key usage request id must contain 1 to 128 characters')
    }
    const createdAt = Math.floor(Date.now() / 1_000)
    const result = await this.db.exec(
      `INSERT INTO ${this.usageTable} (request_id, key_id, cost_cents, created_at)
       SELECT ?, k.id, ?, ?
       FROM ${this.table} AS k
       WHERE k.id = ?
         AND (
           k.spending_limit_cents IS NULL OR
           k.spent_cents + COALESCE((
             SELECT SUM(u.cost_cents) FROM ${this.usageTable} AS u WHERE u.key_id = k.id
           ), 0) + ? <= k.spending_limit_cents
         )
       ON CONFLICT(request_id) DO NOTHING`,
      [usageRequestId, costCents, createdAt, keyId, costCents],
    )
    if (result.rowsAffected === 1) return

    const existing = (await this.db.query<ApiKeyUsageRow>(
      `SELECT request_id, key_id, cost_cents FROM ${this.usageTable} WHERE request_id = ?`,
      [usageRequestId],
    ))[0]
    if (existing) {
      if (
        existing.key_id === keyId
        && sqlSafeInteger(existing.cost_cents, 'cost_cents') === costCents
      ) return
      throw new Error('API key usage request id was reused with different usage')
    }

    const key = (await this.db.query<{
      spending_limit_cents: number | null
      spent_cents: number
    }>(
      `SELECT k.spending_limit_cents,
        k.spent_cents + COALESCE((
          SELECT SUM(u.cost_cents) FROM ${this.usageTable} AS u WHERE u.key_id = k.id
        ), 0) AS spent_cents
       FROM ${this.table} AS k WHERE k.id = ?`,
      [keyId],
    ))[0]
    if (!key) throw new Error('API key no longer exists')
    if (key.spending_limit_cents === null) {
      throw new Error('API key usage could not be recorded')
    }
    throw new ApiKeySpendingLimitExceededError(
      keyId,
      key.spending_limit_cents,
      key.spent_cents,
      costCents,
    )
  }

  private async readRequestClaim(requestId: string): Promise<ApiKeyRequestStateRow | undefined> {
    return (await this.db.query<ApiKeyRequestStateRow>(
      `SELECT r.key_id, r.created_at AS reference_at, r.day_bucket,
        k.rate_limit, k.daily_limit,
        COALESCE((
          SELECT COUNT(*) FROM ${this.requestTable} AS recent
          WHERE recent.key_id = r.key_id
            AND recent.created_at > r.created_at - 60
            AND recent.created_at <= r.created_at
        ), 0) AS minute_count,
        COALESCE((
          SELECT COUNT(*) FROM ${this.requestTable} AS today
          WHERE today.key_id = r.key_id AND today.day_bucket = r.day_bucket
        ), 0) AS day_count,
        COALESCE((
          SELECT MIN(recent.created_at) FROM ${this.requestTable} AS recent
          WHERE recent.key_id = r.key_id
            AND recent.created_at > r.created_at - 60
            AND recent.created_at <= r.created_at
        ), r.created_at) AS oldest_minute_at
       FROM ${this.requestTable} AS r
       JOIN ${this.table} AS k ON k.id = r.key_id
       WHERE r.request_id = ?`,
      [requestId],
    ))[0]
  }

  private async readRequestState(
    keyId: string,
    createdAt: number,
    dayBucket: number,
  ): Promise<ApiKeyRequestStateRow | undefined> {
    return (await this.db.query<ApiKeyRequestStateRow>(
      `SELECT k.id AS key_id, ? AS reference_at, ? AS day_bucket,
        k.rate_limit, k.daily_limit,
        COALESCE((
          SELECT COUNT(*) FROM ${this.requestTable} AS recent
          WHERE recent.key_id = k.id AND recent.created_at > ?
        ), 0) AS minute_count,
        COALESCE((
          SELECT COUNT(*) FROM ${this.requestTable} AS today
          WHERE today.key_id = k.id AND today.day_bucket = ?
        ), 0) AS day_count,
        COALESCE((
          SELECT MIN(recent.created_at) FROM ${this.requestTable} AS recent
          WHERE recent.key_id = k.id AND recent.created_at > ?
        ), ?) AS oldest_minute_at
       FROM ${this.table} AS k WHERE k.id = ?`,
      [
        createdAt,
        dayBucket,
        createdAt - 60,
        dayBucket,
        createdAt - 60,
        createdAt,
        keyId,
      ],
    ))[0]
  }
}

function requestClaimResult(
  row: ApiKeyRequestStateRow,
  allowed: boolean,
): ApiKeyRequestClaimResult {
  const referenceAt = sqlSafeInteger(row.reference_at, 'reference_at')
  const dayBucket = sqlSafeInteger(row.day_bucket, 'day_bucket')
  const rateLimit = sqlSafeInteger(row.rate_limit, 'rate_limit')
  const dailyLimit = sqlSafeInteger(row.daily_limit, 'daily_limit')
  const minuteCount = sqlSafeInteger(row.minute_count, 'minute_count')
  const dayCount = sqlSafeInteger(row.day_count, 'day_count')
  const oldestMinuteAt = sqlSafeInteger(row.oldest_minute_at, 'oldest_minute_at')
  const dailyExceeded = dayCount >= dailyLimit
  return {
    allowed,
    ...(!allowed ? { reason: dailyExceeded ? 'daily' as const : 'minute' as const } : {}),
    minuteRemaining: Math.max(0, rateLimit - minuteCount),
    dailyRemaining: Math.max(0, dailyLimit - dayCount),
    minuteResetAt: Math.max(referenceAt, oldestMinuteAt + 60) * 1_000,
    dailyResetAt: (dayBucket + 1) * 86_400_000,
  }
}

function sqlSafeInteger(value: number | string | bigint, column: string): number {
  const parsed = typeof value === 'number' ? value : Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw new TypeError(`${column} must be a safe integer`)
  }
  return parsed
}

export class ApiKeySpendingLimitExceededError extends Error {
  readonly code = 'api_key.spending_limit_exceeded'

  constructor(
    readonly keyId: string,
    readonly spendingLimitCents: number | null,
    readonly spentCents: number,
    readonly requestedCents: number,
  ) {
    super(`API key spending limit would be exceeded for key ${keyId}`)
    this.name = 'ApiKeySpendingLimitExceededError'
  }
}
