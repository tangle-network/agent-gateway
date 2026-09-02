import type { ApiKey, ApiKeyStore } from './api-keys'
import type { SqlAdapter } from './a2a/task-store-sql'
import { requireSqlIdentifier } from './sql'

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

type PublicApiKeyRow = Omit<ApiKeyRow, 'key_hash'>

export interface SqlApiKeyStoreOptions {
  table?: string
}

export function sqlApiKeyStoreSchemaStatements(
  options: SqlApiKeyStoreOptions = {},
): readonly [string, string, string] {
  const table = requireSqlIdentifier(options.table ?? 'agent_api_key')
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
      spending_limit_cents INTEGER,
      spent_cents INTEGER NOT NULL DEFAULT 0,
      last_used_at INTEGER,
      expires_at INTEGER,
      created_at INTEGER NOT NULL
    )`,
    `CREATE INDEX IF NOT EXISTS idx_${table}_user ON ${table} (user_id, created_at)`,
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_${table}_hash ON ${table} (key_hash)`,
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

  constructor(
    private readonly db: SqlAdapter,
    options: SqlApiKeyStoreOptions = {},
  ) {
    this.table = requireSqlIdentifier(options.table ?? 'agent_api_key')
  }

  /** Idempotent. Call once during deployment. */
  async migrate(): Promise<void> {
    for (const statement of sqlApiKeyStoreSchemaStatements({ table: this.table })) {
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
      `SELECT id, user_id, name, key_prefix, scopes, rate_limit, daily_limit,
        spending_limit_cents, spent_cents, last_used_at, expires_at, created_at
       FROM ${this.table}
       WHERE user_id = ?
       ORDER BY created_at DESC`,
      [userId],
    )
    return rows.map(publicRowToApiKey)
  }

  async findByHash(keyHash: string): Promise<ApiKey | null> {
    const rows = await this.db.query<ApiKeyRow>(
      `SELECT id, user_id, name, key_hash, key_prefix, scopes, rate_limit, daily_limit,
        spending_limit_cents, spent_cents, last_used_at, expires_at, created_at
       FROM ${this.table}
       WHERE key_hash = ?
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

  async recordUsage(keyId: string, costCents: number): Promise<void> {
    if (!Number.isSafeInteger(costCents) || costCents < 0) {
      throw new TypeError('API key usage cost must be a non-negative integer number of cents')
    }
    await this.db.exec(
      `UPDATE ${this.table}
       SET spent_cents = spent_cents + ?, last_used_at = ?
       WHERE id = ?`,
      [costCents, Math.floor(Date.now() / 1_000), keyId],
    )
  }
}
