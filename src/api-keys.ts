/**
 * API key management — create, list, verify, revoke.
 *
 * The gateway package provides:
 * - Types and interfaces (ApiKeyStore)
 * - A Hono router for CRUD (createApiKeyRoutes)
 * - A verifyApiKey function that checks against the store
 *
 * Each agent implements ApiKeyStore against their own DB.
 */

import { Hono } from 'hono'

// --- Types ---

export interface ApiKey {
  id: string
  userId: string
  name: string
  keyHash: string
  keyPrefix: string
  scopes: string[]
  rateLimit: number      // requests per minute
  dailyLimit: number     // requests per day
  spendingLimitCents: number | null  // max spend in cents (null = unlimited)
  spentCents: number     // running total spent
  lastUsedAt: Date | null
  expiresAt: Date | null
  createdAt: Date
}

export interface ApiKeyCreateRequest {
  name: string
  scopes?: string[]
  rateLimit?: number
  dailyLimit?: number
  spendingLimitCents?: number
  expiresAt?: string
}

/** Each agent implements this against their DB */
export interface ApiKeyStore {
  create(userId: string, data: {
    name: string
    keyHash: string
    keyPrefix: string
    scopes: string[]
    rateLimit: number
    dailyLimit: number
    spendingLimitCents: number | null
    expiresAt: Date | null
  }): Promise<ApiKey>

  list(userId: string): Promise<Omit<ApiKey, 'keyHash'>[]>

  findByHash(keyHash: string): Promise<ApiKey | null>

  delete(userId: string, keyId: string): Promise<boolean>

  recordUsage(keyId: string, costCents: number): Promise<void>
}

// --- Key generation ---

function generateRawKey(prefix: string): string {
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
  return `${prefix}${hex}`
}

async function hashKey(raw: string): Promise<string> {
  const encoded = new TextEncoder().encode(raw)
  const digest = await crypto.subtle.digest('SHA-256', encoded)
  return Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, '0')).join('')
}

// --- Verification ---

export async function verifyApiKeyFromStore(
  authHeader: string,
  store: ApiKeyStore,
  prefix = 'ak_',
): Promise<{ key: ApiKey; keyId: string; consumerId: string; scopes: string[]; rateLimitPerMinute: number; dailyLimit: number } | null> {
  const bearerPrefix = `Bearer ${prefix}`
  if (!authHeader.startsWith(bearerPrefix)) return null

  const rawKey = authHeader.slice(7) // strip "Bearer "
  const keyHash = await hashKey(rawKey)
  const key = await store.findByHash(keyHash)
  if (!key) return null

  // Check expiry
  if (key.expiresAt && key.expiresAt < new Date()) return null

  // Check spending limit
  if (key.spendingLimitCents !== null && key.spentCents >= key.spendingLimitCents) return null

  return {
    key,
    consumerId: `apikey:${key.id}`,
    keyId: key.id,
    scopes: key.scopes,
    rateLimitPerMinute: key.rateLimit,
    dailyLimit: key.dailyLimit,
  }
}

// --- CRUD Routes ---

export interface ApiKeyRoutesConfig {
  store: ApiKeyStore
  /** Get the authenticated user ID from the request. Return null if not authenticated. */
  getAuthUserId: (request: Request) => Promise<string | null>
  /** Key prefix (default: "ak_") */
  prefix?: string
  /** Valid scopes for this agent (default: ["chat"]) */
  validScopes?: string[]
}

export function createApiKeyRoutes(config: ApiKeyRoutesConfig) {
  const router = new Hono()
  const prefix = config.prefix ?? 'ak_'
  const validScopes = config.validScopes ?? ['chat']

  // List keys
  router.get('/', async (c) => {
    const userId = await config.getAuthUserId(c.req.raw)
    if (!userId) return c.json({ error: 'Unauthorized' }, 401)

    const keys = await config.store.list(userId)
    return c.json({ keys })
  })

  // Create key
  router.post('/', async (c) => {
    const userId = await config.getAuthUserId(c.req.raw)
    if (!userId) return c.json({ error: 'Unauthorized' }, 401)

    const body = await c.req.json<ApiKeyCreateRequest>()
    if (!body.name?.trim()) return c.json({ error: 'name is required' }, 400)

    const scopes = (body.scopes ?? ['chat']).filter(s => validScopes.includes(s))
    if (scopes.length === 0) scopes.push('chat')

    const rawKey = generateRawKey(prefix)
    const keyHash = await hashKey(rawKey)
    const keyPrefix = rawKey.slice(0, prefix.length + 8)

    const created = await config.store.create(userId, {
      name: body.name.trim(),
      keyHash,
      keyPrefix,
      scopes,
      rateLimit: body.rateLimit ?? 60,
      dailyLimit: body.dailyLimit ?? 1000,
      spendingLimitCents: body.spendingLimitCents ?? null,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
    })

    return c.json({
      key: rawKey,
      id: created.id,
      name: created.name,
      keyPrefix: created.keyPrefix,
      scopes: created.scopes,
      rateLimit: created.rateLimit,
      dailyLimit: created.dailyLimit,
      spendingLimitCents: created.spendingLimitCents,
      expiresAt: created.expiresAt,
      _notice: 'Store this key securely. It will not be shown again.',
    }, 201)
  })

  // Delete key
  router.delete('/:keyId', async (c) => {
    const userId = await config.getAuthUserId(c.req.raw)
    if (!userId) return c.json({ error: 'Unauthorized' }, 401)

    const deleted = await config.store.delete(userId, c.req.param('keyId'))
    if (!deleted) return c.json({ error: 'API key not found' }, 404)

    return c.json({ deleted: true })
  })

  return router
}
