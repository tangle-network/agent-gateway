/**
 * API key management — create, list, verify, revoke.
 *
 * The gateway package provides:
 * - Types and interfaces (ApiKeyStore)
 * - A Hono router for CRUD (createApiKeyRoutes)
 * - A verifyApiKey function that checks against the store
 *
 * Apps can use SqlApiKeyStore or provide a store for another database.
 */

import { Hono } from 'hono'
import type {
  ApiKeyRequestClaimInput,
  ApiKeyRequestClaimResult,
  PaymentResult,
} from './types'

// --- Types ---

export interface ApiKey {
  id: string
  userId: string
  name: string
  keyHash: string
  keyPrefix: string
  scopes: string[]
  rateLimit: number      // requests per rolling minute
  dailyLimit: number     // requests per UTC day
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

/** Persistent storage contract for API keys. */
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

  recordUsage(keyId: string, costCents: number, requestId?: string): Promise<void>

  /** Atomically count one request against the key's minute and daily limits. */
  claimRequest?(
    keyId: string,
    requestId: string,
    requestedAt?: Date,
  ): Promise<ApiKeyRequestClaimResult>
}

const API_KEY_CONSUMER_PREFIX = 'apikey:'

/** Convert a gateway USD settlement to the store's conservative whole cents. */
export function apiKeySettlementCostCents(costUsd: number): number {
  if (!Number.isFinite(costUsd) || costUsd < 0) {
    throw new TypeError('API key settlement cost must be a non-negative USD amount')
  }
  const rawCents = costUsd * 100
  const nearestCent = Math.round(rawCents)
  const tolerance = Number.EPSILON * Math.max(1, Math.abs(rawCents)) * 4
  const normalizedCents = Math.abs(rawCents - nearestCent) <= tolerance
    ? nearestCent
    : rawCents
  const costCents = Math.ceil(normalizedCents)
  if (!Number.isSafeInteger(costCents)) {
    throw new TypeError('API key settlement cost exceeds the cents range')
  }
  return costCents
}

/** Build the gateway settlement callback for an API-key store. */
export function createApiKeyUsageSettlement(
  store: Pick<ApiKeyStore, 'recordUsage'>,
): (payment: PaymentResult, costUsd: number) => Promise<void> {
  return async (payment, costUsd) => {
    if (
      payment.method !== 'apikey'
      || !payment.consumerId.startsWith(API_KEY_CONSUMER_PREFIX)
    ) {
      throw new TypeError('Unsupported API key settlement identity')
    }
    const keyId = payment.consumerId.slice(API_KEY_CONSUMER_PREFIX.length)
    if (!keyId) throw new TypeError('API key settlement key id is missing')
    await store.recordUsage(
      keyId,
      apiKeySettlementCostCents(costUsd),
      payment.requestId,
    )
  }
}

/** Connect a durable API-key store to the gateway request-claim callback. */
export function createApiKeyRequestClaim(
  store: { claimRequest: NonNullable<ApiKeyStore['claimRequest']> },
): (input: ApiKeyRequestClaimInput) => Promise<ApiKeyRequestClaimResult> {
  return (input) => store.claimRequest(
    input.keyInfo.keyId,
    input.requestId,
    input.requestedAt,
  )
}

export class ApiKeyRequestLimitExceededError extends Error {
  readonly code = 'api_key.request_limit_exceeded'

  constructor(readonly claim: ApiKeyRequestClaimResult) {
    super(`API key ${claim.reason ?? 'request'} limit exceeded`)
    this.name = 'ApiKeyRequestLimitExceededError'
  }
}

export class ApiKeyRequestClaimUnavailableError extends Error {
  readonly code = 'api_key.request_claim_unavailable'

  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'ApiKeyRequestClaimUnavailableError'
  }
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
): Promise<{ key: ApiKey; keyId: string; consumerId: string; ownerId: string; scopes: string[]; rateLimitPerMinute: number; dailyLimit: number } | null> {
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
    ownerId: key.userId,
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

function positiveInteger(value: unknown, fallback: number): number | null {
  if (value === undefined) return fallback
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
    ? value
    : null
}

export function createApiKeyRoutes(config: ApiKeyRoutesConfig) {
  const router = new Hono()
  const prefix = config.prefix ?? 'ak_'
  const validScopes = config.validScopes ?? ['chat']
  if (validScopes.length === 0) throw new TypeError('At least one API key scope is required')
  const defaultScope = validScopes.includes('chat') ? 'chat' : validScopes[0]

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

    let input: unknown
    try {
      input = await c.req.json()
    } catch {
      return c.json({ error: 'body must be valid JSON' }, 400)
    }
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      return c.json({ error: 'body must be a JSON object' }, 400)
    }
    const body = input as Record<string, unknown>
    const name = typeof body.name === 'string' ? body.name.trim() : ''
    if (!name) return c.json({ error: 'name is required' }, 400)

    if (body.scopes !== undefined && !Array.isArray(body.scopes)) {
      return c.json({ error: 'scopes must be an array' }, 400)
    }
    const requestedScopes = Array.isArray(body.scopes) ? body.scopes : [defaultScope]
    const scopes = [...new Set(requestedScopes.filter(
      (scope): scope is string => typeof scope === 'string' && validScopes.includes(scope),
    ))]
    if (scopes.length === 0) scopes.push(defaultScope)

    const rateLimit = positiveInteger(body.rateLimit, 60)
    if (rateLimit === null) return c.json({ error: 'rateLimit must be a positive integer' }, 400)
    const dailyLimit = positiveInteger(body.dailyLimit, 1_000)
    if (dailyLimit === null) return c.json({ error: 'dailyLimit must be a positive integer' }, 400)

    let spendingLimitCents: number | null = null
    if (body.spendingLimitCents !== undefined) {
      if (
        typeof body.spendingLimitCents !== 'number'
        || !Number.isSafeInteger(body.spendingLimitCents)
        || body.spendingLimitCents < 0
      ) {
        return c.json({ error: 'spendingLimitCents must be a non-negative integer' }, 400)
      }
      spendingLimitCents = body.spendingLimitCents
    }

    let expiresAt: Date | null = null
    if (body.expiresAt !== undefined) {
      if (typeof body.expiresAt !== 'string') {
        return c.json({ error: 'expiresAt must be a valid date string' }, 400)
      }
      expiresAt = new Date(body.expiresAt)
      if (Number.isNaN(expiresAt.getTime())) {
        return c.json({ error: 'expiresAt must be a valid date string' }, 400)
      }
    }

    const rawKey = generateRawKey(prefix)
    const keyHash = await hashKey(rawKey)
    const keyPrefix = rawKey.slice(0, prefix.length + 8)

    const created = await config.store.create(userId, {
      name,
      keyHash,
      keyPrefix,
      scopes,
      rateLimit,
      dailyLimit,
      spendingLimitCents,
      expiresAt,
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
