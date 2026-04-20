/**
 * Sliding-window rate limiter.
 *
 * Two implementations:
 *   - MemoryRateLimitStore — single-worker, ephemeral, good for tests
 *   - KvRateLimitStore     — Cloudflare Workers KV, distributed
 */

export interface RateLimitConfig {
  /** Max requests per window (default: 60) */
  limit: number
  /** Window size in seconds (default: 60) */
  windowSeconds: number
}

export interface RateLimitResult {
  allowed: boolean
  remaining: number
  resetAt: number
  retryAfterSeconds?: number
}

export interface RateLimitStore {
  /** Get timestamps of recent requests for this key */
  get(key: string): Promise<number[]>
  /** Set timestamps for this key (with TTL) */
  set(key: string, timestamps: number[], ttlSeconds: number): Promise<void>
}

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/** In-memory rate limit store with periodic eviction */
export class MemoryRateLimitStore implements RateLimitStore {
  private store = new Map<string, { timestamps: number[]; expiresAt: number }>()
  private lastEviction = Date.now()

  async get(key: string): Promise<number[]> {
    this.evictExpired()
    const entry = this.store.get(key)
    if (!entry || entry.expiresAt < Date.now()) {
      this.store.delete(key)
      return []
    }
    return entry.timestamps
  }

  async set(key: string, timestamps: number[], ttlSeconds: number): Promise<void> {
    this.store.set(key, { timestamps, expiresAt: Date.now() + ttlSeconds * 1000 })
  }

  private evictExpired() {
    const now = Date.now()
    if (now - this.lastEviction < 30_000) return
    this.lastEviction = now
    for (const [key, entry] of this.store) {
      if (entry.expiresAt < now) this.store.delete(key)
    }
  }
}

// ---------------------------------------------------------------------------
// Cloudflare KV implementation
// ---------------------------------------------------------------------------

/** Minimal KV shape — see nonce-store.ts for rationale. */
export interface KVNamespace {
  get(key: string, options?: { type?: 'text' | 'json' }): Promise<string | null>
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>
  delete(key: string): Promise<void>
}

/**
 * KV-backed RateLimitStore for distributed Cloudflare Workers deployments.
 *
 * Stores timestamp arrays per consumer. Reads are O(1); writes replace the
 * full array (cap is already filtered by checkRateLimit before write).
 *
 * Consistency note: Workers KV is eventually consistent within ~60s. An
 * attacker sitting on two isolates could technically exceed the limit by
 * ~2x for that window. For payment rate limits this is acceptable; for
 * abuse prevention on free endpoints consider Durable Objects instead.
 */
export class KvRateLimitStore implements RateLimitStore {
  constructor(
    private readonly kv: KVNamespace,
    private readonly prefix: string = 'rl',
  ) {}

  async get(key: string): Promise<number[]> {
    const raw = await this.kv.get(this.key(key))
    if (!raw) return []
    try {
      const arr = JSON.parse(raw) as unknown
      return Array.isArray(arr) ? (arr as number[]).filter((t) => typeof t === 'number') : []
    } catch {
      return []
    }
  }

  async set(key: string, timestamps: number[], ttlSeconds: number): Promise<void> {
    // KV minimum TTL is 60 seconds
    const ttl = Math.max(ttlSeconds, 60)
    await this.kv.put(this.key(key), JSON.stringify(timestamps), { expirationTtl: ttl })
  }

  private key(key: string): string {
    return `${this.prefix}:${key}`
  }
}

// ---------------------------------------------------------------------------
// Core limiter
// ---------------------------------------------------------------------------

export async function checkRateLimit(
  consumerId: string,
  config: RateLimitConfig,
  store: RateLimitStore,
): Promise<RateLimitResult> {
  const now = Date.now()
  const windowMs = config.windowSeconds * 1000
  const cutoff = now - windowMs

  const key = `rl:${consumerId}`
  const timestamps = (await store.get(key)).filter(t => t > cutoff)

  if (timestamps.length >= config.limit) {
    const oldestInWindow = Math.min(...timestamps)
    const resetAt = oldestInWindow + windowMs
    return {
      allowed: false,
      remaining: 0,
      resetAt,
      retryAfterSeconds: Math.ceil((resetAt - now) / 1000),
    }
  }

  timestamps.push(now)
  await store.set(key, timestamps, config.windowSeconds * 2)

  return {
    allowed: true,
    remaining: config.limit - timestamps.length,
    resetAt: now + windowMs,
  }
}
