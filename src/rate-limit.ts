/**
 * Sliding window rate limiter.
 * In-memory by default. Override with KV-backed store for Workers.
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
