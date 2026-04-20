/**
 * Nonce replay protection for x402/MPP payments.
 * Tracks seen nonces to prevent the same payment from being used twice.
 */

export interface NonceStore {
  /** Check if nonce has been seen. Returns true if already used (reject). */
  hasSeen(nonce: string): Promise<boolean>
  /** Mark nonce as used. TTL = how long to remember it (seconds). */
  markSeen(nonce: string, ttlSeconds: number): Promise<void>
}

// ---------------------------------------------------------------------------
// In-memory implementation — single-worker, ephemeral
// ---------------------------------------------------------------------------

/** In-memory nonce store with automatic eviction. Use in tests or single-worker deploys. */
export class MemoryNonceStore implements NonceStore {
  private seen = new Map<string, number>() // nonce → expiresAt
  private lastEviction = Date.now()

  async hasSeen(nonce: string): Promise<boolean> {
    this.evictExpired()
    const expiresAt = this.seen.get(nonce)
    if (!expiresAt) return false
    if (expiresAt < Date.now()) {
      this.seen.delete(nonce)
      return false
    }
    return true
  }

  async markSeen(nonce: string, ttlSeconds: number): Promise<void> {
    this.seen.set(nonce, Date.now() + ttlSeconds * 1000)
    this.evictExpired()
  }

  private evictExpired() {
    const now = Date.now()
    // Evict at most every 60 seconds to avoid O(n) on every request
    if (now - this.lastEviction < 60_000) return
    this.lastEviction = now
    for (const [nonce, expiresAt] of this.seen) {
      if (expiresAt < now) this.seen.delete(nonce)
    }
  }
}

// ---------------------------------------------------------------------------
// Cloudflare KV implementation — multi-worker, distributed
// ---------------------------------------------------------------------------

/**
 * Minimal KVNamespace shape — matches Cloudflare Workers' @cloudflare/workers-types
 * without pulling that package as a dep. Production consumers cast their KV
 * binding to this interface at the construction site.
 */
export interface KVNamespace {
  get(key: string, options?: { type?: 'text' | 'json' }): Promise<string | null>
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>
  delete(key: string): Promise<void>
}

/**
 * KV-backed NonceStore for distributed Cloudflare Workers deployments.
 *
 * Why this exists: MemoryNonceStore works on a single worker instance, but
 * Cloudflare routes requests across multiple isolates. Without shared state,
 * an attacker could retry a replayed nonce against a different isolate and
 * have it accepted. This implementation uses Workers KV with native TTL so
 * the nonce automatically expires at payment-expiry time.
 *
 * TTL precision: KV is eventually consistent (propagation ~60s). For x402
 * with 10-minute expiry windows this is fine — by the time KV propagates,
 * the payment itself would be expired anyway.
 *
 * Usage:
 *   const nonceStore = new KvNonceStore(env.NONCE_KV, 'x402')
 *   createAgentGateway({ ...config, nonceStore })
 */
export class KvNonceStore implements NonceStore {
  constructor(
    private readonly kv: KVNamespace,
    /** Key prefix to namespace within a shared KV (default: "nonce"). */
    private readonly prefix: string = 'nonce',
  ) {}

  async hasSeen(nonce: string): Promise<boolean> {
    const value = await this.kv.get(this.key(nonce))
    return value !== null
  }

  async markSeen(nonce: string, ttlSeconds: number): Promise<void> {
    // KV minimum TTL is 60 seconds
    const ttl = Math.max(ttlSeconds, 60)
    await this.kv.put(this.key(nonce), '1', { expirationTtl: ttl })
  }

  private key(nonce: string): string {
    return `${this.prefix}:${nonce}`
  }
}
