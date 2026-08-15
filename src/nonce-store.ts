/**
 * Nonce replay protection for x402/MPP payments.
 * Tracks seen nonces to prevent the same payment from being used twice.
 */

export interface NonceStore {
  /** Check if nonce has been seen. This method never grants ownership. */
  hasSeen(nonce: string): Promise<boolean>
  /**
   * Atomically claim a nonce. An owner id makes a retry by the same payment
   * operation idempotent. This is optional only for the 0.7.1 check-and-mark
   * compatibility contract; durable owner claims require this method.
   */
  claim?(nonce: string, ttlSeconds: number, ownerId?: string): Promise<boolean>
  /** @deprecated Use claim() for atomic ownership in new stores. */
  markSeen?(nonce: string, ttlSeconds: number): Promise<void>
}

export interface AtomicNonceStore extends NonceStore {
  claim(nonce: string, ttlSeconds: number, ownerId?: string): Promise<boolean>
}

/**
 * Return the seconds for which a signed nonce must remain stored.
 *
 * The signed expiry is the replay boundary. A fixed one-hour cap would allow
 * a still-valid authorization to replay after the nonce entry expires.
 */
export function nonceTtlSeconds(
  expiry: bigint,
  nowSeconds = Math.floor(Date.now() / 1000),
): number | undefined {
  const remaining = expiry - BigInt(nowSeconds)
  if (remaining <= 0n || remaining > BigInt(Number.MAX_SAFE_INTEGER)) return undefined
  return Math.max(Number(remaining), 60)
}

// ---------------------------------------------------------------------------
// In-memory implementation — single-worker, ephemeral
// ---------------------------------------------------------------------------

/** In-memory nonce store with automatic eviction. Use in tests or single-worker deploys. */
export class MemoryNonceStore implements NonceStore {
  private seen = new Map<string, { expiresAt: number; ownerId?: string }>()
  private lastEviction = Date.now()

  async hasSeen(nonce: string): Promise<boolean> {
    this.evictExpired()
    const entry = this.seen.get(nonce)
    if (!entry) return false
    if (entry.expiresAt < Date.now()) {
      this.seen.delete(nonce)
      return false
    }
    return true
  }

  async claim(nonce: string, ttlSeconds: number, ownerId?: string): Promise<boolean> {
    this.evictExpired()
    const now = Date.now()
    const entry = this.seen.get(nonce)
    if (entry !== undefined && entry.expiresAt >= now) {
      return ownerId !== undefined && entry.ownerId === ownerId
    }
    this.seen.set(nonce, { expiresAt: now + ttlSeconds * 1000, ownerId })
    return true
  }

  private evictExpired() {
    const now = Date.now()
    // Evict at most every 60 seconds to avoid O(n) on every request
    if (now - this.lastEviction < 60_000) return
    this.lastEviction = now
    for (const [nonce, entry] of this.seen) {
      if (entry.expiresAt < now) this.seen.delete(nonce)
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
  /** Linearizable create-if-absent extension. Cloudflare KV does not provide it. */
  putIfAbsent?(key: string, value: string, options?: { expirationTtl?: number }): Promise<boolean>
  delete(key: string): Promise<void>
}

/**
 * KV-backed NonceStore for distributed Cloudflare Workers deployments.
 *
 * Why this exists: MemoryNonceStore works on a single worker instance, but
 * Cloudflare routes requests across multiple isolates. Without shared state,
 * an attacker could retry a replayed nonce against a different isolate and
 * have it accepted. Version 2 requires an atomic binding for payment claims.
 * This store requires an atomic binding for every payment claim.
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
    return (await this.kv.get(this.key(nonce))) !== null
  }

  async claim(nonce: string, ttlSeconds: number, ownerId?: string): Promise<boolean> {
    if (!this.kv.putIfAbsent) {
      throw new Error('KvNonceStore requires an atomic putIfAbsent binding for payment claims')
    }
    const ttl = Math.max(ttlSeconds, 60)
    const key = this.key(nonce)
    const value = ownerId ?? '1'
    if (ownerId !== undefined) {
      const existing = await this.kv.get(key)
      if (existing !== null) return existing === ownerId
    }
    const inserted = await this.kv.putIfAbsent(key, value, { expirationTtl: ttl })
    if (inserted || ownerId === undefined) return inserted
    // Another isolate may have won between get and putIfAbsent. Re-read so a
    // retry by the same durable operation remains idempotent.
    return (await this.kv.get(key)) === ownerId
  }

  private key(nonce: string): string {
    return `${this.prefix}:${nonce}`
  }
}

/** Claim through the atomic contract or the explicit 0.7.1 legacy path. */
export async function claimStoredNonce(
  store: NonceStore,
  nonce: string,
  ttlSeconds: number,
  ownerId?: string,
): Promise<boolean> {
  if (typeof store.claim === 'function') return store.claim(nonce, ttlSeconds, ownerId)
  if (ownerId !== undefined) {
    throw new Error('NonceStore.claim is required for atomic payment ownership')
  }
  if (typeof store.markSeen !== 'function') {
    throw new Error('NonceStore.markSeen is required for legacy payment replay protection')
  }
  if (await store.hasSeen(nonce)) return false
  await store.markSeen(nonce, ttlSeconds)
  return true
}

/** Durable payment paths must use a store with a single atomic claim operation. */
export function isAtomicNonceStore(store: NonceStore): store is AtomicNonceStore {
  return typeof store.claim === 'function'
}
