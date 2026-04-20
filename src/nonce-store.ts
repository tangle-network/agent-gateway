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

/** In-memory nonce store with automatic eviction */
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
