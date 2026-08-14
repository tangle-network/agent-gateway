import { describe, it, expect, vi, afterEach } from 'vitest'
import { KvNonceStore, type KVNamespace as NonceKV } from '../src/nonce-store'
import { KvRateLimitStore, checkRateLimit } from '../src/rate-limit'
import type { KVNamespace as RlKV } from '../src/rate-limit'

/** Minimal in-memory KV that honors expirationTtl so we can simulate real KV semantics */
class StubKV implements NonceKV, RlKV {
  private store = new Map<string, { value: string; expiresAt: number }>()
  private now = () => Date.now()

  async get(key: string): Promise<string | null> {
    const entry = this.store.get(key)
    if (!entry) return null
    if (entry.expiresAt < this.now()) {
      this.store.delete(key)
      return null
    }
    return entry.value
  }

  async put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void> {
    const ttl = options?.expirationTtl ?? 86400
    this.store.set(key, { value, expiresAt: this.now() + ttl * 1000 })
  }

  async putIfAbsent(key: string, value: string, options?: { expirationTtl?: number }): Promise<boolean> {
    const entry = this.store.get(key)
    if (entry && entry.expiresAt >= this.now()) return false
    const ttl = options?.expirationTtl ?? 86400
    this.store.set(key, { value, expiresAt: this.now() + ttl * 1000 })
    return true
  }

  async delete(key: string): Promise<void> {
    this.store.delete(key)
  }
}

describe('KvNonceStore', () => {
  afterEach(() => vi.useRealTimers())

  it('claims an unseen nonce and rejects its replay', async () => {
    const store = new KvNonceStore(new StubKV())
    expect(await store.claim('fresh', 60)).toBe(true)
    expect(await store.claim('fresh', 60)).toBe(false)
  })

  it('retains an atomic claim — regression: missed replay would let attackers reuse payments', async () => {
    const store = new KvNonceStore(new StubKV())
    expect(await store.claim('replay', 3600)).toBe(true)
    expect(await store.claim('replay', 3600)).toBe(false)
  })

  it('enforces 60s minimum TTL — regression: KV rejects shorter TTLs so shorter expiries silently drop', async () => {
    const kv = new StubKV()
    const putIfAbsentSpy = vi.spyOn(kv, 'putIfAbsent')
    const store = new KvNonceStore(kv)
    await store.claim('n1', 10) // request 10 seconds
    expect(putIfAbsentSpy).toHaveBeenCalledWith(expect.stringContaining('nonce:'), '1', { expirationTtl: 60 })
  })

  it('honors TTL expiry via KV eviction semantics', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new KvNonceStore(new StubKV())
    expect(await store.claim('fleeting', 60)).toBe(true)
    expect(await store.claim('fleeting', 60)).toBe(false)
    vi.advanceTimersByTime(61_000)
    expect(await store.claim('fleeting', 60)).toBe(true)
  })

  it('namespaces by prefix — regression: collisions across shared KVs', async () => {
    const kv = new StubKV()
    const x402 = new KvNonceStore(kv, 'x402')
    const mpp = new KvNonceStore(kv, 'mpp')
    expect(await x402.claim('42', 300)).toBe(true)
    expect(await x402.claim('42', 300)).toBe(false)
    expect(await mpp.claim('42', 300)).toBe(true)
  })

  it('integrates with verifyX402 across distributed isolates — regression: same nonce accepted on a second isolate', async () => {
    // Two KvNonceStore instances sharing one KV simulate two isolates
    const sharedKv = new StubKV()
    const isolateA = new KvNonceStore(sharedKv)
    const isolateB = new KvNonceStore(sharedKv)

    expect(await isolateA.claim('shared-nonce', 300)).toBe(true)
    // Isolate B sees the same nonce as used — no cross-isolate bypass.
    expect(await isolateB.claim('shared-nonce', 300)).toBe(false)
  })

  it('keeps same-owner claims idempotent when isolates race on the atomic insert', async () => {
    const sharedKv = new StubKV()
    const isolateA = new KvNonceStore(sharedKv)
    const isolateB = new KvNonceStore(sharedKv)
    const results = await Promise.all([
      isolateA.claim('shared-operation', 300, 'operation-1'),
      isolateB.claim('shared-operation', 300, 'operation-1'),
    ])
    expect(results).toEqual([true, true])
    expect(await isolateA.claim('shared-operation', 300, 'operation-2')).toBe(false)
  })

  it('fails closed when the KV binding cannot make an atomic claim', async () => {
    const backing = new StubKV()
    const cloudflareKv: NonceKV = {
      get: backing.get.bind(backing),
      put: backing.put.bind(backing),
      delete: backing.delete.bind(backing),
    }
    const store = new KvNonceStore(cloudflareKv)

    await expect(store.claim('legacy', 300)).rejects.toThrow('atomic putIfAbsent')
    await expect(store.claim('version-2', 300, 'operation-1')).rejects.toThrow('atomic putIfAbsent')
  })
})

describe('KvRateLimitStore', () => {
  it('returns empty array for unknown keys', async () => {
    const store = new KvRateLimitStore(new StubKV())
    expect(await store.get('ghost')).toEqual([])
  })

  it('round-trips timestamp arrays', async () => {
    const store = new KvRateLimitStore(new StubKV())
    const stamps = [1000, 2000, 3000]
    await store.set('alice', stamps, 120)
    expect(await store.get('alice')).toEqual(stamps)
  })

  it('rejects non-array JSON payloads — regression: malformed KV value must not crash the limiter', async () => {
    const kv = new StubKV()
    await kv.put('rl:bogus', JSON.stringify({ not: 'an-array' }), { expirationTtl: 300 })
    const store = new KvRateLimitStore(kv)
    expect(await store.get('bogus')).toEqual([])
  })

  it('rejects corrupt JSON — regression: KV bit-rot must degrade gracefully', async () => {
    const kv = new StubKV()
    await kv.put('rl:bogus', '{not json', { expirationTtl: 300 })
    const store = new KvRateLimitStore(kv)
    expect(await store.get('bogus')).toEqual([])
  })

  it('filters non-numeric entries — regression: defensive against mixed-type arrays', async () => {
    const kv = new StubKV()
    await kv.put('rl:mixed', JSON.stringify([1000, 'bad', 2000, null, 3000]), { expirationTtl: 300 })
    const store = new KvRateLimitStore(kv)
    expect(await store.get('mixed')).toEqual([1000, 2000, 3000])
  })

  it('enforces 60s minimum TTL', async () => {
    const kv = new StubKV()
    const putSpy = vi.spyOn(kv, 'put')
    const store = new KvRateLimitStore(kv)
    await store.set('alice', [Date.now()], 10)
    expect(putSpy).toHaveBeenCalledWith(expect.any(String), expect.any(String), { expirationTtl: 60 })
  })

  it('drives checkRateLimit end-to-end — regression: KV store must be plug-compatible with MemoryRateLimitStore', async () => {
    const kv = new StubKV()
    const store = new KvRateLimitStore(kv)
    const config = { limit: 2, windowSeconds: 60 }

    expect((await checkRateLimit('alice', config, store)).allowed).toBe(true)
    expect((await checkRateLimit('alice', config, store)).allowed).toBe(true)
    const blocked = await checkRateLimit('alice', config, store)
    expect(blocked.allowed).toBe(false)
    expect(blocked.retryAfterSeconds).toBeGreaterThan(0)
  })

  it('namespaces by prefix', async () => {
    const kv = new StubKV()
    const prod = new KvRateLimitStore(kv, 'prod-rl')
    const staging = new KvRateLimitStore(kv, 'staging-rl')
    await prod.set('alice', [1000], 120)
    expect(await staging.get('alice')).toEqual([])
  })
})
