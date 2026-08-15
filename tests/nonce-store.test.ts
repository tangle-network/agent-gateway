import { describe, it, expect, vi, afterEach } from 'vitest'
import { claimStoredNonce, KvNonceStore, MemoryNonceStore, nonceTtlSeconds, type NonceStore, type KVNamespace } from '../src/nonce-store'

describe('nonceTtlSeconds', () => {
  it('covers the complete signed validity window', () => {
    expect(nonceTtlSeconds(101n, 100)).toBe(60)
    expect(nonceTtlSeconds(7_300n, 100)).toBe(7_200)
    expect(nonceTtlSeconds(100n, 100)).toBeUndefined()
    expect(nonceTtlSeconds(BigInt(Number.MAX_SAFE_INTEGER) + 101n, 100)).toBeUndefined()
  })
})

describe('MemoryNonceStore', () => {
  afterEach(() => vi.useRealTimers())

  it('claims an unseen nonce — regression: false-positive rejection would break first-time payments', async () => {
    const store = new MemoryNonceStore()
    expect(await store.claim('nonce-never-recorded', 60)).toBe(true)
  })

  it('rejects a second claim — regression: missed replay detection lets attackers reuse signed payments', async () => {
    const store = new MemoryNonceStore()
    expect(await store.claim('replay-target', 60)).toBe(true)
    expect(await store.claim('replay-target', 60)).toBe(false)
  })

  it('evicts nonces after their TTL expires — regression: infinite retention causes unbounded memory growth', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryNonceStore()
    expect(await store.claim('short-lived', 60)).toBe(true)
    expect(await store.claim('short-lived', 60)).toBe(false)

    // Jump 61 seconds — past the TTL
    vi.advanceTimersByTime(61_000)
    expect(await store.claim('short-lived', 60)).toBe(true)
  })

  it('isolates nonce keys — regression: key collision across commitments would let Alice replay Bob\'s nonce', async () => {
    const store = new MemoryNonceStore()
    expect(await store.claim('0xAlice:42', 60)).toBe(true)
    expect(await store.claim('0xBob:42', 60)).toBe(true)
    expect(await store.claim('0xAlice:42', 60)).toBe(false)
  })

  it('background eviction removes expired entries — regression: map grows forever without cleanup', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryNonceStore()

    // Fill with short-lived nonces
    for (let i = 0; i < 100; i++) {
      expect(await store.claim(`n${i}`, 10)).toBe(true)
    }

    // Advance past TTL + past the 60s eviction throttle
    vi.advanceTimersByTime(61_000)

    // Recording a new nonce triggers eviction
    expect(await store.claim('trigger', 60)).toBe(true)

    // All old entries can be claimed again now.
    for (let i = 0; i < 100; i++) {
      expect(await store.claim(`n${i}`, 60)).toBe(true)
    }
  })
})

describe('claimStoredNonce', () => {
  it('fails closed for the 0.7.1 check-and-mark store contract', async () => {
    const seen = new Set<string>()
    const legacyStore: NonceStore = {
      hasSeen: async (nonce) => seen.has(nonce),
      markSeen: async (nonce) => { seen.add(nonce) },
    }

    await expect(claimStoredNonce(legacyStore, 'legacy', 60))
      .rejects.toThrow('atomic payment replay protection')
  })

  it('fails closed for a store without either replay contract', async () => {
    const legacyStore = {
      hasSeen: async () => false,
    } as unknown as NonceStore

    await expect(claimStoredNonce(legacyStore, 'legacy', 60)).rejects.toThrow('atomic payment replay protection')
  })
})

describe('KvNonceStore', () => {
  it('allows only one mixed legacy and version 2 claim', async () => {
    const values = new Map<string, string>()
    const kv: KVNamespace = {
      async get(key) {
        return values.get(key) ?? null
      },
      async put(key, value) {
        values.set(key, value)
      },
      async putIfAbsent(key, value) {
        if (values.has(key)) return false
        values.set(key, value)
        return true
      },
      async delete(key) {
        values.delete(key)
      },
    }
    const store = new KvNonceStore(kv)

    const results = await Promise.all([
      store.claim('mixed-version', 60),
      store.claim('mixed-version', 60, 'operation-1'),
    ])

    expect(results.filter(Boolean)).toHaveLength(1)
    expect(await store.claim('mixed-version', 60, 'operation-2')).toBe(false)
  })
})
