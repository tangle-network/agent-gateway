import { describe, it, expect, vi, afterEach } from 'vitest'
import { KvNonceStore, MemoryNonceStore, nonceTtlSeconds, type KVNamespace } from '../src/nonce-store'

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

  it('returns false for unseen nonces — regression: false-positive rejection would break first-time payments', async () => {
    const store = new MemoryNonceStore()
    expect(await store.hasSeen('nonce-never-recorded')).toBe(false)
  })

  it('returns true after markSeen — regression: missed replay detection lets attackers reuse signed payments', async () => {
    const store = new MemoryNonceStore()
    await store.markSeen('replay-target', 60)
    expect(await store.hasSeen('replay-target')).toBe(true)
  })

  it('evicts nonces after their TTL expires — regression: infinite retention causes unbounded memory growth', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryNonceStore()
    await store.markSeen('short-lived', 60)
    expect(await store.hasSeen('short-lived')).toBe(true)

    // Jump 61 seconds — past the TTL
    vi.advanceTimersByTime(61_000)
    expect(await store.hasSeen('short-lived')).toBe(false)
  })

  it('isolates nonce keys — regression: key collision across commitments would let Alice replay Bob\'s nonce', async () => {
    const store = new MemoryNonceStore()
    await store.markSeen('0xAlice:42', 60)
    expect(await store.hasSeen('0xBob:42')).toBe(false)
    expect(await store.hasSeen('0xAlice:42')).toBe(true)
  })

  it('background eviction removes expired entries — regression: map grows forever without cleanup', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryNonceStore()

    // Fill with short-lived nonces
    for (let i = 0; i < 100; i++) {
      await store.markSeen(`n${i}`, 10)
    }

    // Advance past TTL + past the 60s eviction throttle
    vi.advanceTimersByTime(61_000)

    // Recording a new nonce triggers eviction
    await store.markSeen('trigger', 60)

    // All old entries should report unseen now
    for (let i = 0; i < 100; i++) {
      expect(await store.hasSeen(`n${i}`)).toBe(false)
    }
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
