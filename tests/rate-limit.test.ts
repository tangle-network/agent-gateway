import { describe, it, expect, vi, afterEach } from 'vitest'
import { MemoryRateLimitStore, checkRateLimit } from '../src/rate-limit'

describe('checkRateLimit (sliding window)', () => {
  afterEach(() => vi.useRealTimers())

  it('allows requests up to the limit — regression: premature rejection blocks paying users', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryRateLimitStore()
    const config = { limit: 3, windowSeconds: 60 }

    for (let i = 0; i < 3; i++) {
      const result = await checkRateLimit('alice', config, store)
      expect(result.allowed).toBe(true)
      expect(result.remaining).toBe(2 - i)
    }
  })

  it('blocks on the (limit+1)th request — regression: missing rate-limit enforcement enables DoS', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryRateLimitStore()
    const config = { limit: 3, windowSeconds: 60 }

    for (let i = 0; i < 3; i++) await checkRateLimit('alice', config, store)

    const blocked = await checkRateLimit('alice', config, store)
    expect(blocked.allowed).toBe(false)
    expect(blocked.remaining).toBe(0)
    expect(blocked.retryAfterSeconds).toBeGreaterThan(0)
    expect(blocked.retryAfterSeconds).toBeLessThanOrEqual(60)
  })

  it('resets after the window slides — regression: permanent block after burst would lock users out', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryRateLimitStore()
    const config = { limit: 2, windowSeconds: 60 }

    await checkRateLimit('alice', config, store)
    await checkRateLimit('alice', config, store)
    expect((await checkRateLimit('alice', config, store)).allowed).toBe(false)

    // Advance past the window
    vi.advanceTimersByTime(61_000)

    const after = await checkRateLimit('alice', config, store)
    expect(after.allowed).toBe(true)
    expect(after.remaining).toBe(1)
  })

  it('isolates limits per consumer — regression: shared key would let Alice DoS Bob', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryRateLimitStore()
    const config = { limit: 1, windowSeconds: 60 }

    expect((await checkRateLimit('alice', config, store)).allowed).toBe(true)
    expect((await checkRateLimit('alice', config, store)).allowed).toBe(false)
    // Bob should still be fresh
    expect((await checkRateLimit('bob', config, store)).allowed).toBe(true)
  })

  it('retryAfter reflects oldest-in-window timing — regression: stale advice leaves clients hammering', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryRateLimitStore()
    const config = { limit: 1, windowSeconds: 60 }

    await checkRateLimit('alice', config, store)
    vi.advanceTimersByTime(20_000) // 20s elapsed
    const blocked = await checkRateLimit('alice', config, store)

    expect(blocked.allowed).toBe(false)
    // Original request becomes available ~60s later → ~40s retryAfter
    expect(blocked.retryAfterSeconds).toBeGreaterThanOrEqual(39)
    expect(blocked.retryAfterSeconds).toBeLessThanOrEqual(41)
  })
})

describe('MemoryRateLimitStore', () => {
  afterEach(() => vi.useRealTimers())

  it('returns empty for unknown keys', async () => {
    const store = new MemoryRateLimitStore()
    expect(await store.get('unseen')).toEqual([])
  })

  it('round-trips timestamps', async () => {
    const store = new MemoryRateLimitStore()
    const stamps = [1000, 2000, 3000]
    await store.set('alice', stamps, 60)
    expect(await store.get('alice')).toEqual(stamps)
  })

  it('expires keys after TTL — regression: stale data causes incorrect rate limiting across restarts', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    const store = new MemoryRateLimitStore()
    await store.set('alice', [Date.now()], 5)
    expect((await store.get('alice')).length).toBe(1)

    vi.advanceTimersByTime(6_000)
    expect(await store.get('alice')).toEqual([])
  })
})
