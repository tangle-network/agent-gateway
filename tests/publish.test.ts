import { describe, it, expect, beforeEach } from 'vitest'
import { Hono } from 'hono'
import {
  createPublishRoutes,
  type PublishStore,
  type PublishedConfig,
} from '../src/publish'

class MemoryPublishStore implements PublishStore {
  private configs = new Map<string, PublishedConfig>() // key = ownerId:resourceId
  private owned = new Map<string, string>() // resourceId → ownerId

  register(resourceId: string, ownerId: string) { this.owned.set(resourceId, ownerId) }

  async getPublishedConfig(ownerId: string, resourceId: string) {
    return this.configs.get(`${ownerId}:${resourceId}`) ?? null
  }
  async setPublishedConfig(ownerId: string, resourceId: string, config: PublishedConfig) {
    this.configs.set(`${ownerId}:${resourceId}`, config)
  }
  async clearPublishedConfig(ownerId: string, resourceId: string) {
    this.configs.delete(`${ownerId}:${resourceId}`)
  }
  async verifyOwnership(ownerId: string, resourceId: string) {
    return this.owned.get(resourceId) === ownerId
  }
}

function buildApp(store: MemoryPublishStore) {
  const app = new Hono()
  app.route('/agents', createPublishRoutes({
    store,
    getAuthUserId: async (req) => req.headers.get('X-User'),
    baseUrl: 'https://test.tangle.tools',
  }))
  return app
}

describe('createPublishRoutes', () => {
  let store: MemoryPublishStore
  let app: Hono

  beforeEach(() => {
    store = new MemoryPublishStore()
    store.register('agent-1', 'user_alice')
    app = buildApp(store)
  })

  describe('GET /:resourceId/publish', () => {
    it('returns 401 unauthenticated — regression: publish status must not be publicly readable', async () => {
      const res = await app.request('/agents/agent-1/publish')
      expect(res.status).toBe(401)
    })

    it('returns 404 when the user does not own the agent — regression: IDOR leaking publish config', async () => {
      const res = await app.request('/agents/agent-1/publish', { headers: { 'X-User': 'user_bob' } })
      expect(res.status).toBe(404)
    })

    it('returns null when never published', async () => {
      const res = await app.request('/agents/agent-1/publish', { headers: { 'X-User': 'user_alice' } })
      expect(res.status).toBe(200)
      expect(await res.json()).toEqual({ published: null })
    })
  })

  describe('POST /:resourceId/publish', () => {
    it('publishes with explicit config + returns gateway URL', async () => {
      const res = await app.request('/agents/agent-1/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-User': 'user_alice' },
        body: JSON.stringify({
          slug: 'alice-agent',
          pricePerTokenUsd: 0.00005,
          platformFeePercent: 0.15,
        }),
      })
      expect(res.status).toBe(200)
      const body = await res.json() as {
        success: boolean
        published: PublishedConfig
        gatewayUrl: string
        discoveryUrl: string
      }
      expect(body.success).toBe(true)
      expect(body.published.slug).toBe('alice-agent')
      expect(body.published.pricePerTokenUsd).toBe(0.00005)
      expect(body.published.platformFeePercent).toBe(0.15)
      expect(body.published.enabled).toBe(true)
      expect(body.gatewayUrl).toBe('https://test.tangle.tools/v1/agents/alice-agent/chat/completions')
    })

    it('falls back to resourceId when no slug provided', async () => {
      const res = await app.request('/agents/agent-1/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-User': 'user_alice' },
        body: JSON.stringify({}),
      })
      const body = await res.json() as { published: { slug: string } }
      expect(body.published.slug).toBe('agent-1')
    })

    it('applies default pricing when unspecified — regression: zero-price default would let agents serve for free', async () => {
      const res = await app.request('/agents/agent-1/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-User': 'user_alice' },
        body: JSON.stringify({}),
      })
      const body = await res.json() as { published: PublishedConfig }
      expect(body.published.pricePerTokenUsd).toBe(0.00002)
      expect(body.published.platformFeePercent).toBe(0.20)
    })

    it('refuses to publish another user\'s agent — regression: IDOR on publish', async () => {
      const res = await app.request('/agents/agent-1/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-User': 'user_bob' },
        body: JSON.stringify({ slug: 'hijack' }),
      })
      expect(res.status).toBe(404)
    })
  })

  describe('POST /:resourceId/unpublish', () => {
    it('clears the published config', async () => {
      // Publish first
      await app.request('/agents/agent-1/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-User': 'user_alice' },
        body: JSON.stringify({}),
      })
      expect((await store.getPublishedConfig('user_alice', 'agent-1'))?.enabled).toBe(true)

      // Unpublish
      const res = await app.request('/agents/agent-1/unpublish', {
        method: 'POST',
        headers: { 'X-User': 'user_alice' },
      })
      expect(res.status).toBe(200)
      expect(await store.getPublishedConfig('user_alice', 'agent-1')).toBeNull()
    })

    it('refuses to unpublish another user\'s agent — regression: hostile takedown', async () => {
      await app.request('/agents/agent-1/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-User': 'user_alice' },
        body: JSON.stringify({}),
      })

      const res = await app.request('/agents/agent-1/unpublish', {
        method: 'POST',
        headers: { 'X-User': 'user_bob' },
      })
      expect(res.status).toBe(404)
      expect((await store.getPublishedConfig('user_alice', 'agent-1'))?.enabled).toBe(true)
    })
  })
})
