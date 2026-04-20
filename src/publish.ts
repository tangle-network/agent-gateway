/**
 * Publishing routes — let agent owners publish/unpublish their workspaces
 * as paid API endpoints.
 */

import { Hono } from 'hono'

// --- Types ---

export interface PublishedConfig {
  enabled: boolean
  slug: string
  pricePerTokenUsd: number
  platformFeePercent: number
  /** Remote operator endpoint for sovereignty mode */
  sandboxEndpoint?: string | null
  remoteSandboxId?: string | null
  remoteBearerToken?: string | null
  publishedAt: string
}

export interface PublishRequest {
  slug?: string
  pricePerTokenUsd?: number
  platformFeePercent?: number
  sandboxEndpoint?: string | null
  remoteSandboxId?: string | null
  remoteBearerToken?: string | null
}

/** Each agent implements this against their workspace/session model */
export interface PublishStore {
  /** Get current published config for a workspace/session */
  getPublishedConfig(ownerId: string, resourceId: string): Promise<PublishedConfig | null>
  /** Set published config */
  setPublishedConfig(ownerId: string, resourceId: string, config: PublishedConfig): Promise<void>
  /** Clear published config (unpublish) */
  clearPublishedConfig(ownerId: string, resourceId: string): Promise<void>
  /** Check the resource exists and the user owns it */
  verifyOwnership(ownerId: string, resourceId: string): Promise<boolean>
}

// --- Routes ---

export interface PublishRoutesConfig {
  store: PublishStore
  getAuthUserId: (request: Request) => Promise<string | null>
  /** Base URL for gateway endpoint display (e.g. "https://gtm.tangle.tools") */
  baseUrl?: string
}

export function createPublishRoutes(config: PublishRoutesConfig) {
  const router = new Hono()

  // Get publish status
  router.get('/:resourceId/publish', async (c) => {
    const userId = await config.getAuthUserId(c.req.raw)
    if (!userId) return c.json({ error: 'Unauthorized' }, 401)

    const resourceId = c.req.param('resourceId')
    const owns = await config.store.verifyOwnership(userId, resourceId)
    if (!owns) return c.json({ error: 'Not found' }, 404)

    const published = await config.store.getPublishedConfig(userId, resourceId)
    return c.json({ published })
  })

  // Publish
  router.post('/:resourceId/publish', async (c) => {
    const userId = await config.getAuthUserId(c.req.raw)
    if (!userId) return c.json({ error: 'Unauthorized' }, 401)

    const resourceId = c.req.param('resourceId')
    const owns = await config.store.verifyOwnership(userId, resourceId)
    if (!owns) return c.json({ error: 'Not found' }, 404)

    const body = await c.req.json<PublishRequest>()
    const slug = body.slug ?? resourceId

    const publishedConfig: PublishedConfig = {
      enabled: true,
      slug,
      pricePerTokenUsd: body.pricePerTokenUsd ?? 0.00002,
      platformFeePercent: body.platformFeePercent ?? 0.20,
      sandboxEndpoint: body.sandboxEndpoint ?? null,
      remoteSandboxId: body.remoteSandboxId ?? null,
      remoteBearerToken: body.remoteBearerToken ?? null,
      publishedAt: new Date().toISOString(),
    }

    await config.store.setPublishedConfig(userId, resourceId, publishedConfig)

    const base = config.baseUrl ?? ''
    return c.json({
      success: true,
      published: publishedConfig,
      gatewayUrl: `${base}/v1/agents/${slug}/chat/completions`,
      discoveryUrl: `${base}/v1/agents/${slug}/chat/completions`,
    })
  })

  // Unpublish
  router.post('/:resourceId/unpublish', async (c) => {
    const userId = await config.getAuthUserId(c.req.raw)
    if (!userId) return c.json({ error: 'Unauthorized' }, 401)

    const resourceId = c.req.param('resourceId')
    const owns = await config.store.verifyOwnership(userId, resourceId)
    if (!owns) return c.json({ error: 'Not found' }, 404)

    await config.store.clearPublishedConfig(userId, resourceId)
    return c.json({ success: true, published: null })
  })

  return router
}
