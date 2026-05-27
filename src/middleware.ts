import { Hono } from 'hono'

import { createA2AHandlers } from './a2a/handler'
import { InMemoryTaskStore } from './a2a/task-store'
import {
  type AuthorizedRequest,
  type GatewayState,
  authenticateAndGuard,
  dispatchSandboxStream,
  estimateTokens,
  settleAndRecord,
} from './dispatch'
import { MemoryNonceStore } from './nonce-store'
import { type GatewayObserver, type RequestContext, generateRequestId } from './observer'
import { MemoryRateLimitStore, type RateLimitStore } from './rate-limit'
import type { ChatCompletionChunk, ChatCompletionRequest, GatewayConfig } from './types'

/**
 * Create a Hono router that serves the agent gateway.
 *
 * Mount at any path:
 *   app.route('/v1/agents', createAgentGateway(config))
 *
 * Exposes:
 *   GET  /:slug/chat/completions  — agent discovery metadata (Tangle-native shape)
 *   POST /:slug/chat/completions  — OpenAI-compatible chat endpoint (paid)
 */
export function createAgentGateway(config: GatewayConfig) {
  // Production gateways must verify x402 signatures. Tests and local
  // dev can opt into the explicit demo path.
  if (!config.x402.verifySigner && !config.x402.demoMode) {
    throw new Error(
      'createAgentGateway: x402.verifySigner is required in production. ' +
        'For tests, set x402.demoMode: true explicitly.',
    )
  }
  const gw = new Hono()
  const rateLimitStore: RateLimitStore = config.rateLimitStore ?? new MemoryRateLimitStore()
  const state: GatewayState = {
    rateLimitStore,
    nonceStore: config.nonceStore ?? new MemoryNonceStore(),
    globalRateLimit: config.rateLimit ?? { limit: 60, windowSeconds: 60 },
    requiredScope: config.requiredScope ?? 'chat',
    maxLen: config.maxMessageLength ?? 8000,
    obs: config.observer,
  }
  const obs: GatewayObserver | undefined = state.obs

  // --- Discovery endpoint (no auth) ---

  gw.get('/:slug/chat/completions', async (c) => {
    const slug = c.req.param('slug')
    const agent = await config.resolveAgent(slug)
    if (!agent) return c.json({ error: 'Agent not found or not published' }, 404)

    const paymentMethods: Array<Record<string, unknown>> = [
      {
        type: 'x402',
        operator: config.x402.operatorAddress,
        chain_id: config.x402.chainId,
        credits_contract: config.x402.creditsAddress,
      },
    ]
    if (config.mpp) {
      paymentMethods.push({
        type: 'mpp',
        realm: config.mpp.realm,
        method: config.mpp.method ?? 'blueprintevm',
      })
    }
    paymentMethods.push({ type: 'api_key', prefix: 'sk_agent_' })

    return c.json({
      slug: agent.slug,
      pricing: {
        per_token_usd: agent.pricePerTokenUsd,
        currency: 'USD',
        platform_fee_percent: agent.platformFeePercent,
      },
      hosting: {
        mode: agent.sandboxEndpoint ? 'sovereign' : 'centralized',
        endpoint: agent.sandboxEndpoint ?? config.baseUrl ?? 'tangle.tools',
      },
      payment_methods: paymentMethods,
      capabilities: ['chat.completions', 'streaming'],
      openai_compatible: true,
    })
  })

  // --- Chat completions endpoint (paid) ---

  gw.post('/:slug/chat/completions', async (c) => {
    const slug = c.req.param('slug')

    // Body size limit (before parsing — DoS prevention).
    const contentLength = Number.parseInt(c.req.header('Content-Length') ?? '0', 10)
    if (contentLength > 65536) {
      const requestId = generateRequestId()
      await obs?.onBodyTooLarge?.(
        { requestId, agentSlug: slug, startMs: Date.now() },
        contentLength,
      )
      return c.json(
        {
          error: {
            message: 'Request body too large (max 64KB)',
            type: 'invalid_request',
          },
        },
        413,
      )
    }

    let body: ChatCompletionRequest
    try {
      body = await c.req.json()
    } catch {
      return c.json({ error: { message: 'Invalid JSON', type: 'invalid_request' } }, 400)
    }
    if (!body.messages?.length) {
      return c.json(
        { error: { message: 'messages array required', type: 'invalid_request' } },
        400,
      )
    }

    const guard = await authenticateAndGuard(c, slug, body.messages, config, state)
    if (guard instanceof Response) return guard
    const authz = guard

    return streamChatCompletions(c, authz, config, obs)
  })

  // --- A2A protocol surface (Google Agent-to-Agent, JSON-RPC 2.0 + AgentCard) ---
  // Mounted alongside the OpenAI-compat routes so a single agent speaks both.
  // Both surfaces share authenticateAndGuard + dispatchSandboxStream +
  // settleAndRecord, so every security and billing guarantee applies uniformly
  // regardless of which protocol the caller used.
  const taskStore = config.a2a?.taskStore ?? new InMemoryTaskStore()
  const a2a = createA2AHandlers({ config, state, taskStore })
  gw.get('/:slug/.well-known/agent.json', a2a.handleAgentCard)
  gw.post('/:slug', a2a.handleJsonRpc)

  return gw
}

/**
 * Drain the sandbox stream into an OpenAI-shaped SSE response, settle the
 * payment, fire observer hooks. Identical pre-refactor behavior, just lifted
 * out of the handler so the A2A wrapper can reach the same dispatch path
 * without duplicating it.
 */
function streamChatCompletions(
  c: import('hono').Context,
  authz: AuthorizedRequest,
  config: GatewayConfig,
  obs: GatewayObserver | undefined,
): Response {
  const { agent, consumerId, paymentMethod, requestId, userMessage, rateLimitRemaining } = authz
  const inputTokens = estimateTokens(userMessage)
  let outputTokens = 0
  const ctx: RequestContext = {
    requestId,
    agentSlug: agent.slug,
    startMs: authz.startMs,
  }

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder()
      const sendChunk = (delta: string) => {
        outputTokens += estimateTokens(delta)
        const chunk: ChatCompletionChunk = {
          id: `chatcmpl-${Date.now()}`,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: agent.slug,
          choices: [{ index: 0, delta: { content: delta }, finish_reason: null }],
        }
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`))
      }

      try {
        for await (const delta of dispatchSandboxStream(agent, userMessage, consumerId, config)) {
          sendChunk(delta)
        }

        const done: ChatCompletionChunk = {
          id: `chatcmpl-${Date.now()}`,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: agent.slug,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        }
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(done)}\n\n`))
        controller.enqueue(encoder.encode('data: [DONE]\n\n'))

        await settleAndRecord(agent, authz, inputTokens, outputTokens, config, obs)
      } catch (err) {
        const rawMessage = err instanceof Error ? err.message : String(err)
        // Never expose stack traces / absolute paths from sandbox internals.
        const safeMessage =
          rawMessage.includes('/') || rawMessage.includes('\\')
            ? 'Internal agent error'
            : rawMessage
        await obs?.onStreamError?.(ctx, { consumerId, errorMessage: rawMessage })
        controller.enqueue(
          encoder.encode(
            `data: ${JSON.stringify({ error: { message: safeMessage, type: 'server_error' } })}\n\n`,
          ),
        )
      } finally {
        controller.close()
      }
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Request-Id': requestId,
      'X-Agent-Slug': agent.slug,
      'X-Agent-Hosting': agent.sandboxEndpoint ? 'sovereign' : 'centralized',
      'X-Payment-Method': paymentMethod,
      'X-Payment-Settled': paymentMethod === 'x402' ? 'pending' : 'true',
      ...(rateLimitRemaining !== undefined
        ? { 'X-RateLimit-Remaining': String(rateLimitRemaining) }
        : {}),
    },
  })
}

