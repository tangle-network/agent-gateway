import { Hono } from 'hono'

import { createA2AHandlers } from './a2a/handler'
import { InMemoryTaskStore } from './a2a/task-store'
import {
  type AuthorizedRequest,
  type GatewayState,
  authenticateAndGuard,
  claimPayment,
  dispatchSandboxStreamRich,
  releasePayment,
  releasePaymentAfterFailure,
  settleAndRecord,
} from './dispatch'
import { MemoryNonceStore } from './nonce-store'
import { type GatewayObserver, type RequestContext, generateRequestId } from './observer'
import { MemoryRateLimitStore, type RateLimitStore } from './rate-limit'
import type { ChatCompletionChunk, ChatCompletionRequest, GatewayConfig } from './types'
import { isApiKeyAuthEnabled, isMppAuthEnabled } from './verify'

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
  const maxOutputTokens = config.maxOutputTokens ?? 4096
  const defaultOutputTokens = config.defaultOutputTokens ?? 1024
  if (!Number.isSafeInteger(maxOutputTokens) || maxOutputTokens <= 0) {
    throw new Error('createAgentGateway: maxOutputTokens must be a positive integer')
  }
  if (
    !Number.isSafeInteger(defaultOutputTokens) ||
    defaultOutputTokens <= 0 ||
    defaultOutputTokens > maxOutputTokens
  ) {
    throw new Error(
      'createAgentGateway: defaultOutputTokens must be a positive integer no greater than maxOutputTokens',
    )
  }
  if (
    config.x402.currencyDecimals !== undefined &&
    (!Number.isInteger(config.x402.currencyDecimals) ||
      config.x402.currencyDecimals < 0 ||
      config.x402.currencyDecimals > 18)
  ) {
    throw new Error('createAgentGateway: x402.currencyDecimals must be an integer between 0 and 18')
  }
  const executionBudget = config.executionBudget
  for (const [name, value] of [
    ['maxReasoningTokens', executionBudget?.maxReasoningTokens ?? maxOutputTokens],
    ['maxToolTokens', executionBudget?.maxToolTokens ?? maxOutputTokens],
    ['maxToolCalls', executionBudget?.maxToolCalls ?? 8],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`createAgentGateway: executionBudget.${name} must be a non-negative safe integer`)
    }
  }
  if (
    executionBudget?.maxProviderCostUsd !== undefined &&
    (!Number.isFinite(executionBudget.maxProviderCostUsd) || executionBudget.maxProviderCostUsd < 0)
  ) {
    throw new Error('createAgentGateway: executionBudget.maxProviderCostUsd must be finite and non-negative')
  }
  if (config.x402.paymentOperations && config.x402.paymentProtocolVersion === undefined) {
    throw new Error('createAgentGateway: paymentProtocolVersion must be explicit when durable payment operations are configured')
  }
  if (config.x402.paymentProtocolVersion === 2 &&
      (!config.x402.paymentOperations || config.x402.paymentOperations.protocolVersion !== 2)) {
    throw new Error('createAgentGateway: payment protocol version 2 requires durable payment operations')
  }
  if (config.x402.paymentProtocolVersion === 1 && config.x402.paymentOperations) {
    throw new Error('createAgentGateway: version 1 cannot be combined with version 2 payment operations')
  }
  const gw = new Hono()
  const rateLimitStore: RateLimitStore = config.rateLimitStore ?? new MemoryRateLimitStore()
  const state: GatewayState = {
    rateLimitStore,
    nonceStore: config.nonceStore ?? new MemoryNonceStore(),
    globalRateLimit: config.rateLimit ?? { limit: 60, windowSeconds: 60 },
    requiredScope: config.requiredScope ?? 'chat',
    maxLen: config.maxMessageLength ?? 8000,
    maxOutputTokens,
    defaultOutputTokens,
    maxReasoningTokens: config.executionBudget?.maxReasoningTokens ?? maxOutputTokens,
    maxToolTokens: config.executionBudget?.maxToolTokens ?? maxOutputTokens,
    maxToolCalls: config.executionBudget?.maxToolCalls ?? 8,
    maxProviderCostUsd: config.executionBudget?.maxProviderCostUsd,
    obs: config.observer,
  }
  const obs: GatewayObserver | undefined = state.obs

  // --- Discovery endpoint (no auth) ---

  gw.get('/:slug/chat/completions', async (c) => {
    const slug = c.req.param('slug')
    const agent = await config.resolveAgent(slug)
    if (!agent || !agent.enabled) return c.json({ error: 'Agent not found or not published' }, 404)

    const paymentMethods: Array<Record<string, unknown>> = [
      {
        type: 'x402',
        operator: config.x402.operatorAddress,
        chain_id: config.x402.chainId,
        credits_contract: config.x402.creditsAddress,
      },
    ]
    if (isMppAuthEnabled(config)) {
      paymentMethods.push({
        type: 'mpp',
        realm: config.mpp!.realm,
        method: config.mpp!.method ?? 'blueprintevm',
      })
    }
    if (isApiKeyAuthEnabled(config)) paymentMethods.push({ type: 'api_key', prefix: 'sk_agent_' })

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

    const guard = await authenticateAndGuard(
      c,
      slug,
      body.messages,
      config,
      state,
      body.max_tokens,
    )
    if (guard instanceof Response) return guard
    const authz = guard
    try {
      await claimPayment(authz, config, state)
    } catch {
      try {
        await releasePayment(authz, config, 'payment authorization failed')
      } catch (releaseError) {
        console.error(
          `[agent-gateway] payment release failed for ${authz.requestId}:`,
          releaseError instanceof Error ? releaseError.message : String(releaseError),
        )
      }
      await obs?.onAuthFailure?.(
        {
          requestId: authz.requestId,
          agentSlug: authz.agent.slug,
          startMs: authz.startMs,
        },
        { method: authz.paymentMethod, code: 'payment_authorization_failed', httpStatus: 402 },
      )
      return c.json(
        {
          error: {
            message: 'Payment authorization failed',
            type: 'payment_required',
            code: 'payment_authorization_failed',
          },
        },
        { status: 402, headers: { 'X-Payment-Required': 'spendauth', 'X-Request-Id': authz.requestId } },
      )
    }

    return streamChatCompletions(c, authz, config, obs)
  })

  // --- A2A protocol surface (Google Agent-to-Agent, JSON-RPC 2.0 + AgentCard) ---
  // Mounted alongside the OpenAI-compat routes so a single agent speaks both.
  // Both surfaces share authenticateAndGuard + dispatchSandboxStream +
  // settleAndRecord, so every security and billing guarantee applies uniformly
  // regardless of which protocol the caller used.
  const taskStore = config.a2a?.taskStore ?? new InMemoryTaskStore()
  const pushStore = config.a2a?.pushStore
  const a2a = createA2AHandlers({ config, state, taskStore, pushStore })
  gw.get('/:slug/.well-known/agent.json', a2a.handleAgentCard)
  gw.post('/:slug', a2a.handleJsonRpc)

  return gw
}

// Consumers that bind the gateway through a package boundary can fail closed
// when an old binary ignores the version 2 operation contract.
Object.assign(createAgentGateway, { paymentProtocolVersion: 2 as const })

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
  const {
    agent,
    consumerId,
    paymentMethod,
    requestId,
    userMessage,
    rateLimitRemaining,
    maxOutputTokens,
  } = authz
  let outputText = ''
  let usage: import('./types').SandboxUsageReceipt | undefined
  let workObserved = false
  const ctx: RequestContext = {
    requestId,
    agentSlug: agent.slug,
    startMs: authz.startMs,
  }

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder()
      const sendChunk = (delta: string) => {
        outputText += delta
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
        for await (const event of dispatchSandboxStreamRich(
          agent,
          userMessage,
          consumerId,
          config,
          undefined,
          undefined,
          maxOutputTokens,
        )) {
          if (event.kind === 'text') {
            sendChunk(event.delta)
            workObserved = true
          }
          if (event.kind === 'activity') workObserved = true
          if (event.kind === 'usage') usage = event.usage
        }

        if (!usage) throw new Error('sandbox did not provide a usage receipt')

        const done: ChatCompletionChunk = {
          id: `chatcmpl-${Date.now()}`,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: agent.slug,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        }
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(done)}\n\n`))
        controller.enqueue(encoder.encode('data: [DONE]\n\n'))

        await settleAndRecord(
          agent,
          authz,
          usage,
          config,
          obs,
        )
      } catch (err) {
        const rawMessage = err instanceof Error ? err.message : String(err)
        // Never expose stack traces / absolute paths from sandbox internals.
        const safeMessage =
          rawMessage.includes('/') || rawMessage.includes('\\')
            ? 'Internal agent error'
            : rawMessage
        try {
          await obs?.onStreamError?.(ctx, { consumerId, errorMessage: rawMessage })
        } catch (observerError) {
          console.error(
            `[agent-gateway] stream observer failed for ${requestId}:`,
            observerError instanceof Error ? observerError.message : String(observerError),
          )
        }
        try {
          await releasePaymentAfterFailure(authz, config, rawMessage, workObserved || usage !== undefined)
        } catch (releaseError) {
          console.error(
            `[agent-gateway] payment release failed for ${authz.requestId}:`,
            releaseError instanceof Error ? releaseError.message : String(releaseError),
          )
        }
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
