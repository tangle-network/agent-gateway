import { Hono } from 'hono'
import type {
  GatewayConfig,
  ChatCompletionRequest,
  ChatCompletionChunk,
  PaymentMethod,
  ApiKeyInfo,
} from './types'
import { verifyX402, verifyMpp, defaultVerifyApiKey } from './verify'
import { filterConsumerMessagesStrict, redactSystemPromptFromOutput } from './filter'
import { checkRateLimit, MemoryRateLimitStore, type RateLimitStore } from './rate-limit'
import { MemoryNonceStore } from './nonce-store'
import { generateRequestId, type GatewayObserver, type RequestContext } from './observer'

/**
 * Create a Hono router that serves the agent gateway.
 *
 * Mount at any path:
 *   app.route('/v1/agents', createAgentGateway(config))
 *
 * Exposes:
 *   GET  /:slug/chat/completions  — agent discovery metadata
 *   POST /:slug/chat/completions  — OpenAI-compatible chat endpoint (paid)
 */
export function createAgentGateway(config: GatewayConfig) {
  const gw = new Hono()
  const maxLen = config.maxMessageLength ?? 8000
  const rateLimitStore: RateLimitStore = config.rateLimitStore ?? new MemoryRateLimitStore()
  const globalRateLimit = config.rateLimit ?? { limit: 60, windowSeconds: 60 }
  const nonceStore = config.nonceStore ?? new MemoryNonceStore()
  const requiredScope = config.requiredScope ?? 'chat'
  const obs: GatewayObserver | undefined = config.observer

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
    const startMs = Date.now()
    const requestId = generateRequestId()
    const ctx: RequestContext = { requestId, agentSlug: slug, startMs }

    await obs?.onRequestStart?.(ctx)

    // 1. Resolve agent
    const agent = await config.resolveAgent(slug)
    if (!agent) {
      return c.json({ error: { message: 'Agent not found', type: 'not_found' } }, 404)
    }

    // 2. Body size limit (before parsing — DoS prevention)
    const contentLength = parseInt(c.req.header('Content-Length') ?? '0', 10)
    if (contentLength > 65536) {
      await obs?.onBodyTooLarge?.(ctx, contentLength)
      return c.json(
        { error: { message: 'Request body too large (max 64KB)', type: 'invalid_request' } },
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
      return c.json({ error: { message: 'messages array required', type: 'invalid_request' } }, 400)
    }

    // 3. Authenticate — x402 SpendAuth, MPP, or API key
    const spendAuthHeader = c.req.header('X-Payment-Signature')
    const authHeader = c.req.header('Authorization') ?? ''
    let consumerId: string | null = null
    let paymentMethod: PaymentMethod = 'none'
    let keyInfo: ApiKeyInfo | null = null

    if (spendAuthHeader) {
      const signer = await verifyX402(spendAuthHeader, config.x402, nonceStore)
      if (!signer) {
        await obs?.onAuthFailure?.(ctx, { method: 'x402', code: 'invalid_spend_auth', httpStatus: 402 })
        return c.json(
          { error: { message: 'Invalid X-Payment-Signature', type: 'authentication_error', code: 'invalid_spend_auth' } },
          { status: 402, headers: { 'X-Payment-Required': 'spendauth', 'X-Request-Id': requestId } },
        )
      }
      consumerId = signer
      paymentMethod = 'x402'
    } else if (config.mpp && authHeader.toLowerCase().startsWith('payment ')) {
      const signer = await verifyMpp(authHeader, config.mpp, config.x402)
      if (!signer) {
        const realm = config.mpp.realm
        const method = config.mpp.method ?? 'blueprintevm'
        await obs?.onAuthFailure?.(ctx, { method: 'mpp', code: 'invalid_mpp_credential', httpStatus: 401 })
        return c.json(
          { error: { message: 'Invalid Payment credential', type: 'authentication_error', code: 'invalid_mpp_credential' } },
          { status: 401, headers: { 'WWW-Authenticate': `Payment realm="${realm}", method="${method}"`, 'X-Request-Id': requestId } },
        )
      }
      consumerId = signer
      paymentMethod = 'mpp'
    } else if (authHeader.startsWith('Bearer ')) {
      const verify = config.verifyApiKey ?? defaultVerifyApiKey
      const key = await verify(authHeader)
      if (!key) {
        await obs?.onAuthFailure?.(ctx, { method: 'apikey', code: 'invalid_api_key', httpStatus: 401 })
        return c.json(
          { error: { message: 'Invalid API key', type: 'authentication_error' } },
          { status: 401, headers: { 'X-Request-Id': requestId } },
        )
      }

      // Scope enforcement — API key must include the required scope
      if (key.scopes && key.scopes.length > 0 && !key.scopes.includes(requiredScope)) {
        await obs?.onAuthFailure?.(ctx, { method: 'apikey', code: 'insufficient_scope', httpStatus: 403 })
        return c.json(
          { error: { message: `API key missing required scope: ${requiredScope}`, type: 'forbidden', code: 'insufficient_scope' } },
          { status: 403, headers: { 'X-Request-Id': requestId } },
        )
      }

      consumerId = key.consumerId
      paymentMethod = 'apikey'
      keyInfo = key
    } else {
      // No payment — return 402 with instructions
      await obs?.onAuthFailure?.(ctx, { method: 'none', code: 'payment_required', httpStatus: 402 })
      const methods: string[] = ['x402']
      if (config.mpp) methods.push('mpp')
      methods.push('api_key')

      const headers: Record<string, string> = {
        'X-Payment-Required': methods.join(', '),
        'X-Request-Id': requestId,
      }
      if (config.mpp) {
        headers['WWW-Authenticate'] = `Payment realm="${config.mpp.realm}", method="${config.mpp.method ?? 'blueprintevm'}"`
      }

      return c.json({
        error: {
          message: 'Payment required',
          type: 'payment_required',
          payment_methods: methods,
          x402: {
            operator: config.x402.operatorAddress,
            chain_id: config.x402.chainId,
            credits_address: config.x402.creditsAddress,
            estimated_amount_per_request: '20000',
          },
          ...(config.mpp ? {
            mpp: { realm: config.mpp.realm, method: config.mpp.method ?? 'blueprintevm' },
          } : {}),
          api_key: {
            purchase_url: config.baseUrl ? `${config.baseUrl}/agents/${slug}/api-keys` : undefined,
          },
        },
      }, { status: 402, headers })
    }

    await obs?.onPaymentVerified?.(ctx, { method: paymentMethod, consumerId: consumerId!, keyId: keyInfo?.keyId })

    // 4. Rate limit — per-key override or global
    const effectiveRateLimit = keyInfo?.rateLimitPerMinute
      ? { limit: keyInfo.rateLimitPerMinute, windowSeconds: 60 }
      : globalRateLimit

    const rl = await checkRateLimit(consumerId!, effectiveRateLimit, rateLimitStore)
    if (!rl.allowed) {
      await obs?.onRateLimited?.(ctx, { consumerId: consumerId!, retryAfterSeconds: rl.retryAfterSeconds ?? 60 })
      return c.json(
        { error: { message: 'Rate limit exceeded', type: 'rate_limit_error', retry_after: rl.retryAfterSeconds } },
        { status: 429, headers: { 'Retry-After': String(rl.retryAfterSeconds ?? 60), 'X-Request-Id': requestId } },
      )
    }

    // 5. Filter messages — injection detection + sanitization
    const { messages: filtered, injectionWarnings } = filterConsumerMessagesStrict(body.messages, maxLen)

    if (injectionWarnings.length > 0) {
      await obs?.onInjectionDetected?.(ctx, {
        consumerId: consumerId!,
        patterns: injectionWarnings,
        blocked: !!config.blockInjection,
      })

      if (config.blockInjection) {
        return c.json(
          { error: { message: 'Request rejected: potential prompt injection detected', type: 'content_policy_violation' } },
          { status: 400, headers: { 'X-Request-Id': requestId } },
        )
      }
      // In non-blocking mode, continue but the warning is logged for auditing
    }

    const userMessage = filtered
      .filter((m) => m.role === 'user')
      .map((m) => m.content)
      .join('\n\n')

    if (!userMessage) {
      return c.json({ error: { message: 'No user message provided', type: 'invalid_request' } }, 400)
    }

    // 6. Get sandbox and stream response with output filtering
    const inputTokens = Math.ceil(userMessage.length / 4)
    let outputTokens = 0

    const stream = new ReadableStream({
      async start(controller) {
        const encoder = new TextEncoder()
        const sendChunk = (rawDelta: string) => {
          // Redact system prompt leakage from output
          const delta = redactSystemPromptFromOutput(rawDelta, agent.systemPrompt)
          outputTokens += Math.ceil(delta.length / 4)
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
          const box = await config.getSandbox(agent)
          const promptStream = box.streamPrompt(userMessage, {
            sessionId: `consumer:${consumerId}`,
            systemPrompt: agent.systemPrompt,
          })

          for await (const event of promptStream) {
            if (
              event.type === 'message.part.updated' &&
              event.data?.part?.type === 'text' &&
              event.data.delta
            ) {
              sendChunk(event.data.delta)
            }
          }

          // Final chunk
          const done: ChatCompletionChunk = {
            id: `chatcmpl-${Date.now()}`,
            object: 'chat.completion.chunk',
            created: Math.floor(Date.now() / 1000),
            model: agent.slug,
            choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
          }
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(done)}\n\n`))
          controller.enqueue(encoder.encode('data: [DONE]\n\n'))

          // 7. Record usage + settle payment
          const totalCost = (inputTokens + outputTokens) * agent.pricePerTokenUsd
          const ownerEarned = totalCost * (1 - agent.platformFeePercent)
          const platformFee = totalCost * agent.platformFeePercent

          const usageEvent = {
            agentId: agent.id,
            agentSlug: agent.slug,
            consumerId: consumerId!,
            paymentMethod,
            inputTokens,
            outputTokens,
            totalCostUsd: totalCost,
            ownerEarnedUsd: ownerEarned,
            platformFeeUsd: platformFee,
            durationMs: Date.now() - startMs,
          }

          await config.recordUsage(usageEvent)
          await obs?.onRequestComplete?.(ctx, usageEvent)

          if (config.settlePayment) {
            await config.settlePayment({ method: paymentMethod, consumerId: consumerId! }, totalCost).catch(async err => {
              const msg = err instanceof Error ? err.message : String(err)
              console.error(`[agent-gateway] settlement failed for ${consumerId}: ${msg}`)
              await obs?.onSettlementError?.(ctx, { consumerId: consumerId!, method: paymentMethod, errorMessage: msg })
            })
          }
        } catch (err) {
          // Sanitize error — never expose stack traces or internal paths
          const rawMessage = err instanceof Error ? err.message : String(err)
          const safeMessage =
            rawMessage.includes('/') || rawMessage.includes('\\')
              ? 'Internal agent error'
              : rawMessage
          await obs?.onStreamError?.(ctx, { consumerId: consumerId!, errorMessage: rawMessage })
          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify({ error: { message: safeMessage, type: 'server_error' } })}\n\n`),
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
        ...(rl.remaining !== undefined ? { 'X-RateLimit-Remaining': String(rl.remaining) } : {}),
      },
    })
  })

  return gw
}
