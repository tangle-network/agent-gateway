/**
 * Shared inner pipeline used by every wire-format the gateway exposes
 * (OpenAI-compatible chat completions, A2A JSON-RPC). Each handler parses its
 * own protocol's request body into a canonical `messages[]` form + headers,
 * then calls into here for auth → rate-limit → injection filter →
 * authorize → sandbox stream → settle. Keeping the pipeline single-sourced
 * means every protocol surface gets the same security and billing guarantees
 * for free; bugs fixed here fix every wrapper.
 */

import type { Context } from 'hono'

import { filterConsumerMessagesStrict, redactSystemPromptFromOutput } from './filter'
import { type GatewayObserver, type RequestContext, generateRequestId } from './observer'
import { type RateLimitStore, checkRateLimit } from './rate-limit'
import type { NonceStore } from './nonce-store'
import type {
  AgentMeta,
  ApiKeyInfo,
  ChatMessage,
  GatewayConfig,
  PaymentMethod,
} from './types'
import { defaultVerifyApiKey, verifyMpp, verifyX402 } from './verify'

/** Single bundle of long-lived gateway state shared across all handlers in one createAgentGateway call. */
export interface GatewayState {
  rateLimitStore: RateLimitStore
  nonceStore: NonceStore
  globalRateLimit: { limit: number; windowSeconds: number }
  requiredScope: string
  maxLen: number
  obs?: GatewayObserver
}

/** Returned by {@link authenticateAndGuard} on the success path. */
export interface AuthorizedRequest {
  agent: AgentMeta
  consumerId: string
  paymentMethod: PaymentMethod
  keyInfo: ApiKeyInfo | null
  userMessage: string
  rateLimitRemaining: number | undefined
  requestId: string
  startMs: number
}

/**
 * Resolve the agent, then run the full pre-dispatch pipeline: payment +
 * rate-limit + injection filter + user-message extraction + optional
 * `authorizeConsumer` hook. Returns the success record on the happy path
 * or a fully-formed `Response` (402/404/429/400/403) on any short-circuit.
 *
 * Body parsing is the caller's responsibility — different wire formats
 * (OpenAI chat completions vs A2A JSON-RPC) have different envelopes; both
 * still ultimately produce a `ChatMessage[]`.
 */
export async function authenticateAndGuard(
  c: Context,
  slug: string,
  messages: ChatMessage[],
  config: GatewayConfig,
  state: GatewayState,
): Promise<AuthorizedRequest | Response> {
  const startMs = Date.now()
  const requestId = generateRequestId()
  const ctx: RequestContext = { requestId, agentSlug: slug, startMs }
  await state.obs?.onRequestStart?.(ctx)

  const agent = await config.resolveAgent(slug)
  if (!agent) {
    return c.json({ error: { message: 'Agent not found', type: 'not_found' } }, 404)
  }
  if (!messages?.length) {
    return c.json(
      { error: { message: 'messages array required', type: 'invalid_request' } },
      400,
    )
  }

  // Payment / auth.
  const spendAuthHeader = c.req.header('X-Payment-Signature')
  const authHeader = c.req.header('Authorization') ?? ''
  let consumerId: string | null = null
  let paymentMethod: PaymentMethod = 'none'
  let keyInfo: ApiKeyInfo | null = null

  if (spendAuthHeader) {
    const signer = await verifyX402(spendAuthHeader, config.x402, state.nonceStore)
    if (!signer) {
      await state.obs?.onAuthFailure?.(ctx, {
        method: 'x402',
        code: 'invalid_spend_auth',
        httpStatus: 402,
      })
      return c.json(
        {
          error: {
            message: 'Invalid X-Payment-Signature',
            type: 'authentication_error',
            code: 'invalid_spend_auth',
          },
        },
        {
          status: 402,
          headers: { 'X-Payment-Required': 'spendauth', 'X-Request-Id': requestId },
        },
      )
    }
    consumerId = signer
    paymentMethod = 'x402'
  } else if (config.mpp && authHeader.toLowerCase().startsWith('payment ')) {
    const signer = await verifyMpp(authHeader, config.mpp, config.x402)
    if (!signer) {
      const realm = config.mpp.realm
      const method = config.mpp.method ?? 'blueprintevm'
      await state.obs?.onAuthFailure?.(ctx, {
        method: 'mpp',
        code: 'invalid_mpp_credential',
        httpStatus: 401,
      })
      return c.json(
        {
          error: {
            message: 'Invalid Payment credential',
            type: 'authentication_error',
            code: 'invalid_mpp_credential',
          },
        },
        {
          status: 401,
          headers: {
            'WWW-Authenticate': `Payment realm="${realm}", method="${method}"`,
            'X-Request-Id': requestId,
          },
        },
      )
    }
    consumerId = signer
    paymentMethod = 'mpp'
  } else if (authHeader.startsWith('Bearer ')) {
    const verify = config.verifyApiKey ?? defaultVerifyApiKey
    const key = await verify(authHeader)
    if (!key) {
      await state.obs?.onAuthFailure?.(ctx, {
        method: 'apikey',
        code: 'invalid_api_key',
        httpStatus: 401,
      })
      return c.json(
        { error: { message: 'Invalid API key', type: 'authentication_error' } },
        { status: 401, headers: { 'X-Request-Id': requestId } },
      )
    }
    if (key.scopes && key.scopes.length > 0 && !key.scopes.includes(state.requiredScope)) {
      await state.obs?.onAuthFailure?.(ctx, {
        method: 'apikey',
        code: 'insufficient_scope',
        httpStatus: 403,
      })
      return c.json(
        {
          error: {
            message: `API key missing required scope: ${state.requiredScope}`,
            type: 'forbidden',
            code: 'insufficient_scope',
          },
        },
        { status: 403, headers: { 'X-Request-Id': requestId } },
      )
    }
    consumerId = key.consumerId
    paymentMethod = 'apikey'
    keyInfo = key
  } else {
    await state.obs?.onAuthFailure?.(ctx, {
      method: 'none',
      code: 'payment_required',
      httpStatus: 402,
    })
    const methods: string[] = ['x402']
    if (config.mpp) methods.push('mpp')
    methods.push('api_key')
    const headers: Record<string, string> = {
      'X-Payment-Required': methods.join(', '),
      'X-Request-Id': requestId,
    }
    if (config.mpp) {
      headers['WWW-Authenticate'] =
        `Payment realm="${config.mpp.realm}", method="${config.mpp.method ?? 'blueprintevm'}"`
    }
    return c.json(
      {
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
          ...(config.mpp
            ? { mpp: { realm: config.mpp.realm, method: config.mpp.method ?? 'blueprintevm' } }
            : {}),
          api_key: {
            purchase_url: config.baseUrl
              ? `${config.baseUrl}/agents/${slug}/api-keys`
              : undefined,
          },
        },
      },
      { status: 402, headers },
    )
  }

  await state.obs?.onPaymentVerified?.(ctx, {
    method: paymentMethod,
    consumerId: consumerId,
    keyId: keyInfo?.keyId,
  })

  // Rate limit.
  const effectiveRateLimit = keyInfo?.rateLimitPerMinute
    ? { limit: keyInfo.rateLimitPerMinute, windowSeconds: 60 }
    : state.globalRateLimit
  const rl = await checkRateLimit(consumerId, effectiveRateLimit, state.rateLimitStore)
  if (!rl.allowed) {
    await state.obs?.onRateLimited?.(ctx, {
      consumerId: consumerId,
      retryAfterSeconds: rl.retryAfterSeconds ?? 60,
    })
    return c.json(
      {
        error: {
          message: 'Rate limit exceeded',
          type: 'rate_limit_error',
          retry_after: rl.retryAfterSeconds,
        },
      },
      {
        status: 429,
        headers: {
          'Retry-After': String(rl.retryAfterSeconds ?? 60),
          'X-Request-Id': requestId,
        },
      },
    )
  }

  // Filter consumer messages — strip consumer-side system, length-cap, injection scan.
  const { messages: filtered, injectionWarnings } = filterConsumerMessagesStrict(
    messages,
    state.maxLen,
  )
  if (injectionWarnings.length > 0) {
    await state.obs?.onInjectionDetected?.(ctx, {
      consumerId: consumerId,
      patterns: injectionWarnings,
      blocked: !!config.blockInjection,
    })
    if (config.blockInjection) {
      return c.json(
        {
          error: {
            message: 'Request rejected: potential prompt injection detected',
            type: 'content_policy_violation',
          },
        },
        { status: 400, headers: { 'X-Request-Id': requestId } },
      )
    }
  }

  const userMessage = filtered
    .filter((m) => m.role === 'user')
    .map((m) => m.content)
    .join('\n\n')
  if (!userMessage) {
    return c.json(
      { error: { message: 'No user message provided', type: 'invalid_request' } },
      400,
    )
  }

  if (config.authorizeConsumer) {
    const authz = await config.authorizeConsumer(agent, {
      method: paymentMethod,
      consumerId: consumerId,
      keyId: keyInfo?.keyId,
      requestId,
    })
    if (!authz.allow) {
      return c.json(
        {
          error: {
            message: authz.reason,
            type: 'authorization_denied',
            code: authz.code,
          },
        },
        { status: 403, headers: { 'X-Request-Id': requestId } },
      )
    }
  }

  return {
    agent,
    consumerId,
    paymentMethod,
    keyInfo,
    userMessage,
    rateLimitRemaining: rl.remaining,
    requestId,
    startMs,
  }
}

/**
 * Yield the inner sandbox's response as text deltas, applying the
 * system-prompt redaction filter on each delta so leakage of the agent's
 * system prompt back through the model's output is suppressed identically
 * whether the caller is on the OpenAI-compat path or A2A.
 *
 * Aborts when `signal` fires (used by A2A `tasks/cancel`).
 */
export async function* dispatchSandboxStream(
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  config: GatewayConfig,
  signal?: AbortSignal,
): AsyncIterable<string> {
  const box = await config.getSandbox(agent)
  const promptStream = box.streamPrompt(userMessage, {
    sessionId: `consumer:${consumerId}`,
    systemPrompt: agent.systemPrompt,
  })
  for await (const event of promptStream) {
    if (signal?.aborted) return
    if (
      event.type === 'message.part.updated' &&
      event.data?.part?.type === 'text' &&
      event.data.delta
    ) {
      yield redactSystemPromptFromOutput(event.data.delta, agent.systemPrompt)
    }
  }
}

/**
 * Record usage event + settle payment + invoke the observer. Both wire
 * formats call this once their stream has drained, so settlement happens
 * exactly once per request regardless of protocol.
 */
export async function settleAndRecord(
  agent: AgentMeta,
  authz: AuthorizedRequest,
  inputTokens: number,
  outputTokens: number,
  config: GatewayConfig,
  obs: GatewayObserver | undefined,
): Promise<void> {
  const totalCost = (inputTokens + outputTokens) * agent.pricePerTokenUsd
  const ownerEarned = totalCost * (1 - agent.platformFeePercent)
  const platformFee = totalCost * agent.platformFeePercent
  const usageEvent = {
    requestId: authz.requestId,
    agentId: agent.id,
    agentSlug: agent.slug,
    consumerId: authz.consumerId,
    paymentMethod: authz.paymentMethod,
    inputTokens,
    outputTokens,
    totalCostUsd: totalCost,
    ownerEarnedUsd: ownerEarned,
    platformFeeUsd: platformFee,
    durationMs: Date.now() - authz.startMs,
  }
  await config.recordUsage(usageEvent)
  const ctx: RequestContext = {
    requestId: authz.requestId,
    agentSlug: agent.slug,
    startMs: authz.startMs,
  }
  await obs?.onRequestComplete?.(ctx, usageEvent)
  if (config.settlePayment) {
    await config
      .settlePayment(
        {
          method: authz.paymentMethod,
          consumerId: authz.consumerId,
          requestId: authz.requestId,
        },
        totalCost,
      )
      .catch(async (err) => {
        const msg = err instanceof Error ? err.message : String(err)
        console.error(`[agent-gateway] settlement failed for ${authz.consumerId}: ${msg}`)
        await obs?.onSettlementError?.(ctx, {
          consumerId: authz.consumerId,
          method: authz.paymentMethod,
          errorMessage: msg,
        })
      })
  }
}

/** Token estimate matching the existing chat-completions handler (4 chars ≈ 1 token). */
export function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4)
}
