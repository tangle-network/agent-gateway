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
import {
  defaultVerifyApiKey,
  isApiKeyAuthEnabled,
  isMppAuthEnabled,
  verifyMpp,
  verifyX402,
} from './verify'

/** Single bundle of long-lived gateway state shared across all handlers in one createAgentGateway call. */
export interface GatewayState {
  rateLimitStore: RateLimitStore
  nonceStore: NonceStore
  globalRateLimit: { limit: number; windowSeconds: number }
  requiredScope: string
  maxLen: number
  maxOutputTokens: number
  defaultOutputTokens: number
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
  maxOutputTokens: number
}

function decimalFraction(value: number): { numerator: bigint; denominator: bigint } {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error('agent pricePerTokenUsd must be a finite positive number')
  }
  const [mantissa, exponentText] = value.toString().toLowerCase().split('e')
  const exponent = exponentText ? Number(exponentText) : 0
  const [whole, fraction = ''] = mantissa.split('.')
  let numerator = BigInt(`${whole}${fraction}`)
  let scale = fraction.length - exponent
  if (scale < 0) {
    numerator *= 10n ** BigInt(-scale)
    scale = 0
  }
  return { numerator, denominator: 10n ** BigInt(scale) }
}

/** Exact base-unit reservation required to cover the request's token ceiling. */
export function requiredX402Amount(
  pricePerTokenUsd: number,
  inputTokens: number,
  maxOutputTokens: number,
  currencyDecimals = 6,
): bigint {
  if (!Number.isSafeInteger(inputTokens) || inputTokens < 0) {
    throw new Error('input token estimate must be a non-negative safe integer')
  }
  if (!Number.isSafeInteger(maxOutputTokens) || maxOutputTokens <= 0) {
    throw new Error('max output tokens must be a positive safe integer')
  }
  if (!Number.isInteger(currencyDecimals) || currencyDecimals < 0 || currencyDecimals > 18) {
    throw new Error('x402 currencyDecimals must be an integer between 0 and 18')
  }
  const { numerator, denominator } = decimalFraction(pricePerTokenUsd)
  const scaled = BigInt(inputTokens + maxOutputTokens) * numerator * 10n ** BigInt(currencyDecimals)
  return (scaled + denominator - 1n) / denominator
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
  requestedMaxOutputTokens?: number,
): Promise<AuthorizedRequest | Response> {
  const startMs = Date.now()
  const requestId = generateRequestId()
  const ctx: RequestContext = { requestId, agentSlug: slug, startMs }
  await state.obs?.onRequestStart?.(ctx)

  const agent = await config.resolveAgent(slug)
  if (!agent || !agent.enabled) {
    return c.json({ error: { message: 'Agent not found', type: 'not_found' } }, 404)
  }
  if (!messages?.length) {
    return c.json(
      { error: { message: 'messages array required', type: 'invalid_request' } },
      400,
    )
  }

  const maxOutputTokens = requestedMaxOutputTokens ?? state.defaultOutputTokens
  if (
    !Number.isInteger(maxOutputTokens) ||
    maxOutputTokens <= 0 ||
    maxOutputTokens > state.maxOutputTokens
  ) {
    return c.json(
      {
        error: {
          message: `max_tokens must be an integer between 1 and ${state.maxOutputTokens}`,
          type: 'invalid_request',
          code: 'invalid_max_tokens',
        },
      },
      400,
    )
  }

  // Price the exact filtered input that reaches the sandbox. This must happen
  // before x402 verification because a production verifier can reserve funds.
  const { messages: filtered, injectionWarnings } = filterConsumerMessagesStrict(
    messages,
    state.maxLen,
  )
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

  let requiredPaymentAmount: bigint
  try {
    requiredPaymentAmount = requiredX402Amount(
      agent.pricePerTokenUsd,
      maximumBillableInputTokens(agent, userMessage),
      maxOutputTokens,
      config.x402.currencyDecimals,
    )
  } catch {
    return c.json(
      {
        error: {
          message: 'Agent payment configuration is invalid',
          type: 'server_error',
          code: 'invalid_payment_configuration',
        },
      },
      503,
    )
  }

  // Payment / auth.
  const spendAuthHeader = c.req.header('X-Payment-Signature')
  const authHeader = c.req.header('Authorization') ?? ''
  let consumerId: string | null = null
  let paymentMethod: PaymentMethod = 'none'
  let keyInfo: ApiKeyInfo | null = null
  let x402Payload: Record<string, unknown> | null = null

  if (spendAuthHeader) {
    const signer = await verifyX402(
      spendAuthHeader,
      config.x402,
      state.nonceStore,
      requiredPaymentAmount,
      false,
    )
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
            required_amount: requiredPaymentAmount.toString(),
            currency_decimals: config.x402.currencyDecimals ?? 6,
          },
        },
        {
          status: 402,
          headers: { 'X-Payment-Required': 'spendauth', 'X-Request-Id': requestId },
        },
      )
    }
    x402Payload = JSON.parse(spendAuthHeader) as Record<string, unknown>
    consumerId = signer
    paymentMethod = 'x402'
  } else if (isMppAuthEnabled(config) && authHeader.toLowerCase().startsWith('payment ')) {
    const signer = await verifyMpp(
      authHeader,
      config.mpp!,
      config.x402,
      state.nonceStore,
      requiredPaymentAmount,
    )
    if (!signer) {
      const realm = config.mpp!.realm
      const method = config.mpp!.method ?? 'blueprintevm'
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
    const verify = config.verifyApiKey ?? (config.x402.demoMode ? defaultVerifyApiKey : null)
    if (!verify || !isApiKeyAuthEnabled(config)) {
      await state.obs?.onAuthFailure?.(ctx, {
        method: 'apikey',
        code: 'api_keys_not_configured',
        httpStatus: 401,
      })
      return c.json(
        { error: { message: 'API key authentication is not configured', type: 'authentication_error' } },
        { status: 401, headers: { 'X-Request-Id': requestId } },
      )
    }
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
    if (isMppAuthEnabled(config)) methods.push('mpp')
    if (isApiKeyAuthEnabled(config)) methods.push('api_key')
    const headers: Record<string, string> = {
      'X-Payment-Required': methods.join(', '),
      'X-Request-Id': requestId,
    }
    if (isMppAuthEnabled(config) && config.mpp) {
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
            required_amount: requiredPaymentAmount.toString(),
            currency_decimals: config.x402.currencyDecimals ?? 6,
            max_output_tokens: maxOutputTokens,
          },
          ...(isMppAuthEnabled(config) && config.mpp
            ? { mpp: { realm: config.mpp.realm, method: config.mpp.method ?? 'blueprintevm' } }
            : {}),
          ...(isApiKeyAuthEnabled(config)
            ? {
                api_key: {
                  purchase_url: config.baseUrl
                    ? `${config.baseUrl}/agents/${slug}/api-keys`
                    : undefined,
                },
              }
            : {}),
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

  // Reject or report injection only after authentication so observer events
  // retain the authenticated consumer identity.
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

  if (paymentMethod === 'x402' && x402Payload) {
    try {
      if (config.x402.authorizePayment) {
        const authorized = await config.x402.authorizePayment(x402Payload, {
          requestId,
          agentId: agent.id,
          requiredAmount: requiredPaymentAmount,
          maxOutputTokens,
        })
        if (!authorized) throw new Error('payment authorization was rejected')
      }

      const nonce = BigInt(String(x402Payload.nonce))
      const expiry = BigInt(String(x402Payload.expiry))
      const nonceKey = `${String(x402Payload.commitment)}:${nonce.toString()}`
      if (!config.x402.authorizePayment && await state.nonceStore.hasSeen(nonceKey)) {
        throw new Error('payment nonce was already consumed')
      }
      const ttl = Math.min(Number(expiry) - Math.floor(Date.now() / 1000), 3600)
      await state.nonceStore.markSeen(nonceKey, Math.max(ttl, 60))
    } catch {
      await state.obs?.onAuthFailure?.(ctx, {
        method: 'x402',
        code: 'payment_authorization_failed',
        httpStatus: 402,
      })
      return c.json(
        {
          error: {
            message: 'Payment authorization failed',
            type: 'payment_required',
            code: 'payment_authorization_failed',
          },
        },
        {
          status: 402,
          headers: { 'X-Payment-Required': 'spendauth', 'X-Request-Id': requestId },
        },
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
    maxOutputTokens,
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
  sessionId?: string,
  maxOutputTokens?: number,
): AsyncIterable<string> {
  for await (const event of dispatchSandboxStreamRich(
    agent,
    userMessage,
    consumerId,
    config,
    signal,
    sessionId,
    maxOutputTokens,
  )) {
    if (event.kind === 'text') yield event.delta
  }
}

/**
 * A2A-shaped dispatch event. Distinguishes text deltas from sandbox-signalled
 * pause-for-input events. The A2A handler uses this richer variant so it can
 * emit `input-required` status updates; the OpenAI-compat path consumes the
 * text-only `dispatchSandboxStream` adapter above.
 */
export type A2ADispatchEvent =
  | { kind: 'text'; delta: string }
  | { kind: 'input-required'; prompt?: string }

/**
 * Like `dispatchSandboxStream` but yields a discriminated union so callers can
 * react to `input-required` signals from the sandbox. The sandbox opts in by
 * emitting `{ type: 'input-required', data: { inputRequired: { prompt? } } }`
 * (or by setting `data.inputRequired` on any event); sandboxes that don't
 * emit such events see identical behavior.
 *
 * `sessionId` defaults to `consumer:<id>` matching the existing single-turn
 * path; multi-turn continuations pass an explicit `taskId` so the sandbox can
 * keep per-task conversation memory.
 */
export async function* dispatchSandboxStreamRich(
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  config: GatewayConfig,
  signal?: AbortSignal,
  sessionId?: string,
  maxOutputTokens?: number,
): AsyncIterable<A2ADispatchEvent> {
  const box = await config.getSandbox(agent)
  const outputLimit = maxOutputTokens ?? config.defaultOutputTokens ?? 1024
  if (!Number.isSafeInteger(outputLimit) || outputLimit <= 0) {
    throw new Error('max output tokens must be a positive safe integer')
  }
  let outputCharacters = 0
  const maxOutputCharacters = outputLimit * 4
  const promptStream = box.streamPrompt(userMessage, {
    sessionId: sessionId ?? `consumer:${consumerId}`,
    systemPrompt: agent.systemPrompt,
    maxOutputTokens: outputLimit,
  })
  for await (const event of promptStream) {
    if (signal?.aborted) return
    if (
      event.type === 'message.part.updated' &&
      event.data?.part?.type === 'text' &&
      event.data.delta
    ) {
      const remainingCharacters = maxOutputCharacters - outputCharacters
      if (remainingCharacters <= 0) return
      const boundedDelta = event.data.delta.slice(0, remainingCharacters)
      outputCharacters += boundedDelta.length
      yield {
        kind: 'text',
        delta: redactSystemPromptFromOutput(boundedDelta, agent.systemPrompt),
      }
      if (boundedDelta.length < event.data.delta.length) return
      continue
    }
    if (event.type === 'input-required' || event.data?.inputRequired) {
      yield { kind: 'input-required', prompt: event.data?.inputRequired?.prompt }
      // Terminal for the sandbox stream — sandbox SHOULD stop emitting until
      // the gateway dispatches a continuation message with the new user input.
      return
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

/** Include the host-owned system prompt because the provider bills it too. */
export function estimateBillableInputTokens(agent: AgentMeta, userMessage: string): number {
  return estimateTokens(userMessage) + estimateTokens(agent.systemPrompt ?? '')
}

/** A tokenizer cannot emit more tokens than the UTF-8 bytes it consumes. */
export function maximumBillableInputTokens(agent: AgentMeta, userMessage: string): number {
  const encoder = new TextEncoder()
  return encoder.encode(userMessage).byteLength + encoder.encode(agent.systemPrompt ?? '').byteLength
}
