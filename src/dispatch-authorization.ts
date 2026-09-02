import type { Context } from 'hono'

import { isChatMessageArray } from './chat-input'
import { filterConsumerMessagesStrict } from './filter'
import { type RequestContext, generateRequestId } from './observer'
import { type GatewayState, type AuthorizedRequest } from './dispatch-types'
import { checkRateLimit } from './rate-limit'
import {
  maximumBillableInputTokens,
  requiredX402Amount,
} from './dispatch-pricing'
import type {
  ApiKeyInfo,
  ChatMessage,
  GatewayConfig,
  PaymentMethod,
  SandboxExecutionBudget,
} from './types'
import {
  defaultVerifyApiKey,
  isApiKeyAuthEnabled,
  isMppAuthEnabled,
  isX402AuthEnabled,
  mppPaymentPayload,
  mppPaymentCredential,
  verifyMppCredential,
  verifyX402,
} from './verify'

/**
 * Resolve the agent, then run the full pre-dispatch pipeline: payment +
 * rate-limit + injection filter + user-message extraction + optional
 * `authorizeConsumer` hook. Returns the success record on the happy path
 * or a fully-formed `Response` (402/404/429/400/403) on any short-circuit.
 *
 * Body parsing is the caller's responsibility — different wire formats
 * (OpenAI chat completions vs A2A JSON-RPC) have different envelopes; both
 * still ultimately produce a `ChatMessage[]`.
 * A2A may provide its durable context ID so authorization and adapter access
 * use the same thread identity.
 */
export async function authenticateAndGuard(
  c: Context,
  slug: string,
  messages: ChatMessage[],
  config: GatewayConfig,
  state: GatewayState,
  requestedMaxOutputTokens?: number,
  requestedThreadId?: string,
): Promise<AuthorizedRequest | Response> {
  const startMs = Date.now()
  const requestId = generateRequestId()
  const ctx: RequestContext = { requestId, agentSlug: slug, startMs }
  await state.obs?.onRequestStart?.(ctx)

  let threadId: string | undefined
  if (config.conversationMode === 'thread') {
    const headerThreadId = c.req.header('X-Tangle-Thread-Id')?.trim()
    const requestedContextThreadId = typeof requestedThreadId === 'string'
      ? requestedThreadId.trim()
      : undefined
    if (
      headerThreadId &&
      requestedContextThreadId &&
      headerThreadId !== requestedContextThreadId
    ) {
      return c.json(
        { error: { message: 'Thread identity does not match A2A context', type: 'invalid_request' } },
        { status: 400, headers: { 'X-Request-Id': requestId } },
      )
    }
    const requestedThread = headerThreadId || requestedContextThreadId
    if (requestedThread && !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(requestedThread)) {
      return c.json(
        { error: { message: 'Invalid X-Tangle-Thread-Id', type: 'invalid_request' } },
        { status: 400, headers: { 'X-Request-Id': requestId } },
      )
    }
    threadId = requestedThread || requestId
  }

  const agent = await config.resolveAgent(slug)
  if (!agent || !agent.enabled) {
    return c.json({ error: { message: 'Agent not found', type: 'not_found' } }, 404)
  }
  if (!isChatMessageArray(messages)) {
    return c.json(
      {
        error: {
          message: 'messages must be a non-empty array of role/content objects',
          type: 'invalid_request',
        },
      },
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

  // Quote the maximum UTF-8 input plus every hidden provider cost before
  // verification. The verifier must remain read-only at this point.
  const { messages: filtered, injectionWarnings } = filterConsumerMessagesStrict(
    messages,
    state.maxLen,
  )
  const userMessages = filtered
    .filter((m) => m.role === 'user')
  // A thread-backed host already owns the persisted history. Send only the
  // new turn to avoid storing the caller's full history as one new user row.
  // Keep the historical concatenation for the default consumer session.
  const userMessage = config.conversationMode === 'thread'
    ? userMessages[userMessages.length - 1]?.content ?? ''
    : userMessages.map((m) => m.content).join('\n\n')
  if (!userMessage) {
    return c.json(
      { error: { message: 'No user message provided', type: 'invalid_request' } },
      400,
    )
  }

  let requiredPaymentAmount: bigint
  const messageInputBound = maximumBillableInputTokens(agent, filtered)
  let maxInputTokens = messageInputBound
  if (config.inputTokenBound) {
    let configuredBound: number
    try {
      configuredBound = config.inputTokenBound({ agent, messages: filtered })
    } catch {
      return c.json(
        {
          error: {
            message: 'Agent input token bound is unavailable',
            type: 'server_error',
            code: 'input_token_bound_unavailable',
          },
        },
        503,
      )
    }
    if (!Number.isSafeInteger(configuredBound) || configuredBound < messageInputBound) {
      return c.json(
        {
          error: {
            message: 'Agent input token bound is invalid',
            type: 'server_error',
            code: 'invalid_input_token_bound',
          },
        },
        503,
      )
    }
    maxInputTokens = configuredBound
  }
  const maxReasoningTokens = state.maxReasoningTokens
  const maxToolTokens = state.maxToolTokens
  const maxToolCalls = state.maxToolCalls
  const maxProviderCostUsd = state.maxProviderCostUsd ??
    (maxInputTokens + maxOutputTokens + maxReasoningTokens + maxToolTokens) * agent.pricePerTokenUsd
  const executionBudget: SandboxExecutionBudget = {
    maxInputTokens,
    maxOutputTokens,
    maxReasoningTokens,
    maxToolTokens,
    maxToolCalls,
    maxProviderCostUsd,
  }
  try {
    requiredPaymentAmount = requiredX402Amount(
      agent.pricePerTokenUsd,
      maxInputTokens,
      maxOutputTokens,
      config.x402.currencyDecimals,
      maxReasoningTokens,
      maxToolTokens,
      maxProviderCostUsd,
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
  let paymentNonceKey: string | undefined
  let mppMethod: string | undefined
  let mppCredential: string | undefined
  let mppPaymentIdentity: string | undefined

  if (spendAuthHeader) {
    if (!isX402AuthEnabled(config)) {
      return c.json(
        { error: { message: 'x402 authentication is not configured', type: 'authentication_error' } },
        { status: 401, headers: { 'X-Request-Id': requestId } },
      )
    }
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
    paymentNonceKey = `${String(x402Payload.commitment).toLowerCase()}:${BigInt(String(x402Payload.nonce)).toString()}`
    consumerId = signer
    paymentMethod = 'x402'
  } else if (isMppAuthEnabled(config) && authHeader.toLowerCase().startsWith('payment ')) {
    const authenticated = await verifyMppCredential(
      authHeader,
      config.mpp!,
      config.x402,
      state.nonceStore,
      requiredPaymentAmount,
      false,
    )
    if (!authenticated) {
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
    consumerId = authenticated.consumerId
    paymentMethod = 'mpp'
    mppMethod = authHeader.match(/^Payment\s+(\S+)\s+/i)?.[1]?.toLowerCase()
    mppCredential = mppPaymentCredential(authHeader)
    mppPaymentIdentity = authenticated.paymentIdentity
    x402Payload = mppPaymentPayload(authHeader) ?? null
    paymentNonceKey = authenticated.replayKey
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
    const methods: string[] = []
    if (isX402AuthEnabled(config)) methods.push('x402')
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
          ...(isX402AuthEnabled(config)
            ? {
                x402: {
                  operator: config.x402.operatorAddress,
                  chain_id: config.x402.chainId,
                  credits_address: config.x402.creditsAddress,
                  required_amount: requiredPaymentAmount.toString(),
                  currency_decimals: config.x402.currencyDecimals ?? 6,
                  max_output_tokens: maxOutputTokens,
                },
              }
            : {}),
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
      ownerId: keyInfo?.ownerId,
      requestId,
      ...(threadId ? { threadId } : {}),
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
    messages: filtered,
    ...(threadId ? { threadId } : {}),
    startMs,
    maxOutputTokens,
    executionBudget,
    requiredPaymentAmount,
    paymentPayload: x402Payload,
    paymentNonceKey,
    mppMethod,
    mppCredential,
    mppPaymentIdentity,
  }
}

export type { AuthorizedRequest, GatewayState }
