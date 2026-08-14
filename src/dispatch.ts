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
import type { PaymentOperation } from './payment-operations'
import type {
  AgentMeta,
  ApiKeyInfo,
  ChatMessage,
  GatewayConfig,
  PaymentMethod,
  SandboxExecutionBudget,
  SandboxUsageReceipt,
} from './types'
import {
  defaultVerifyApiKey,
  isApiKeyAuthEnabled,
  isMppAuthEnabled,
  mppReplayNonceKey,
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
  maxReasoningTokens: number
  maxToolTokens: number
  maxToolCalls: number
  maxProviderCostUsd?: number
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
  executionBudget: SandboxExecutionBudget
  requiredPaymentAmount: bigint
  paymentPayload: Record<string, unknown> | null
  paymentNonceKey?: string
  paymentOperation?: PaymentOperation
}

function decimalFraction(value: number): { numerator: bigint; denominator: bigint } {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error('agent pricePerTokenUsd must be a finite non-negative number')
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
  maxReasoningTokens = 0,
  maxToolTokens = 0,
  maxProviderCostUsd = 0,
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
  for (const [name, value] of [
    ['maxReasoningTokens', maxReasoningTokens],
    ['maxToolTokens', maxToolTokens],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a non-negative safe integer`)
  }
  if (!Number.isFinite(maxProviderCostUsd) || maxProviderCostUsd < 0) {
    throw new Error('maxProviderCostUsd must be finite and non-negative')
  }
  const { numerator, denominator } = decimalFraction(pricePerTokenUsd)
  const tokenCount = inputTokens + maxOutputTokens + maxReasoningTokens + maxToolTokens
  if (!Number.isSafeInteger(tokenCount)) throw new Error('token budget exceeds safe integer range')
  const tokenScaled = BigInt(tokenCount) *
    numerator * 10n ** BigInt(currencyDecimals)
  const tokenAmount = (tokenScaled + denominator - 1n) / denominator
  const provider = maxProviderCostUsd === 0
    ? { numerator: 0n, denominator: 1n }
    : decimalFraction(maxProviderCostUsd)
  const providerScaled = provider.numerator * 10n ** BigInt(currencyDecimals)
  const providerAmount = (providerScaled + provider.denominator - 1n) / provider.denominator
  return tokenAmount > providerAmount ? tokenAmount : providerAmount
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

  // Quote the maximum UTF-8 input plus every hidden provider cost before
  // verification. The verifier must remain read-only at this point.
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
  const maxInputTokens = maximumBillableInputTokens(agent, userMessage)
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
    paymentNonceKey = `${String(x402Payload.commitment).toLowerCase()}:${BigInt(String(x402Payload.nonce)).toString()}`
    consumerId = signer
    paymentMethod = 'x402'
  } else if (isMppAuthEnabled(config) && authHeader.toLowerCase().startsWith('payment ')) {
    const signer = await verifyMpp(
      authHeader,
      config.mpp!,
      config.x402,
      state.nonceStore,
      requiredPaymentAmount,
      false,
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
    paymentNonceKey = mppReplayNonceKey(authHeader)
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
    executionBudget,
    requiredPaymentAmount,
    paymentPayload: x402Payload,
    paymentNonceKey,
  }
}

/** Claim payment ownership after every request guard has accepted the call. */
export async function claimPayment(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  state: GatewayState,
): Promise<void> {
  if (authz.paymentMethod === 'x402' && authz.paymentPayload) {
    const context = {
      requestId: authz.requestId,
      agentId: authz.agent.id,
      requiredAmount: authz.requiredPaymentAmount,
      maxOutputTokens: authz.maxOutputTokens,
      executionBudget: authz.executionBudget,
    }
    let operation: PaymentOperation | undefined
    if (config.x402.authorizePayment) {
      // Version 1 has no durable operation to release if another request wins
      // the shared nonce while this callback is still running. Claim first so
      // an external reserve or charge cannot happen for a losing request.
      const legacyClaimed = config.x402.paymentProtocolVersion !== 2 && authz.paymentNonceKey
        ? await claimNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload)
        : undefined
      if (legacyClaimed === false) throw new Error('payment nonce was already consumed')
      const result = await config.x402.authorizePayment(authz.paymentPayload, context)
      if (!result) throw new Error('payment authorization was rejected')
      if (typeof result !== 'boolean') {
        operation = result
        // Retain the durable owner even if a later compatibility check fails.
        // The caller can then release or reclaim the operation instead of
        // losing its recovery handle.
        authz.paymentOperation = operation
      }
      else if (config.x402.paymentProtocolVersion === 2) {
        throw new Error('version 2 payment authorization did not return an operation')
      } else if (authz.paymentNonceKey && legacyClaimed === undefined) {
        const claimed = await claimNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload)
        if (!claimed) throw new Error('payment nonce was already consumed')
      }
    } else if (config.x402.paymentOperations) {
      operation = await config.x402.paymentOperations.claimPayment(authz.paymentPayload, context)
    } else if (authz.paymentNonceKey) {
      const claimed = await claimNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload)
      if (!claimed) throw new Error('payment nonce was already consumed')
    }
    if (operation && operation.protocolVersion !== 2) {
      throw new Error('payment operation protocol version mismatch')
    }
    if (operation && !config.x402.paymentOperations) {
      throw new Error('durable payment operations are required to settle a claimed operation')
    }
    if (operation && authz.paymentNonceKey) {
      const claimed = await claimNonce(
        state.nonceStore,
        authz.paymentNonceKey,
        authz.paymentPayload,
        operation.operationId,
      )
      if (!claimed) {
        try {
          await config.x402.paymentOperations!.releasePayment(operation, 'shared payment nonce was already owned')
        } catch (releaseError) {
          console.error(
            `[agent-gateway] payment release failed for ${authz.requestId}:`,
            releaseError instanceof Error ? releaseError.message : String(releaseError),
          )
        }
        throw new Error('payment nonce was already consumed')
      }
    }
    authz.paymentOperation = operation
  } else if (authz.paymentMethod === 'mpp' && authz.paymentNonceKey) {
    const claimed = await claimNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload ?? {})
    if (!claimed) throw new Error('payment nonce was already consumed')
  }

  try {
    await state.obs?.onPaymentVerified?.(
      {
        requestId: authz.requestId,
        agentSlug: authz.agent.slug,
        startMs: authz.startMs,
      },
      {
        method: authz.paymentMethod,
        consumerId: authz.consumerId,
        keyId: authz.keyInfo?.keyId,
      },
    )
  } catch (error) {
    // Observability must not turn a durable claim into a stranded payment.
    console.error(
      '[agent-gateway] payment observer failed for ' + authz.requestId + ':',
      error instanceof Error ? error.message : String(error),
    )
  }
}

/** Release an owned operation when execution cannot produce a valid receipt. */
export async function releasePayment(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  reason: string,
): Promise<void> {
  if (!authz.paymentOperation || !config.x402.paymentOperations) return
  await config.x402.paymentOperations.releasePayment(authz.paymentOperation, reason)
}

/**
 * Release only when no sandbox work was observed. Once output or a receipt
 * exists, retain the owner for settlement or background recovery.
 */
export async function releasePaymentAfterFailure(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  reason: string,
  workObserved: boolean,
): Promise<void> {
  if (workObserved) {
    console.error(
      `[agent-gateway] retaining payment ownership after sandbox work for ${authz.requestId}: ${reason}`,
    )
    return
  }
  await releasePayment(authz, config, reason)
}

export async function reclaimPayment(
  operationId: string,
  config: GatewayConfig,
): Promise<PaymentOperation> {
  if (!config.x402.paymentOperations) throw new Error('durable payment operations are not configured')
  return config.x402.paymentOperations.reclaimPayment(operationId)
}

async function claimNonce(
  nonceStore: NonceStore,
  nonceKey: string,
  payload: Record<string, unknown>,
  ownerId?: string,
): Promise<boolean> {
  const expiry = Number(payload.expiry ?? Math.floor(Date.now() / 1000) + 3600)
  const ttl = Math.min(expiry - Math.floor(Date.now() / 1000), 3600)
  if (!Number.isFinite(ttl) || ttl <= 0) return false
  return nonceStore.claim(nonceKey, Math.max(ttl, 60), ownerId)
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
  | { kind: 'activity' }
  | { kind: 'usage'; usage: SandboxUsageReceipt }

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
  let outputBytes = 0
  // Bound untrusted adapters while the final receipt is pending. The receipt
  // remains authoritative for token count, so over-limit output is never sent.
  const maxOutputBytes = outputLimit * 4
  if (!Number.isSafeInteger(maxOutputBytes)) {
    throw new Error('max output token bound exceeds safe integer range')
  }
  const encoder = new TextEncoder()
  let usageParts: Partial<SandboxUsageReceipt> = {}
  let observedReasoningTokens = 0
  let observedToolTokens = 0
  let observedToolCalls = 0
  let legacyOutputText = ''
  const executionBudget: SandboxExecutionBudget = {
    maxInputTokens: maximumBillableInputTokens(agent, userMessage),
    maxOutputTokens: outputLimit,
    maxReasoningTokens: config.executionBudget?.maxReasoningTokens ?? outputLimit,
    maxToolTokens: config.executionBudget?.maxToolTokens ?? outputLimit,
    maxToolCalls: config.executionBudget?.maxToolCalls ?? 8,
    maxProviderCostUsd: config.executionBudget?.maxProviderCostUsd ??
      (maximumBillableInputTokens(agent, userMessage) + outputLimit +
        (config.executionBudget?.maxReasoningTokens ?? outputLimit) +
        (config.executionBudget?.maxToolTokens ?? outputLimit)) * agent.pricePerTokenUsd,
  }
  const promptStream = box.streamPrompt(userMessage, {
    sessionId: sessionId ?? `consumer:${consumerId}`,
    systemPrompt: agent.systemPrompt,
    maxOutputTokens: outputLimit,
    executionBudget,
  })
  for await (const event of promptStream) {
    if (signal?.aborted) return
    if (event.data?.usage) usageParts = mergeUsage(usageParts, event.data.usage)
    if (event.data?.reasoning?.tokens !== undefined) {
      observedReasoningTokens += nonNegativeSafeInteger(event.data.reasoning.tokens, 'reasoning tokens')
      yield { kind: 'activity' }
    }
    if (event.data?.tool) {
      observedToolCalls += 1
      observedToolTokens +=
        nonNegativeSafeInteger(event.data.tool.inputTokens ?? 0, 'tool input tokens') +
        nonNegativeSafeInteger(event.data.tool.outputTokens ?? 0, 'tool output tokens')
      yield { kind: 'activity' }
    }
    enforceUsageBudget(withObservedUsage(
      usageParts,
      observedReasoningTokens,
      observedToolTokens,
      observedToolCalls,
    ), executionBudget)
    if (
      event.type === 'message.part.updated' &&
      event.data?.part?.type === 'text' &&
      event.data.delta
    ) {
      const remainingBytes = maxOutputBytes - outputBytes
      if (remainingBytes <= 0) throw new Error('sandbox exceeded max output tokens')
      const bounded = truncateUtf8(event.data.delta, remainingBytes, encoder)
      if (bounded.truncated) {
        yield { kind: 'activity' }
        throw new Error('sandbox exceeded max output tokens')
      }
      outputBytes += bounded.bytes
      legacyOutputText += bounded.text
      yield { kind: 'activity' }
      yield { kind: 'text', delta: redactSystemPromptFromOutput(bounded.text, agent.systemPrompt) }
      continue
    }
    if (event.type === 'input-required' || event.data?.inputRequired) {
      const usage = completeUsage(
        usageParts,
        observedReasoningTokens,
        observedToolTokens,
        observedToolCalls,
        userMessage,
        legacyOutputText,
        executionBudget,
        config.x402.paymentOperations !== undefined,
      )
      yield { kind: 'input-required', prompt: event.data?.inputRequired?.prompt }
      // Terminal for the sandbox stream — sandbox SHOULD stop emitting until
      // the gateway dispatches a continuation message with the new user input.
      yield { kind: 'usage', usage }
      return
    }
  }
  const usage = completeUsage(
    usageParts,
    observedReasoningTokens,
    observedToolTokens,
    observedToolCalls,
    userMessage,
    legacyOutputText,
    executionBudget,
    config.x402.paymentOperations !== undefined,
  )
  yield { kind: 'usage', usage }
}

/**
 * Record usage event + settle payment + invoke the observer. Both wire
 * formats call this once their stream has drained, so settlement happens
 * exactly once per request regardless of protocol.
 */
export async function settleAndRecord(
  agent: AgentMeta,
  authz: AuthorizedRequest,
  usage: SandboxUsageReceipt,
  config: GatewayConfig,
  obs: GatewayObserver | undefined,
): Promise<void> {
  const tokenCost = (
    usage.inputTokens + usage.outputTokens + usage.reasoningTokens + usage.toolTokens
  ) * agent.pricePerTokenUsd
  const totalCost = Math.max(tokenCost, usage.providerCostUsd)
  const ownerEarned = totalCost * (1 - agent.platformFeePercent)
  const platformFee = totalCost * agent.platformFeePercent
  const usageEvent = {
    requestId: authz.requestId,
    agentId: agent.id,
    agentSlug: agent.slug,
    consumerId: authz.consumerId,
    paymentMethod: authz.paymentMethod,
    inputTokens: usage.inputTokens,
    outputTokens: usage.outputTokens,
    reasoningTokens: usage.reasoningTokens,
    toolTokens: usage.toolTokens,
    toolCallCount: usage.toolCallCount,
    providerCostUsd: usage.providerCostUsd,
    totalCostUsd: totalCost,
    ownerEarnedUsd: ownerEarned,
    platformFeeUsd: platformFee,
    durationMs: Date.now() - authz.startMs,
  }
  const ctx: RequestContext = {
    requestId: authz.requestId,
    agentSlug: agent.slug,
    startMs: authz.startMs,
  }
  try {
    // Record attribution before settlement. Marketplace adapters use this
    // row to map the request to its agent and consumer before deducting funds.
    await config.recordUsage(usageEvent)
    if (authz.paymentOperation && config.x402.paymentOperations) {
      const amount = actualX402Amount(
        agent.pricePerTokenUsd,
        usage.inputTokens,
        usage.outputTokens,
        usage.reasoningTokens,
        usage.toolTokens,
        config.x402.currencyDecimals,
        usage.providerCostUsd,
      )
      authz.paymentOperation = await config.x402.paymentOperations.settlePayment(
        authz.paymentOperation,
        { amount, totalCostUsd: totalCost, usage },
      )
    } else if (config.settlePayment) {
      await config.settlePayment(
        {
          method: authz.paymentMethod,
          consumerId: authz.consumerId,
          requestId: authz.requestId,
        },
        totalCost,
      )
    }
    await obs?.onRequestComplete?.(ctx, usageEvent)
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    console.error(`[agent-gateway] settlement failed for ${authz.consumerId}: ${msg}`)
    await obs?.onSettlementError?.(ctx, {
      consumerId: authz.consumerId,
      method: authz.paymentMethod,
      errorMessage: msg,
    })
    throw err
  }
}

function mergeUsage(
  current: Partial<SandboxUsageReceipt>,
  update: Partial<SandboxUsageReceipt>,
): Partial<SandboxUsageReceipt> {
  const merged = { ...current, ...update }
  for (const key of ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount', 'providerCostUsd'] as const) {
    const value = update[key]
    if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
      throw new Error(`sandbox usage field ${key} is invalid`)
    }
    if (value !== undefined && current[key] !== undefined) {
      // Usage events are cumulative receipts. Never let a later partial or
      // final event erase spend observed earlier in the same execution.
      merged[key] = Math.max(current[key]!, value)
    }
  }
  if (current.budgetEnforced === false || update.budgetEnforced === false) {
    merged.budgetEnforced = false
  }
  return merged
}

function withObservedUsage(
  usage: Partial<SandboxUsageReceipt>,
  reasoningTokens: number,
  toolTokens: number,
  toolCallCount: number,
): Partial<SandboxUsageReceipt> {
  return {
    ...usage,
    ...(usage.reasoningTokens !== undefined || reasoningTokens > 0
      ? { reasoningTokens: Math.max(usage.reasoningTokens ?? 0, reasoningTokens) }
      : {}),
    ...(usage.toolTokens !== undefined || toolTokens > 0
      ? { toolTokens: Math.max(usage.toolTokens ?? 0, toolTokens) }
      : {}),
    ...(usage.toolCallCount !== undefined || toolCallCount > 0
      ? { toolCallCount: Math.max(usage.toolCallCount ?? 0, toolCallCount) }
      : {}),
  }
}

function nonNegativeSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`sandbox ${name} is invalid`)
  }
  return value
}

function enforceUsageBudget(
  usage: Partial<SandboxUsageReceipt>,
  budget: SandboxExecutionBudget,
): void {
  if (usage.inputTokens !== undefined && usage.inputTokens > budget.maxInputTokens) {
    throw new Error('sandbox exceeded max input tokens')
  }
  if (usage.outputTokens !== undefined && usage.outputTokens > budget.maxOutputTokens) {
    throw new Error('sandbox exceeded max output tokens')
  }
  if (usage.reasoningTokens !== undefined && usage.reasoningTokens > budget.maxReasoningTokens) {
    throw new Error('sandbox exceeded max reasoning tokens')
  }
  if (usage.toolTokens !== undefined && usage.toolTokens > budget.maxToolTokens) {
    throw new Error('sandbox exceeded max tool tokens')
  }
  if (usage.toolCallCount !== undefined && usage.toolCallCount > budget.maxToolCalls) {
    throw new Error('sandbox exceeded max tool calls')
  }
  if (usage.providerCostUsd !== undefined && usage.providerCostUsd > budget.maxProviderCostUsd) {
    throw new Error('sandbox exceeded max provider cost')
  }
}

function finalizeUsage(
  parts: Partial<SandboxUsageReceipt>,
  budget: SandboxExecutionBudget,
): SandboxUsageReceipt {
  const fields = ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount', 'providerCostUsd', 'budgetEnforced'] as const
  if (fields.some((field) => parts[field] === undefined)) {
    throw new Error('sandbox did not provide a complete usage receipt')
  }
  const usage = parts as SandboxUsageReceipt
  for (const field of ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount'] as const) {
    if (!Number.isSafeInteger(usage[field]) || usage[field] < 0) {
      throw new Error(`sandbox usage field ${field} is invalid`)
    }
  }
  if (!Number.isFinite(usage.providerCostUsd) || usage.providerCostUsd < 0) {
    throw new Error('sandbox usage provider cost is invalid')
  }
  if (typeof usage.budgetEnforced !== 'boolean') {
    throw new Error('sandbox usage budget flag is invalid')
  }
  if (!Number.isSafeInteger(
    usage.inputTokens + usage.outputTokens + usage.reasoningTokens + usage.toolTokens,
  )) {
    throw new Error('sandbox usage token total exceeds safe integer range')
  }
  enforceUsageBudget(usage, budget)
  if (!usage.budgetEnforced) throw new Error('sandbox did not enforce the execution budget')
  return usage
}

function completeUsage(
  parts: Partial<SandboxUsageReceipt>,
  reasoningTokens: number,
  toolTokens: number,
  toolCallCount: number,
  userMessage: string,
  outputText: string,
  budget: SandboxExecutionBudget,
  requiresReceipt: boolean,
): SandboxUsageReceipt {
  const observed = withObservedUsage(parts, reasoningTokens, toolTokens, toolCallCount)
  if (
    !requiresReceipt &&
    Object.keys(parts).length === 0 &&
    reasoningTokens === 0 &&
    toolTokens === 0 &&
    toolCallCount === 0
  ) {
    // Preserve the pre-receipt SandboxBox contract for legacy API-key
    // adapters. Durable payment operations must use provider-enforced usage.
    return {
      inputTokens: estimateTokens(userMessage),
      outputTokens: estimateTokens(outputText),
      reasoningTokens: 0,
      toolTokens: 0,
      toolCallCount: 0,
      providerCostUsd: 0,
      budgetEnforced: false,
    }
  }
  return finalizeUsage(observed, budget)
}

function actualX402Amount(
  pricePerTokenUsd: number,
  inputTokens: number,
  outputTokens: number,
  reasoningTokens: number,
  toolTokens: number,
  currencyDecimals = 6,
  providerCostUsd = 0,
): bigint {
  const { numerator, denominator } = decimalFraction(pricePerTokenUsd)
  const scaled = BigInt(inputTokens + outputTokens + reasoningTokens + toolTokens) *
    numerator * 10n ** BigInt(currencyDecimals)
  const tokenAmount = (scaled + denominator - 1n) / denominator
  const provider = providerCostUsd === 0
    ? { numerator: 0n, denominator: 1n }
    : decimalFraction(providerCostUsd)
  const providerScaled = provider.numerator * 10n ** BigInt(currencyDecimals)
  const providerAmount = (providerScaled + provider.denominator - 1n) / provider.denominator
  return tokenAmount > providerAmount ? tokenAmount : providerAmount
}

function truncateUtf8(
  value: string,
  maxBytes: number,
  encoder: TextEncoder,
): { text: string; bytes: number; truncated: boolean } {
  const bytes = encoder.encode(value).byteLength
  if (bytes <= maxBytes) return { text: value, bytes, truncated: false }
  let text = ''
  let used = 0
  for (const character of value) {
    const characterBytes = encoder.encode(character).byteLength
    if (used + characterBytes > maxBytes) break
    text += character
    used += characterBytes
  }
  return { text, bytes: used, truncated: true }
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
