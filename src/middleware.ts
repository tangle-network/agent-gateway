import { Hono } from 'hono'

import { isChatMessageArray } from './chat-input'
import { createA2AHandlers } from './a2a/handler'
import { InMemoryTaskStore } from './a2a/task-store'
import {
  ApiKeyRequestClaimUnavailableError,
  ApiKeyRequestLimitExceededError,
} from './api-keys'
import {
  type AuthorizedRequest,
  type GatewayState,
  authenticateAndGuard,
  beginPaymentExecution,
  markPaymentExecutionStarted,
  renewPaymentExecution,
  claimPayment,
  buildGatewaySandboxContext,
  dispatchSandboxStreamRich,
  releasePayment,
  releasePaymentAfterFailure,
  settleAndRecord,
} from './dispatch'
import { isAtomicNonceStore, MemoryNonceStore } from './nonce-store'
import { type GatewayObserver, type RequestContext, generateRequestId } from './observer'
import {
  MemoryPaymentRecoveryStore,
  PaymentRecoveryReplayError,
  assertPaymentRecoveryConfig,
} from './payment-recovery'
import { MemoryRateLimitStore, type RateLimitStore } from './rate-limit'
import type {
  ChatCompletionChunk,
  ChatCompletionRequest,
  CreateAgentGatewayConfig,
  GatewayConfig,
} from './types'
import { isApiKeyAuthEnabled, isMppAuthEnabled, isX402AuthEnabled } from './verify'

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
export function createAgentGateway(inputConfig: CreateAgentGatewayConfig) {
  let config: GatewayConfig = inputConfig.x402
    ? inputConfig
    : {
        ...inputConfig,
        x402: { operatorAddress: '', chainId: 0 },
      }
  // Production gateways must verify x402 signatures. Tests and local
  // dev can opt into the explicit demo path.
  if (!isX402AuthEnabled(config) && !config.verifyApiKey) {
    throw new Error(
      'createAgentGateway: verifySigner is required in production unless verifyApiKey is configured; ' +
        'configure x402.verifySigner or verifyApiKey. ' +
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
  if (
    config.x402.authorizePayment &&
    config.x402.paymentProtocolVersion !== 2 &&
    !config.x402.demoMode
  ) {
    throw new Error(
      'createAgentGateway: production x402 version 1 cannot use authorizePayment; ' +
        'use paymentProtocolVersion: 2 with paymentOperations for durable payment ownership',
    )
  }
  if (config.x402.paymentProtocolVersion === 2 &&
      (!config.x402.paymentOperations || config.x402.paymentOperations.protocolVersion !== 2)) {
    throw new Error('createAgentGateway: payment protocol version 2 requires durable payment operations')
  }
  if (config.x402.paymentProtocolVersion === 1 && config.x402.paymentOperations) {
    throw new Error('createAgentGateway: version 1 cannot be combined with version 2 payment operations')
  }
  if (
    config.a2a?.pushStore &&
    !config.x402.demoMode &&
    (!config.a2a.webhookSecret || config.a2a.webhookSecret.trim().length === 0)
  ) {
    throw new Error('createAgentGateway: production A2A push requires a webhookSecret')
  }
  const mppMethod = (config.mpp?.method ?? 'blueprintevm').toLowerCase()
  if (config.mpp?.authenticateCredential !== undefined &&
      typeof config.mpp.authenticateCredential !== 'function') {
    throw new Error('createAgentGateway: mpp.authenticateCredential must be a function')
  }
  if (config.mpp?.verifySigner !== undefined && typeof config.mpp.verifySigner !== 'function') {
    throw new Error('createAgentGateway: mpp.verifySigner must be a function')
  }
  const mppAuthenticator = typeof config.mpp?.authenticateCredential === 'function'
    ? config.mpp.authenticateCredential
    : typeof config.mpp?.verifySigner === 'function'
      ? config.mpp.verifySigner
      : undefined
  if (config.mpp?.charge && config.mpp.charge.protocolVersion !== 1) {
    throw new Error('createAgentGateway: unsupported MPP charge lifecycle version')
  }
  if (
    config.mpp?.charge &&
    mppMethod !== 'blueprintevm' &&
    !mppAuthenticator
  ) {
    throw new Error('createAgentGateway: generic MPP methods require credential authentication')
  }
  if (
    config.mpp &&
    mppMethod !== 'blueprintevm' &&
    mppAuthenticator &&
    !config.mpp.charge
  ) {
    throw new Error('createAgentGateway: generic MPP methods require a charge lifecycle')
  }
  if (config.mpp && mppMethod !== 'blueprintevm' && !mppAuthenticator) {
    throw new Error('createAgentGateway: generic MPP methods require credential authentication')
  }
  if (config.nonceStore && !isAtomicNonceStore(config.nonceStore)) {
    throw new Error('createAgentGateway: durable payment ownership requires an atomic nonce store')
  }
  const needsRecovery = config.x402.paymentProtocolVersion === 2 ||
    (mppMethod !== 'blueprintevm' && config.mpp?.charge !== undefined)
  if (needsRecovery && !config.paymentRecovery) {
    if (!config.x402.demoMode) {
      throw new Error('createAgentGateway: durable payment recovery is required in production')
    }
    config = {
      ...config,
      paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
    }
  }
  if (config.paymentRecovery) assertPaymentRecoveryConfig(config.paymentRecovery)
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

    const paymentMethods: Array<Record<string, unknown>> = []
    if (isX402AuthEnabled(config)) {
      paymentMethods.push({
        type: 'x402',
        operator: config.x402.operatorAddress,
        chain_id: config.x402.chainId,
        credits_contract: config.x402.creditsAddress,
      })
    }
    if (isMppAuthEnabled(config)) {
      paymentMethods.push({
        type: 'mpp',
        realm: config.mpp!.realm,
        method: config.mpp!.method ?? 'blueprintevm',
      })
    }
    if (isApiKeyAuthEnabled(config)) {
      paymentMethods.push({ type: 'api_key', prefix: config.apiKeyPrefix ?? 'sk_agent_' })
    }

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

    let input: unknown
    try {
      input = await c.req.json()
    } catch {
      return c.json({ error: { message: 'Invalid JSON', type: 'invalid_request' } }, 400)
    }
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      return c.json({ error: { message: 'JSON object required', type: 'invalid_request' } }, 400)
    }
    const body = input as Partial<ChatCompletionRequest>
    if (!isChatMessageArray(body.messages)) {
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
    if (body.stream !== undefined && typeof body.stream !== 'boolean') {
      return c.json({ error: { message: 'stream must be a boolean', type: 'invalid_request' } }, 400)
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
    } catch (error) {
      if (error instanceof ApiKeyRequestLimitExceededError) {
        const resetAt = error.claim.reason === 'daily'
          ? error.claim.dailyResetAt
          : error.claim.minuteResetAt
        const retryAfterSeconds = Math.max(1, Math.ceil((resetAt - Date.now()) / 1_000))
        await obs?.onRateLimited?.(
          {
            requestId: authz.requestId,
            agentSlug: authz.agent.slug,
            startMs: authz.startMs,
          },
          { consumerId: authz.consumerId, retryAfterSeconds },
        )
        return c.json(
          {
            error: {
              message: `API key ${error.claim.reason} request limit exceeded`,
              type: 'rate_limit_error',
              code: `api_key_${error.claim.reason}_limit_exceeded`,
              retry_after: retryAfterSeconds,
            },
          },
          {
            status: 429,
            headers: {
              'Retry-After': String(retryAfterSeconds),
              'X-Request-Id': authz.requestId,
              'X-RateLimit-Remaining': String(error.claim.minuteRemaining),
              'X-RateLimit-Daily-Remaining': String(error.claim.dailyRemaining),
            },
          },
        )
      }
      if (error instanceof ApiKeyRequestClaimUnavailableError) {
        return c.json(
          {
            error: {
              message: 'API key request limits are unavailable',
              type: 'server_error',
              code: error.code,
            },
          },
          { status: 503, headers: { 'X-Request-Id': authz.requestId } },
        )
      }
      const replayedGenericMpp = error instanceof PaymentRecoveryReplayError &&
        authz.paymentMethod === 'mpp' &&
        authz.mppMethod !== 'blueprintevm'
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
        {
          method: authz.paymentMethod,
          code: replayedGenericMpp ? 'invalid_mpp_credential' : 'payment_authorization_failed',
          httpStatus: replayedGenericMpp ? 401 : 402,
        },
      )
      const status = replayedGenericMpp ? 401 : 402
      return c.json(
        {
          error: {
            message: replayedGenericMpp ? 'Invalid Payment credential' : 'Payment authorization failed',
            type: replayedGenericMpp ? 'authentication_error' : 'payment_required',
            code: replayedGenericMpp ? 'invalid_mpp_credential' : 'payment_authorization_failed',
          },
        },
        replayedGenericMpp
          ? {
              status,
              headers: {
                'WWW-Authenticate': `Payment realm="${config.mpp!.realm}", method="${config.mpp!.method ?? 'blueprintevm'}"`,
                'X-Request-Id': authz.requestId,
              },
            }
          : { status, headers: { 'X-Payment-Required': 'spendauth', 'X-Request-Id': authz.requestId } },
      )
    }

    return streamChatCompletions(c, authz, config, obs)
  })

  // --- A2A protocol surface (Google Agent-to-Agent, JSON-RPC 2.0 + AgentCard) ---
  // Mounted alongside the OpenAI-compat routes so a single agent speaks both.
  // Both surfaces share authenticateAndGuard + dispatchSandboxStream +
  // settleAndRecord, so every security and billing guarantee applies uniformly
  // regardless of which protocol the caller used.
  const pushStore = config.a2a?.pushStore
  try {
    // Do not create process-local state for production A2A.
    if (!config.x402.demoMode && !config.a2a?.taskStore) {
      throw new Error('A2A production requires an explicitly configured atomic task store')
    }
    const taskStore = config.a2a?.taskStore ?? new InMemoryTaskStore()
    const a2a = createA2AHandlers({ config, state, taskStore, pushStore })
    gw.get('/:slug/.well-known/agent.json', a2a.handleAgentCard)
    gw.post('/:slug', a2a.handleJsonRpc)
  } catch (error) {
    // A missing or older custom store must not take down the OpenAI surface.
    // Keep A2A unavailable until its owner supplies atomic methods.
    console.error(
      '[agent-gateway] A2A is unavailable until its task store is configured with atomic methods:',
      error instanceof Error ? error.message : String(error),
    )
    const unavailable = (c: import('hono').Context) => c.json(
      { error: 'A2A task persistence is not configured for concurrent workers' },
      503,
    )
    gw.get('/:slug/.well-known/agent.json', unavailable)
    gw.post('/:slug', unavailable)
  }

  return gw
}

// Consumers that bind the gateway through a package boundary can fail closed
// when an old binary ignores the version 2 operation contract.
Object.assign(createAgentGateway, { paymentProtocolVersion: 2 as const })

/** Public package-boundary version marker for durable payment operations. */
export namespace createAgentGateway {
  export const paymentProtocolVersion = 2 as const
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
  const requestSignal = c.req.raw.signal
  const abortController = new AbortController()
  const abortFromRequest = () => abortController.abort()
  if (requestSignal.aborted) abortFromRequest()
  else requestSignal.addEventListener('abort', abortFromRequest, { once: true })
  const ctx: RequestContext = {
    requestId,
    agentSlug: agent.slug,
    startMs: authz.startMs,
  }

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder()
      const sendChunk = (delta: string, role?: string) => {
        if (controller.desiredSize === null) return
        outputText += delta
        const chunk: ChatCompletionChunk = {
          id: `chatcmpl-${Date.now()}`,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: agent.slug,
          choices: [{ index: 0, delta: role ? { role } : { content: delta }, finish_reason: null }],
        }
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`))
      }
      const sendKeepalive = () => {
        if (controller.desiredSize === null) return
        controller.enqueue(encoder.encode(': keep-alive\n\n'))
      }
      const keepaliveTimer = setInterval(sendKeepalive, 15_000)

      try {
        sendChunk('', 'assistant')
        for await (const event of dispatchSandboxStreamRich(
          agent,
          userMessage,
          consumerId,
          config,
          abortController.signal,
          authz.threadId,
          maxOutputTokens,
          () => beginPaymentExecution(authz, config),
          authz.paymentOperation !== undefined || authz.mppChargeOperation !== undefined,
          async () => {
            if (authz.paymentRecoveryId) workObserved = true
            await markPaymentExecutionStarted(authz, config)
          },
          authz.executionBudget.maxInputTokens,
          () => renewPaymentExecution(authz, config),
          buildGatewaySandboxContext(authz),
        )) {
          if (event.kind === 'text') {
            sendChunk(event.delta)
            workObserved = true
          }
          if (event.kind === 'activity') workObserved = true
          if (event.kind === 'usage') usage = event.usage
        }

        if (!usage) throw new Error('sandbox did not provide a usage receipt')

        await settleAndRecord(
          agent,
          authz,
          usage,
          config,
          obs,
        )

        const done: ChatCompletionChunk = {
          id: `chatcmpl-${Date.now()}`,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: agent.slug,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        }
        if (controller.desiredSize !== null) {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(done)}\n\n`))
          controller.enqueue(encoder.encode('data: [DONE]\n\n'))
        }
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
        if (!abortController.signal.aborted && controller.desiredSize !== null) {
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({ error: { message: safeMessage, type: 'server_error' } })}\n\n`,
            ),
          )
        }
      } finally {
        clearInterval(keepaliveTimer)
        requestSignal.removeEventListener('abort', abortFromRequest)
        if (controller.desiredSize !== null) controller.close()
      }
    },
    cancel() {
      abortController.abort()
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Request-Id': requestId,
      'X-Agent-Slug': agent.slug,
      'X-Agent-Hosting': agent.sandboxEndpoint ? 'sovereign' : 'centralized',
      ...(authz.threadId ? { 'X-Tangle-Thread-Id': authz.threadId } : {}),
      'X-Payment-Method': paymentMethod,
      'X-Payment-Settled': paymentMethod === 'x402' || authz.paymentOperation ? 'pending' : 'true',
      ...(authz.mppChargeOperation
        ? { 'Payment-Receipt': authz.mppChargeOperation.receipt }
        : {}),
      ...(authz.paymentRecoveryId
        ? { 'X-Payment-Operation-Id': authz.paymentRecoveryId }
        : {}),
      ...(rateLimitRemaining !== undefined
        ? { 'X-RateLimit-Remaining': String(rateLimitRemaining) }
        : {}),
    },
  })
}
