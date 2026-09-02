import type {
  PaymentAuthorizationContext,
  PaymentOperation,
  PaymentOperations,
} from './payment-operations'
import type { MppAuthenticatedCredential, MppChargeLifecycle } from './mpp-payment'
import type { PaymentRecoveryConfig } from './payment-recovery'
import type { GatewayObserver } from './observer-types'
import type {
  GatewayUsageEvent,
  PaymentMethod,
  SandboxExecutionBudget,
  SandboxUsageReceipt,
} from './payment-types'

export type {
  GatewayUsageEvent,
  PaymentMethod,
  SandboxExecutionBudget,
  SandboxUsageReceipt,
} from './payment-types'

// --- Agent resolution ---

export interface AgentMeta {
  /** Unique agent identifier (workspace ID, session ID, etc.) */
  id: string
  /** Owner/creator user ID */
  ownerId: string
  /** Public URL slug */
  slug: string
  /** System prompt for the agent (injected before consumer messages) */
  systemPrompt?: string
  /** Per-token price in USD (default: 0.00002) */
  pricePerTokenUsd: number
  /** Platform fee as decimal 0-1 (default: 0.20 = 20%) */
  platformFeePercent: number
  /** Remote operator endpoint for sovereignty mode (null = centralized) */
  sandboxEndpoint: string | null
  /** Sandbox ID on remote operator */
  remoteSandboxId: string | null
  /** PASETO bearer token for remote operator auth */
  remoteBearerToken: string | null
  /** Whether agent is published and accepting requests */
  enabled: boolean
  /**
   * CLI harness backend that runs this agent inside the sandbox sidecar.
   *
   * When set, the host's `getSandbox()` SHOULD return a `SandboxBox`
   * whose `streamPrompt` POSTs to the sidecar's
   * `POST /agent/invoke/chat/completions` endpoint with
   * `model: "<harness>/<harnessModel>"` — that endpoint runs the
   * harness against the sandbox workspace and streams OpenAI-shape
   * `chat.completion.chunk` frames back.
   *
   * When unset (legacy / template mode), the host's `streamPrompt`
   * falls back to the template's own `/api/chat/completions` (proxied
   * via the sidecar's `/agent/invoke`).
   *
   * Known harnesses (registered in agent-dev-container's
   * cli-agent-bindings.ts): opencode, claude-code, codex, kimi-code,
   * amp, factory-droids, pi, hermes, openclaw, forge, acp, cursor.
   * Aliases the sidecar canonicalizes: claude → claude-code,
   * kimi → kimi-code, factory → factory-droids.
   */
  harness?: string
  /**
   * Model identifier to pass after the harness in the
   * `<harness>/<model>` slash form. Format is harness-specific:
   *   claude-code:   "sonnet", "opus", or a versioned id like
   *                  "claude-sonnet-4-20250514"
   *   opencode:      "anthropic/claude-sonnet-4-5", "openai/gpt-4o", …
   *                  (opencode embeds provider before model)
   *   codex:         "gpt-5-codex"
   *   kimi-code:     "kimi-for-coding"
   *
   * Only meaningful when `harness` is set; ignored otherwise.
   */
  harnessModel?: string
  /**
   * Optional human description surfaced in the A2A Agent Card. Defaults to
   * `"{slug} agent"` when absent.
   */
  description?: string
  /**
   * Optional A2A skill descriptors. Each entry advertises what the agent
   * can do so non-Tangle A2A clients can select agents by capability. When
   * absent, the gateway synthesizes a single default `chat` skill from
   * `slug` + `description`.
   */
  skills?: import('./a2a/types').AgentSkill[]
}

// --- Payment ---

export interface X402Config {
  /** Ethereum operator address for SpendAuth verification */
  operatorAddress: string
  /** Blockchain network ID (default: 3799) */
  chainId: number
  /** ShieldedCredits contract address */
  creditsAddress?: string
  /** RPC URL for on-chain verification (optional, demo mode skips this) */
  rpcUrl?: string
  /** Demo mode: skip signature verification (default: false). NEVER enable in production. */
  demoMode?: boolean
  /** Protocol version for new durable payment operations. Production version 1 is read-only. */
  paymentProtocolVersion?: 1 | 2
  /**
   * Production signature verification. This callback must not reserve, claim,
   * or mutate payment state.
   */
  verifySigner?: (
    payload: Record<string, unknown>,
    context?: { protocolVersion: 1 | 2; requestId?: string },
  ) => Promise<boolean>
  /**
   * Claim the verified payment after all request checks pass and immediately
   * before sandbox work starts. Version 2 returns durable operation ownership.
   * A boolean return is the version 1 demo-only compatibility path.
   * Production version 1 must omit this callback because it has no durable
   * provider operation or recovery identity.
   */
  authorizePayment?: (
    payload: Record<string, unknown>,
    context: PaymentAuthorizationContext,
  ) => Promise<boolean | PaymentOperation>
  /** Version 2 operation store. It owns claim, settle, release, and reclaim. */
  paymentOperations?: PaymentOperations
  /**
   * Number of base-unit decimals used by the payment token. Defaults to 6.
   * The gateway uses this value to reject a payment that cannot cover the
   * request's maximum token charge before it calls `verifySigner`.
   */
  currencyDecimals?: number
}

export interface MppConfig {
  /** MPP realm (e.g. "agents.tangle.tools") */
  realm: string
  /** MPP method name (default: "blueprintevm") */
  method?: string
  /**
   * Pure credential authentication. Return stable method-owned identity, or null.
   * This callback must not consume a credential, create a processor object,
   * reserve funds, confirm payment, or perform any other financial mutation.
   */
  authenticateCredential?: (
    payload: Record<string, unknown>,
    context: { method: string; credential: string },
  ) => Promise<MppAuthenticatedCredential | null>
  /**
   * @deprecated Use authenticateCredential and return a stable payment identity.
   * This 0.7.1 callback remains supported through an explicit compatibility adapter.
   */
  verifySigner?: (
    payload: Record<string, unknown>,
    context: { method: string; credential: string },
  ) => Promise<string | null>
  /** Required immediate-charge lifecycle for every non-BlueprinTEVM method. */
  charge?: MppChargeLifecycle
}

export interface PaymentResult {
  method: PaymentMethod
  consumerId: string
  /**
   * Per-request id (matches `RequestContext.requestId` from the
   * Observer pattern). Threaded into `settlePayment` so callers can
   * attribute revenue deterministically per-request without scanning
   * a FIFO queue keyed by consumerId — when the same consumer is
   * paying for two concurrent requests against agents A and B, FIFO
   * misroutes one. With `requestId` the call site can write a
   * settlement row keyed exactly to the request that earned it.
   */
  requestId: string
}

export interface ApiKeyInfo {
  keyId: string
  consumerId: string
  /** Server-side owner identity for routing a private workspace request. */
  ownerId?: string
  /** Scopes this key is authorized for (e.g. ["chat", "forms"]) */
  scopes?: string[]
  /** Per-key rate limit override (requests per minute). If set, overrides global rate limit. */
  rateLimitPerMinute?: number
  /** Per-key daily limit override. */
  dailyLimit?: number
}

// --- Sandbox interface ---

export interface SandboxStreamEvent {
  /** Runtime event cursor used to resume a detached execution. */
  id?: string
  type?: string
  data?: {
    part?: { type?: string; text?: string }
    delta?: string
    finalText?: string
    /** Structured failure fields emitted by terminal `error` events. */
    code?: string
    message?: string
    details?: Record<string, unknown>
    /**
     * Optional sandbox-side signal that the agent has paused and is waiting
     * for additional input from the caller. The A2A gateway translates this
     * into an `input-required` task status; the caller can then submit a
     * follow-up `message/send` with the same `taskId` to continue. Ignored
     * by the OpenAI-compat path. Carry an optional `prompt` to surface to
     * the caller (rendered as the input-required message body).
     */
    inputRequired?: { prompt?: string }
    /** Provider receipt fields. Version 2 operations require every field. */
    usage?: Partial<SandboxUsageReceipt>
    /** Tool or reasoning events may carry hidden usage without visible text. */
    tool?: { name?: string; inputTokens?: number; outputTokens?: number }
    reasoning?: { tokens?: number }
  }
}

export type SandboxRunControlRef = { environmentId: string; sessionId: string; executionId: string }

interface SandboxDispatchResult {
  sessionId: string
  executionId?: string
  runControlRef?: SandboxRunControlRef
  dispatched?: boolean
}

export interface SandboxPromptResult {
  success: boolean
  status: string
  executionId?: string
  response?: string
  error?: string
  question?: string
  usage?: { inputTokens: number; outputTokens: number; cacheReadTokens?: number; cacheWriteTokens?: number }
  costUsd?: number
}

interface SandboxDurableSession {
  events: (opts?: { since?: string; executionId?: string; signal?: AbortSignal }) => AsyncIterable<SandboxStreamEvent>
  result: (opts?: { executionId?: string }) => Promise<SandboxPromptResult>
  interrupt: (opts?: { executionId?: string }) => Promise<{ cancelled: boolean }>
}

export interface SandboxBox {
  /** Stable sandbox/environment id from the provider, when available. */
  id?: string
  streamPrompt(
    message: string,
    opts?: {
      sessionId?: string
      systemPrompt?: string
      maxOutputTokens?: number
      executionBudget?: SandboxExecutionBudget
      signal?: AbortSignal
    },
  ): AsyncIterable<SandboxStreamEvent>
  /** Start one idempotent run and detach it from the caller's stream. */
  dispatchPrompt?: (
    message: string,
    opts?: {
      sessionId?: string
      turnId?: string
      systemPrompt?: string
      maxOutputTokens?: number
      executionBudget?: SandboxExecutionBudget
      signal?: AbortSignal
    },
  ) => Promise<SandboxDispatchResult>
  /** Resolve a lazy reference for one durable session. */
  session?: (id: string) => SandboxDurableSession
}

/** Authenticated request identity supplied when the host resolves a sandbox. */
export interface GatewaySandboxContext {
  consumerId: string
  paymentMethod: PaymentMethod
  keyInfo: ApiKeyInfo | null
  requestId: string
  messages: ChatMessage[]
  /** Stable UI conversation id when `conversationMode` is `thread`. */
  threadId?: string
}

// --- Gateway config ---

export interface GatewayConfig {
  /** Resolve agent metadata by slug. Return null if not found or not published. */
  resolveAgent: (slug: string) => Promise<AgentMeta | null>

  /**
   * Get the agent execution adapter after payment is verified.
   * Hosts that use agent-app can drive their normal persisted chat route here.
   */
  getSandbox: (agent: AgentMeta, context?: GatewaySandboxContext) => Promise<SandboxBox>

  /**
   * Optional host authorization hook fired after payment verification
   * and before sandbox resolution. Use it for per-agent allowlists,
   * per-consumer quotas, contract scope checks, and instance ownership.
   */
  authorizeConsumer?: (
    agent: AgentMeta,
    consumer: {
      method: PaymentMethod
      consumerId: string
      keyId?: string
      /** Verified server-side API-key owner. Never accept this from the request body. */
      ownerId?: string
      requestId: string
      /** Requested stable conversation id, after syntax validation. */
      threadId?: string
    },
  ) => Promise<{ allow: true } | { allow: false; reason: string; code: string }>

  /**
   * Record a usage event after request completes.
   * The implementation must atomically upsert by requestId and return
   * success when the row already exists. Recovery may retry after an
   * acknowledgement is lost, so one request ID must produce one usage row.
   */
  recordUsage: (event: GatewayUsageEvent) => Promise<void>

  /** x402 payment configuration */
  x402: X402Config

  /** MPP (Machine Payments Protocol) configuration. It is advertised only when a production verifier or explicit demo mode is available. */
  mpp?: MppConfig

  /**
   * Durable payment recovery outbox. Production payment protocol version 2
   * and generic MPP charge methods require this configuration.
   */
  paymentRecovery?: PaymentRecoveryConfig

  /**
   * Verify an API key. Return key info if valid, null if invalid.
   * In explicit x402 demo mode, the built-in verifier accepts `sk_agent_*` keys.
   * Production gateways must provide this callback.
   */
  verifyApiKey?: (authHeader: string) => Promise<ApiKeyInfo | null>

  /**
   * Settle a legacy payment after usage attribution is recorded.
   * Version 2 x402 operations use `x402.paymentOperations` instead.
   * Production x402 version 1 rejects this callback before nonce claim.
   * For API keys, deduct from the spending limit.
   * Default: no-op in explicit demo mode.
   */
  settlePayment?: (payment: PaymentResult, cost: number) => Promise<void>

  /** Base URL for API key purchase links (e.g. "https://film.tangle.tools") */
  baseUrl?: string

  /** Public API key prefix shown by discovery. Defaults to `sk_agent_`. */
  apiKeyPrefix?: string

  /**
   * `consumer` keeps the historical session per API consumer.
   * `thread` accepts `X-Tangle-Thread-Id` or creates one per request and returns
   * it in the response, so a host can display the same conversation.
   */
  conversationMode?: 'consumer' | 'thread'

  /** Max message length in chars (default: 8000) */
  maxMessageLength?: number

  /** Maximum output token request the gateway accepts. Defaults to 4096. */
  maxOutputTokens?: number

  /** Output token limit used when a request omits `max_tokens`. Defaults to 1024. */
  defaultOutputTokens?: number

  /**
   * Return a safe upper bound for the complete provider input.
   * Include system, chat framing, retained history, tools, harness, and workspace context.
   */
  inputTokenBound?: (input: {
    agent: AgentMeta
    messages: ChatMessage[]
  }) => number

  /** Hidden provider spend limits included in the pre-execution payment quote. */
  executionBudget?: {
    maxReasoningTokens?: number
    maxToolTokens?: number
    maxToolCalls?: number
    maxProviderCostUsd?: number
  }

  /** Required scope for chat endpoint (default: "chat"). API keys must include this scope. */
  requiredScope?: string

  /** Block requests with detected injection patterns (default: false — log only) */
  blockInjection?: boolean

  /** Rate limiting config. Default: 60 requests per 60 seconds per consumer. */
  rateLimit?: { limit: number; windowSeconds: number }

  /** Custom rate limit store (default: in-memory). Use KV-backed for Workers. */
  rateLimitStore?: import('./rate-limit').RateLimitStore

  /** Nonce replay protection store (default: in-memory). Rejects reused x402 nonces. */
  nonceStore?: import('./nonce-store').NonceStore

  /**
   * Observability hook. When set, the gateway emits typed events for request
   * lifecycle, auth outcomes, rate limits, injection detection, usage, errors,
   * and settlement failures. See ./observer.ts for the interface and
   * ConsoleObserver / CompositeObserver implementations.
   */
  observer?: GatewayObserver

  /**
   * A2A protocol configuration. Explicit demo mode uses an in-memory task
   * store by default. Production must provide an atomic durable task store;
   * without one, A2A returns 503 while the OpenAI surface remains available.
   * Set this object to provide durable storage or push:
   *   GET  /:slug/.well-known/agent.json   — AgentCard discovery
   *   POST /:slug                          — JSON-RPC 2.0 endpoint
   *     methods: message/send, message/stream, tasks/get, tasks/cancel
   * Auth + rate-limit + injection-filter + authorization all share the
   * same pipeline as the OpenAI-compat path. Demo mode defaults to
   * `InMemoryTaskStore`; production must configure D1/postgres/DO storage.
  */
  a2a?: {
    /**
     * Authorize reads, cancellation, resubscription, and push configuration
     * for an existing task. Production control methods fail closed when this
     * hook is absent; explicit demo mode permits local tests.
     */
    authorizeTaskAccess?: (
      task: import('./a2a/types').Task,
      context: {
        method: string
        agentSlug: string
        authorization: string
        paymentSignature: string
      },
    ) => Promise<boolean>
    /**
     * Where tasks live. Required in production and must implement the
     * atomic methods. Demo mode defaults to `InMemoryTaskStore`; swap in
     * `SqlTaskStore` (D1, postgres, sqlite, libSQL) for durability across
     * gateway restarts.
     */
    taskStore?: import('./a2a/task-store').TaskStore
    /**
     * Where push notification configs live. When set, the gateway advertises
     * `capabilities.pushNotifications: true` and exposes the four
     * `tasks/pushNotificationConfig/*` JSON-RPC methods. Defaults to
     * undefined (push support disabled), so the agent card honestly reflects
     * what the gateway will actually do.
     */
    pushStore?: import('./a2a/push-notifications').PushNotificationStore
    /**
     * Shared HMAC secret used to sign webhook deliveries (`X-A2A-Signature:
     * sha256=<hex>`). The consumer's webhook verifies the body against this
     * secret to confirm the call originated from this gateway. Required when
     * `pushStore` is set in production. Explicit demo mode may omit it for
     * local tests; production deliveries never run unsigned.
     */
    webhookSecret?: string
    /**
     * Optional fetcher override for webhook delivery. Defaults to global
     * `fetch`. Override for tests or to wire a queue-backed sender.
     */
    pushFetcher?: typeof fetch
    /**
     * DNS-aware policy for push destinations. Required when production
     * push delivery is enabled so private DNS names cannot receive task data.
     */
    pushUrlValidator?: (url: URL) => boolean | Promise<boolean>
  }
}

/** API-key-only gateway input. Payment transports stay disabled. */
export type ApiKeyGatewayConfig = Omit<GatewayConfig, 'mpp' | 'verifyApiKey' | 'x402'> & {
  mpp?: never
  verifyApiKey: NonNullable<GatewayConfig['verifyApiKey']>
  x402?: never
}

/** Configuration accepted by `createAgentGateway`. */
export type CreateAgentGatewayConfig = GatewayConfig | ApiKeyGatewayConfig

// --- Chat completion types (OpenAI-compatible) ---

export interface ChatMessage {
  role: 'system' | 'user' | 'assistant' | 'tool'
  content: string
}

export interface ChatCompletionRequest {
  model?: string
  messages: ChatMessage[]
  stream?: boolean
  temperature?: number
  max_tokens?: number
}

export interface ChatCompletionChunk {
  id: string
  object: 'chat.completion.chunk'
  created: number
  model: string
  choices: Array<{
    index: number
    delta: { content?: string; role?: string }
    finish_reason: string | null
  }>
}
