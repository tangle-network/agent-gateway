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

export type PaymentMethod = 'x402' | 'mpp' | 'apikey' | 'none'

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
  /** Production signer verification. Called with the raw SpendAuth payload. Return true if signature is valid. */
  verifySigner?: (payload: Record<string, unknown>) => Promise<boolean>
}

export interface MppConfig {
  /** MPP realm (e.g. "agents.tangle.tools") */
  realm: string
  /** MPP method name (default: "blueprintevm") */
  method?: string
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
  /** Scopes this key is authorized for (e.g. ["chat", "forms"]) */
  scopes?: string[]
  /** Per-key rate limit override (requests per minute). If set, overrides global rate limit. */
  rateLimitPerMinute?: number
  /** Per-key daily limit override. */
  dailyLimit?: number
}

// --- Usage tracking ---

export interface GatewayUsageEvent {
  /**
   * Per-request id (matches `RequestContext.requestId`). Lets
   * `recordUsage` correlate the usage row to the same request that
   * `settlePayment` settles, observability hooks observe, and
   * `onRequestComplete` reports — without re-deriving from a
   * synthetic key. Required field as of 0.4.0; the gateway always has
   * it in scope at the recordUsage call site.
   */
  requestId: string
  agentId: string
  agentSlug: string
  consumerId: string
  paymentMethod: PaymentMethod
  inputTokens: number
  outputTokens: number
  totalCostUsd: number
  ownerEarnedUsd: number
  platformFeeUsd: number
  durationMs: number
}

// --- Sandbox interface ---

export interface SandboxStreamEvent {
  type?: string
  data?: {
    part?: { type?: string; text?: string }
    delta?: string
    finalText?: string
    /**
     * Optional sandbox-side signal that the agent has paused and is waiting
     * for additional input from the caller. The A2A gateway translates this
     * into an `input-required` task status; the caller can then submit a
     * follow-up `message/send` with the same `taskId` to continue. Ignored
     * by the OpenAI-compat path. Carry an optional `prompt` to surface to
     * the caller (rendered as the input-required message body).
     */
    inputRequired?: { prompt?: string }
  }
}

export interface SandboxBox {
  streamPrompt(message: string, opts?: { sessionId?: string; systemPrompt?: string }): AsyncIterable<SandboxStreamEvent>
}

// --- Gateway config ---

export interface GatewayConfig {
  /** Resolve agent metadata by slug. Return null if not found or not published. */
  resolveAgent: (slug: string) => Promise<AgentMeta | null>

  /** Get a sandbox instance for the agent. Called after payment is verified. */
  getSandbox: (agent: AgentMeta) => Promise<SandboxBox>

  /**
   * Optional host authorization hook fired after payment verification
   * and before sandbox resolution. Use it for per-agent allowlists,
   * per-consumer quotas, contract scope checks, and instance ownership.
   */
  authorizeConsumer?: (
    agent: AgentMeta,
    consumer: { method: PaymentMethod; consumerId: string; keyId?: string; requestId: string },
  ) => Promise<{ allow: true } | { allow: false; reason: string; code: string }>

  /** Record a usage event after request completes. */
  recordUsage: (event: GatewayUsageEvent) => Promise<void>

  /** x402 payment configuration */
  x402: X402Config

  /** MPP (Machine Payments Protocol) configuration. If provided, gateway accepts Authorization: Payment headers. */
  mpp?: MppConfig

  /**
   * Verify an API key. Return key info if valid, null if invalid.
   * Default: accepts any `sk_agent_*` key (demo mode).
   */
  verifyApiKey?: (authHeader: string) => Promise<ApiKeyInfo | null>

  /**
   * Settle payment after successful response.
   * For x402: call ShieldedCredits.claimPayment()
   * For API key: deduct from spending limit
   * Default: no-op (demo mode).
   */
  settlePayment?: (payment: PaymentResult, cost: number) => Promise<void>

  /** Base URL for API key purchase links (e.g. "https://film.tangle.tools") */
  baseUrl?: string

  /** Max message length in chars (default: 8000) */
  maxMessageLength?: number

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
  observer?: import('./observer').GatewayObserver

  /**
   * A2A protocol configuration. When set, the gateway exposes the A2A
   * surface alongside its OpenAI-compatible endpoints:
   *   GET  /:slug/.well-known/agent.json   — AgentCard discovery
   *   POST /:slug                          — JSON-RPC 2.0 endpoint
   *     methods: message/send, message/stream, tasks/get, tasks/cancel
   * Auth + rate-limit + injection-filter + authorization all share the
   * same pipeline as the OpenAI-compat path. `taskStore` defaults to
   * `InMemoryTaskStore`; swap in D1/postgres/DO for durable deployments.
   */
  a2a?: {
    /**
     * Where tasks live. Defaults to `InMemoryTaskStore`; swap in
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
     * `pushStore` is set; without it, deliveries fire unsigned and a
     * malicious party that knows the webhook URL can forge deliveries.
     */
    webhookSecret?: string
    /**
     * Optional fetcher override for webhook delivery. Defaults to global
     * `fetch`. Override for tests or to wire a queue-backed sender.
     */
    pushFetcher?: typeof fetch
  }
}

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
