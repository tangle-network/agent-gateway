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
