export type PaymentMethod = 'x402' | 'mpp' | 'apikey' | 'none'

export interface SandboxExecutionBudget {
  maxInputTokens: number
  maxOutputTokens: number
  maxReasoningTokens: number
  maxToolTokens: number
  maxToolCalls: number
  maxProviderCostUsd: number
}

export interface SandboxUsageReceipt {
  inputTokens: number
  outputTokens: number
  reasoningTokens: number
  toolTokens: number
  toolCallCount: number
  providerCostUsd: number
  /** True only when the provider/adapter enforced every supplied budget. */
  budgetEnforced: boolean
}

export type PaymentSettlementBasis = 'usage-receipt' | 'quoted-ceiling'

export interface GatewayUsageEvent {
  /** Correlates usage, settlement, and observer records for one request. */
  requestId: string
  agentId: string
  agentSlug: string
  consumerId: string
  paymentMethod: PaymentMethod
  inputTokens: number
  outputTokens: number
  /** Optional in 0.7.2 so 0.7.1 event constructors remain source-compatible. */
  reasoningTokens?: number
  /** Optional in 0.7.2 so 0.7.1 event constructors remain source-compatible. */
  toolTokens?: number
  /** Optional in 0.7.2 so 0.7.1 event constructors remain source-compatible. */
  toolCallCount?: number
  /** Optional in 0.7.2 so 0.7.1 event constructors remain source-compatible. */
  providerCostUsd?: number
  totalCostUsd: number
  ownerEarnedUsd: number
  platformFeeUsd: number
  durationMs: number
  /** Exact receipt in normal operation; quoted ceiling only after receipt timeout. */
  settlementBasis?: PaymentSettlementBasis
}
