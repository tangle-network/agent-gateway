export type PaymentMethod = 'x402' | 'mpp' | 'apikey' | 'none'

/** Inclusive totals bound all calls, retries, and tool messages within one execution. */
export interface SandboxExecutionBudget {
  maxInputTokens: number
  maxOutputTokens: number
  /** Optional subset cap. Omission does not add a separate reasoning budget. */
  maxReasoningTokens?: number
  /** Optional subset cap for tool-attributed tokens already included in input/output. */
  maxToolTokens?: number
  maxToolCalls: number
  maxProviderCostUsd: number
}

/** Input/output are inclusive provider totals. Optional reasoning/tool counts are subsets, never extra charges. */
export interface SandboxUsageReceipt {
  inputTokens: number
  outputTokens: number
  reasoningTokens?: number
  toolTokens?: number
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
  /** Optional measured detail. Omitted when the adapter cannot report it. */
  reasoningTokens?: number
  /** Optional measured detail. Omitted when the adapter cannot report it. */
  toolTokens?: number
  /** Optional measured detail. Omitted when the adapter cannot report it. */
  toolCallCount?: number
  /** Optional measured detail. Omitted when the adapter cannot report it. */
  providerCostUsd?: number
  totalCostUsd: number
  ownerEarnedUsd: number
  platformFeeUsd: number
  durationMs: number
  /** Exact receipt in normal operation; quoted ceiling only after receipt timeout. */
  settlementBasis?: PaymentSettlementBasis
}
