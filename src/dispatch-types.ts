import type { MppChargeOperation } from './mpp-payment'
import type { NonceStore } from './nonce-store'
import type { GatewayObserver } from './observer'
import type { RateLimitStore } from './rate-limit'
import type { PaymentOperation } from './payment-operations'
import type { PaymentSettlementBasis } from './payment-recovery'
import type {
  AgentMeta,
  ApiKeyInfo,
  PaymentMethod,
  SandboxExecutionBudget,
  SandboxUsageReceipt,
} from './types'

/** Long-lived state shared by all handlers created for one gateway. */
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

/** Successful output from the request authorization pipeline. */
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
  mppMethod?: string
  /** Live generic MPP credential. Never write it to the recovery store. */
  mppCredential?: string
  /** Stable method identity. Persist only its digest. */
  mppPaymentIdentity?: string
  mppChargeOperation?: MppChargeOperation
  paymentOperation?: PaymentOperation
  paymentOperationAcquired?: boolean
  paymentRecoveryId?: string
  /** Unique ownership fence for live or recovery transitions. */
  paymentRecoveryFence?: string
}

export interface PaymentClaimHooks {
  /** Persist the recovery identity before the provider can mutate payment state. */
  onRecoveryPrepared?: (recoveryId: string) => Promise<void>
}

export type A2ADispatchEvent =
  | { kind: 'text'; delta: string }
  | { kind: 'input-required'; prompt?: string }
  | { kind: 'activity' }
  | { kind: 'usage'; usage: SandboxUsageReceipt }

export interface SettleAndRecordOptions {
  /** Skip attribution after a durable finalization marker confirms it ran. */
  usageAlreadyRecorded?: boolean
  /** Skip provider settlement after an authoritative read found it settled. */
  paymentAlreadySettled?: boolean
  /** Persist the recovery marker after attribution succeeds. */
  onUsageRecorded?: () => Promise<void>
  /** Recovery uses the original quoted ceiling when no receipt arrives. */
  settlementBasis?: PaymentSettlementBasis
  /** Exact base-unit charge selected by the recovery policy. */
  paymentAmount?: bigint
}
