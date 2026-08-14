import type { SandboxUsageReceipt } from './types'

/** Version negotiated by gateways that use durable payment operations. */
export const PAYMENT_PROTOCOL_VERSION = 2 as const

export type PaymentOperationState =
  | 'claiming'
  | 'claimed'
  | 'settling'
  | 'settled'
  | 'releasing'
  | 'released'
  | 'reclaimable'
  | 'reclaimed'

/** Durable ownership of one signed payment authorization. */
export interface PaymentOperation {
  protocolVersion: typeof PAYMENT_PROTOCOL_VERSION
  operationId: string
  nonceKey: string
  authorizationId: string
  reservedAmount: bigint
  settledAmount: bigint
  refundAmount: bigint
  expiresAt: number
  state: PaymentOperationState
}

export interface PaymentAuthorizationContext {
  requestId: string
  agentId: string
  requiredAmount: bigint
  maxOutputTokens: number
  executionBudget: {
    maxInputTokens: number
    maxOutputTokens: number
    maxReasoningTokens: number
    maxToolTokens: number
    maxToolCalls: number
    maxProviderCostUsd: number
  }
}

export interface PaymentSettlementInput {
  amount: bigint
  totalCostUsd: number
  usage: SandboxUsageReceipt
}

/**
 * One payment lifecycle shared by every payment-backed gateway surface.
 * Implementations must persist the operation before external side effects.
 */
export interface PaymentOperations {
  readonly protocolVersion: typeof PAYMENT_PROTOCOL_VERSION
  claimPayment(
    payload: Record<string, unknown>,
    context: PaymentAuthorizationContext,
  ): Promise<PaymentOperation>
  settlePayment(
    operation: PaymentOperation,
    input: PaymentSettlementInput,
  ): Promise<PaymentOperation>
  releasePayment(operation: PaymentOperation, reason: string): Promise<PaymentOperation>
  reclaimPayment(operationId: string): Promise<PaymentOperation>
}

export interface MemoryPaymentOperationsOptions {
  now?: () => number
  onClaim?: (operation: PaymentOperation) => Promise<void>
  onSettle?: (operation: PaymentOperation, input: PaymentSettlementInput) => Promise<void>
  onRelease?: (operation: PaymentOperation, reason: string) => Promise<void>
  onReclaim?: (operation: PaymentOperation) => Promise<void>
}

/** Small atomic implementation used by single-process deployments and tests. */
export class MemoryPaymentOperations implements PaymentOperations {
  readonly protocolVersion = PAYMENT_PROTOCOL_VERSION
  private readonly operations = new Map<string, PaymentOperation>()
  private readonly settleFlights = new Map<string, Promise<PaymentOperation>>()
  private readonly releaseFlights = new Map<string, Promise<PaymentOperation>>()
  private readonly reclaimFlights = new Map<string, Promise<PaymentOperation>>()
  private readonly now: () => number

  constructor(private readonly options: MemoryPaymentOperationsOptions = {}) {
    if (options.onClaim && !options.onReclaim) {
      throw new Error('onReclaim is required when onClaim can reserve external funds')
    }
    this.now = options.now ?? (() => Math.floor(Date.now() / 1000))
  }

  async claimPayment(
    payload: Record<string, unknown>,
    context: PaymentAuthorizationContext,
  ): Promise<PaymentOperation> {
    const nonceKey = paymentNonceKey(payload)
    const operationId = `x402:${nonceKey}`
    const existing = this.operations.get(operationId)
    if (existing) {
      if (existing.state === 'claiming') throw new Error('payment operation is being recovered')
      throw new Error('payment operation was already claimed')
    }

    const reservedAmount = unsignedAmount(payload.amount)
    if (reservedAmount < context.requiredAmount) {
      throw new Error('payment authorization is below the request ceiling')
    }
    const expiresAt = unsignedAmount(payload.expiry, 'expiry')
    if (expiresAt > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('payment expiry is too large')
    if (Number(expiresAt) <= this.now()) throw new Error('payment authorization has expired')

    const operation: PaymentOperation = {
      protocolVersion: PAYMENT_PROTOCOL_VERSION,
      operationId,
      nonceKey,
      authorizationId: typeof payload.authHash === 'string' ? payload.authHash : operationId,
      reservedAmount,
      settledAmount: 0n,
      refundAmount: reservedAmount,
      expiresAt: Number(expiresAt),
      state: 'claiming',
    }
    // The map write is synchronous. No second caller can observe a free nonce
    // between the uniqueness check and ownership write.
    this.operations.set(operationId, operation)
    try {
      await this.options.onClaim?.(operation)
      const claimed = { ...operation, state: 'claimed' as const }
      this.operations.set(operationId, claimed)
      return claimed
    } catch (error) {
      // Keep the durable `claiming` row. The external authorization may have
      // committed before its acknowledgement was lost, so expiry recovery
      // must own the decision to reclaim it.
      throw error
    }
  }

  async settlePayment(
    operation: PaymentOperation,
    input: PaymentSettlementInput,
  ): Promise<PaymentOperation> {
    const current = this.requireCurrent(operation)
    if (current.state === 'settled') {
      if (current.settledAmount !== input.amount) throw new Error('payment operation was settled twice')
      return current
    }
    if (current.state === 'settling') {
      if (current.settledAmount !== input.amount) throw new Error('payment operation has a different pending settlement')
      const flight = this.settleFlights.get(current.operationId)
      if (flight) return flight
      const reclaimFlight = this.reclaimFlights.get(current.operationId)
      if (reclaimFlight) return reclaimFlight
      const recovery = this.recoverSettlement(current, input)
      this.settleFlights.set(current.operationId, recovery)
      try {
        return await recovery
      } finally {
        if (this.settleFlights.get(current.operationId) === recovery) {
          this.settleFlights.delete(current.operationId)
        }
      }
    } else if (current.state !== 'claimed') {
      throw new Error(`cannot settle payment in state ${current.state}`)
    }
    validateSettlement(current, input)
    const settling = {
      ...current,
      state: 'settling' as const,
      settledAmount: input.amount,
      refundAmount: current.reservedAmount - input.amount,
    }
    this.operations.set(current.operationId, settling)
    const flight = this.runSettlement(settling, input)
    this.settleFlights.set(current.operationId, flight)
    try {
      return await flight
    } finally {
      if (this.settleFlights.get(current.operationId) === flight) {
        this.settleFlights.delete(current.operationId)
      }
    }
  }

  async releasePayment(operation: PaymentOperation, reason: string): Promise<PaymentOperation> {
    const current = this.requireCurrent(operation)
    if (current.state === 'released') return current
    if (current.state === 'releasing') {
      const flight = this.releaseFlights.get(current.operationId)
      if (flight) return flight
      return this.reclaimPayment(current.operationId)
    }
    if (current.state !== 'claimed') throw new Error(`cannot release payment in state ${current.state}`)
    const releasing = { ...current, state: 'releasing' as const }
    this.operations.set(current.operationId, releasing)
    return this.runRelease(releasing, reason)
  }

  async reclaimPayment(operationId: string): Promise<PaymentOperation> {
    const current = this.operations.get(operationId)
    if (!current) throw new Error('payment operation was not found')
    if (current.state === 'reclaimed') return current
    if (current.state === 'settling') {
      const flight = this.settleFlights.get(operationId)
      if (flight) return flight
      const recoveryFlight = this.reclaimFlights.get(operationId)
      if (recoveryFlight) return recoveryFlight
      const recovery = this.recoverSettlement(current, {
        amount: current.settledAmount,
        totalCostUsd: 0,
        usage: {
          inputTokens: 0,
          outputTokens: 0,
          reasoningTokens: 0,
          toolTokens: 0,
          toolCallCount: 0,
          providerCostUsd: 0,
          budgetEnforced: true,
        },
      })
      this.reclaimFlights.set(operationId, recovery)
      try {
        return await recovery
      } finally {
        if (this.reclaimFlights.get(operationId) === recovery) this.reclaimFlights.delete(operationId)
      }
    }
    if (current.state === 'releasing') {
      if (!this.options.onReclaim) throw new Error('payment release recovery is not configured')
      const releaseFlight = this.releaseFlights.get(operationId)
      if (releaseFlight) return releaseFlight
      const flight = this.reclaimFlights.get(operationId)
      if (flight) return flight
      const recovery = this.runReclaim(current, 'released')
      this.reclaimFlights.set(operationId, recovery)
      try {
        return await recovery
      } finally {
        if (this.reclaimFlights.get(operationId) === recovery) this.reclaimFlights.delete(operationId)
      }
    }
    if (current.state !== 'claiming' && current.state !== 'claimed' && current.state !== 'reclaimable') {
      throw new Error(`cannot reclaim payment in state ${current.state}`)
    }
    const flight = this.reclaimFlights.get(operationId)
    if (flight) return flight
    if (current.expiresAt > this.now()) throw new Error('payment operation has not expired')
    const reclaimable = { ...current, state: 'reclaimable' as const }
    this.operations.set(operationId, reclaimable)
    const recovery = this.runReclaim(reclaimable, 'reclaimed')
    this.reclaimFlights.set(operationId, recovery)
    try {
      return await recovery
    } finally {
      if (this.reclaimFlights.get(operationId) === recovery) this.reclaimFlights.delete(operationId)
    }
  }

  private async runSettlement(
    settling: PaymentOperation,
    input: PaymentSettlementInput,
  ): Promise<PaymentOperation> {
    await this.options.onSettle?.(settling, input)
    const settled = {
      ...settling,
      state: 'settled' as const,
      settledAmount: input.amount,
      refundAmount: settling.reservedAmount - input.amount,
    }
    this.operations.set(settling.operationId, settled)
    return settled
  }

  private async recoverSettlement(
    settling: PaymentOperation,
    input: PaymentSettlementInput,
  ): Promise<PaymentOperation> {
    if (!this.options.onReclaim) throw new Error('payment settlement recovery is not configured')
    await this.options.onReclaim(settling)
    const settled = {
      ...settling,
      state: 'settled' as const,
      settledAmount: input.amount,
      refundAmount: settling.reservedAmount - input.amount,
    }
    this.operations.set(settling.operationId, settled)
    return settled
  }

  private async runReclaim(
    operation: PaymentOperation,
    finalState: 'released' | 'reclaimed',
  ): Promise<PaymentOperation> {
    await this.options.onReclaim?.(operation)
    const recovered = { ...operation, state: finalState as PaymentOperationState }
    this.operations.set(operation.operationId, recovered)
    return recovered
  }

  private async runRelease(
    releasing: PaymentOperation,
    reason: string,
  ): Promise<PaymentOperation> {
    const flight = (async () => {
      await this.options.onRelease?.(releasing, reason)
      const released = { ...releasing, state: 'released' as const }
      this.operations.set(releasing.operationId, released)
      return released
    })()
    this.releaseFlights.set(releasing.operationId, flight)
    try {
      return await flight
    } finally {
      if (this.releaseFlights.get(releasing.operationId) === flight) {
        this.releaseFlights.delete(releasing.operationId)
      }
    }
  }

  get(operationId: string): PaymentOperation | undefined {
    const operation = this.operations.get(operationId)
    return operation ? { ...operation } : undefined
  }

  private requireCurrent(operation: PaymentOperation): PaymentOperation {
    const current = this.operations.get(operation.operationId)
    if (!current) throw new Error('payment operation was not found')
    if (current.protocolVersion !== operation.protocolVersion) {
      throw new Error('payment operation protocol version mismatch')
    }
    return current
  }
}

export function paymentNonceKey(payload: Record<string, unknown>): string {
  const commitment = String(payload.commitment ?? '').toLowerCase()
  const nonce = unsignedAmount(payload.nonce, 'nonce').toString()
  if (!commitment) throw new Error('payment commitment is required')
  return `${commitment}:${nonce}`
}

function unsignedAmount(value: unknown, name = 'amount'): bigint {
  const raw = typeof value === 'string'
    ? value
    : typeof value === 'number' && Number.isSafeInteger(value)
      ? String(value)
      : ''
  if (!/^\d+$/.test(raw)) throw new Error(`${name} is not an unsigned integer`)
  return BigInt(raw)
}

function validateSettlement(operation: PaymentOperation, input: PaymentSettlementInput): void {
  if (input.amount < 0n || input.amount > operation.reservedAmount) {
    throw new Error('settled amount must be between zero and the reserved amount')
  }
  if (!Number.isFinite(input.totalCostUsd) || input.totalCostUsd < 0) {
    throw new Error('settlement cost must be finite and non-negative')
  }
  if (!input.usage.budgetEnforced) throw new Error('sandbox usage receipt is not budget-enforced')
}
