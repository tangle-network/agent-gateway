import type { MppChargeOperation } from './mpp-payment'
import type { PaymentOperation } from './payment-operations'
import type {
  PaymentMethod,
  SandboxExecutionBudget,
  SandboxUsageReceipt,
} from './types'

export const PAYMENT_RECOVERY_VERSION = 1 as const

export type PaymentRecoveryState =
  | 'claiming'
  | 'claimed'
  | 'executing'
  | 'retained'
  | 'settling'
  | 'releasing'
  | 'reconciled'

export type PaymentSettlementBasis = 'usage-receipt' | 'quoted-ceiling'

export interface SerializedPaymentOperation {
  protocolVersion: 2
  operationId: string
  acquiredByRequestId: string
  executionStartedAt?: number
  retentionReason?: string
  nonceKey: string
  authorizationId: string
  reservedAmount: string
  settledAmount: string
  refundAmount: string
  expiresAt: number
  state: PaymentOperation['state']
}

export interface PaymentRecoveryAttribution {
  requestId: string
  agentId: string
  agentSlug: string
  consumerId: string
  paymentMethod: PaymentMethod
  startMs: number
  pricePerTokenUsd: number
  platformFeePercent: number
  requiredAmount: string
  currencyDecimals: number
  maxOutputTokens: number
  executionBudget: SandboxExecutionBudget
}

export type PaymentRecoveryTarget =
  | {
      kind: 'x402'
      operationId: string
      operation?: SerializedPaymentOperation
    }
  | {
      kind: 'mpp-charge'
      method: string
      operationId: string
      operation?: MppChargeOperation
    }

/** Durable outbox row for one payment identity. Rows are never deleted here. */
export interface PaymentRecoveryRecord {
  version: typeof PAYMENT_RECOVERY_VERSION
  id: string
  revision: number
  state: PaymentRecoveryState
  payment: PaymentRecoveryTarget
  attribution: PaymentRecoveryAttribution
  workStarted: boolean
  /** Earliest time a missing receipt may settle at the quoted ceiling. */
  fallbackAt?: number
  usage?: SandboxUsageReceipt
  usageRecorded: boolean
  settlementBasis?: PaymentSettlementBasis
  reason?: string
  attempts: number
  lastError?: string
  nextAttemptAt: number
  lease?: { id: string; expiresAt: number }
  createdAt: number
  updatedAt: number
  reconciledAt?: number
}

export interface PaymentRecoveryStore {
  createIfAbsent(record: PaymentRecoveryRecord): Promise<boolean>
  get(id: string): Promise<PaymentRecoveryRecord | undefined>
  compareAndSet(
    expected: PaymentRecoveryRecord,
    next: PaymentRecoveryRecord,
  ): Promise<boolean>
  listDue(now: number, limit: number): Promise<PaymentRecoveryRecord[]>
}

export interface PaymentRecoveryConfig {
  store: PaymentRecoveryStore
  /** Claimed payment with no execution becomes recoverable after this delay. */
  staleRequestMs?: number
  /** Work without a final receipt settles at the quoted ceiling after this delay. */
  receiptTimeoutMs?: number
  /** Failed provider recovery waits this long before its next attempt. */
  retryDelayMs?: number
  /** One recovery worker owns a row for this duration. */
  leaseMs?: number
}

export const DEFAULT_STALE_REQUEST_MS = 30_000
export const DEFAULT_RECEIPT_TIMEOUT_MS = 5 * 60_000
export const DEFAULT_RECOVERY_RETRY_MS = 30_000
export const DEFAULT_RECOVERY_LEASE_MS = 30_000

/** Atomic single-process store for tests and explicit local demo mode. */
export class MemoryPaymentRecoveryStore implements PaymentRecoveryStore {
  private readonly records = new Map<string, PaymentRecoveryRecord>()

  async createIfAbsent(record: PaymentRecoveryRecord): Promise<boolean> {
    if (this.records.has(record.id)) return false
    this.records.set(record.id, clone(record))
    return true
  }

  async get(id: string): Promise<PaymentRecoveryRecord | undefined> {
    const record = this.records.get(id)
    return record ? clone(record) : undefined
  }

  async compareAndSet(
    expected: PaymentRecoveryRecord,
    next: PaymentRecoveryRecord,
  ): Promise<boolean> {
    const current = this.records.get(expected.id)
    if (!current || current.revision !== expected.revision) return false
    this.records.set(expected.id, clone(next))
    return true
  }

  async listDue(now: number, limit: number): Promise<PaymentRecoveryRecord[]> {
    return [...this.records.values()]
      .filter((record) => record.state !== 'reconciled' && record.nextAttemptAt <= now)
      .sort((left, right) => left.nextAttemptAt - right.nextAttemptAt)
      .slice(0, limit)
      .map(clone)
  }
}

export class PaymentRecoveryFenceError extends Error {
  constructor(id: string) {
    super(`payment recovery fence was lost for ${id}`)
    this.name = 'PaymentRecoveryFenceError'
  }
}

export class PaymentRecoveryReplayError extends Error {
  constructor(id: string) {
    super(`payment recovery identity was already reconciled: ${id}`)
    this.name = 'PaymentRecoveryReplayError'
  }
}

/** Update only while the caller still owns the durable row fence. */
export async function updateOwnedPaymentRecovery(
  store: PaymentRecoveryStore,
  id: string,
  fenceId: string,
  update: (record: PaymentRecoveryRecord) => PaymentRecoveryRecord,
  now = Date.now(),
): Promise<PaymentRecoveryRecord> {
  for (let attempt = 0; attempt < 16; attempt += 1) {
    const current = await store.get(id)
    if (!current) throw new Error(`payment recovery record ${id} was not found`)
    if (current.state === 'reconciled' || current.lease?.id !== fenceId) {
      throw new PaymentRecoveryFenceError(id)
    }
    const candidate = update(clone(current))
    assertRecoveryUpdate(current, candidate)
    const next: PaymentRecoveryRecord = {
      ...candidate,
      id: current.id,
      version: PAYMENT_RECOVERY_VERSION,
      revision: current.revision + 1,
      createdAt: current.createdAt,
      updatedAt: now,
    }
    if (await store.compareAndSet(current, next)) return next
  }
  throw new Error(`payment recovery record ${id} changed too many times`)
}

const PAYMENT_RECOVERY_TRANSITIONS: Record<PaymentRecoveryState, ReadonlySet<PaymentRecoveryState>> = {
  claiming: new Set(['claiming', 'claimed', 'releasing', 'reconciled']),
  claimed: new Set(['claimed', 'executing', 'releasing', 'reconciled']),
  executing: new Set(['executing', 'retained', 'settling', 'releasing', 'reconciled']),
  retained: new Set(['retained', 'settling', 'reconciled']),
  settling: new Set(['settling', 'reconciled']),
  releasing: new Set(['releasing', 'reconciled']),
  reconciled: new Set(['reconciled']),
}

function assertRecoveryUpdate(
  current: PaymentRecoveryRecord,
  candidate: PaymentRecoveryRecord,
): void {
  if (!PAYMENT_RECOVERY_TRANSITIONS[current.state].has(candidate.state)) {
    throw new Error(`invalid payment recovery transition ${current.state} -> ${candidate.state}`)
  }
  if (JSON.stringify(current.attribution) !== JSON.stringify(candidate.attribution)) {
    throw new Error('payment recovery attribution is immutable')
  }
  if (
    current.payment.kind !== candidate.payment.kind ||
    current.payment.operationId !== candidate.payment.operationId ||
    (current.payment.kind === 'mpp-charge' &&
      candidate.payment.kind === 'mpp-charge' &&
      current.payment.method !== candidate.payment.method)
  ) {
    throw new Error('payment recovery identity is immutable')
  }
  if (current.workStarted && !candidate.workStarted) {
    throw new Error('payment recovery cannot forget started work')
  }
  if (current.usageRecorded && !candidate.usageRecorded) {
    throw new Error('payment recovery cannot forget usage attribution')
  }
  if (current.usage && (!candidate.usage || !sameRecoveryUsage(current.usage, candidate.usage))) {
    throw new Error('payment recovery usage receipt is immutable')
  }
  if (
    current.settlementBasis &&
    current.settlementBasis !== candidate.settlementBasis
  ) {
    throw new Error('payment recovery settlement basis is immutable')
  }
}

function sameRecoveryUsage(
  left: SandboxUsageReceipt,
  right: SandboxUsageReceipt,
): boolean {
  return left.inputTokens === right.inputTokens &&
    left.outputTokens === right.outputTokens &&
    left.reasoningTokens === right.reasoningTokens &&
    left.toolTokens === right.toolTokens &&
    left.toolCallCount === right.toolCallCount &&
    left.providerCostUsd === right.providerCostUsd &&
    left.budgetEnforced === right.budgetEnforced
}

export function serializePaymentOperation(
  operation: PaymentOperation,
): SerializedPaymentOperation {
  return {
    protocolVersion: 2,
    operationId: operation.operationId,
    acquiredByRequestId: operation.acquiredByRequestId,
    ...(operation.executionStartedAt !== undefined
      ? { executionStartedAt: operation.executionStartedAt }
      : {}),
    ...(operation.retentionReason ? { retentionReason: operation.retentionReason } : {}),
    nonceKey: operation.nonceKey,
    authorizationId: operation.authorizationId,
    reservedAmount: operation.reservedAmount.toString(),
    settledAmount: operation.settledAmount.toString(),
    refundAmount: operation.refundAmount.toString(),
    expiresAt: operation.expiresAt,
    state: operation.state,
  }
}

export function deserializePaymentOperation(
  value: SerializedPaymentOperation,
): PaymentOperation {
  if (value.protocolVersion !== 2) throw new Error('unsupported payment operation version')
  if (!value.operationId || !value.acquiredByRequestId || !value.nonceKey || !value.authorizationId) {
    throw new Error('incomplete payment operation recovery record')
  }
  return {
    protocolVersion: 2,
    operationId: value.operationId,
    acquiredByRequestId: value.acquiredByRequestId,
    ...(value.executionStartedAt !== undefined ? { executionStartedAt: value.executionStartedAt } : {}),
    ...(value.retentionReason ? { retentionReason: value.retentionReason } : {}),
    nonceKey: value.nonceKey,
    authorizationId: value.authorizationId,
    reservedAmount: BigInt(value.reservedAmount),
    settledAmount: BigInt(value.settledAmount),
    refundAmount: BigInt(value.refundAmount),
    expiresAt: value.expiresAt,
    state: value.state,
  }
}

export function recoveryTiming(config: PaymentRecoveryConfig): {
  staleRequestMs: number
  receiptTimeoutMs: number
  retryDelayMs: number
  leaseMs: number
} {
  return {
    staleRequestMs: config.staleRequestMs ?? DEFAULT_STALE_REQUEST_MS,
    receiptTimeoutMs: config.receiptTimeoutMs ?? DEFAULT_RECEIPT_TIMEOUT_MS,
    retryDelayMs: config.retryDelayMs ?? DEFAULT_RECOVERY_RETRY_MS,
    leaseMs: config.leaseMs ?? DEFAULT_RECOVERY_LEASE_MS,
  }
}

export function assertPaymentRecoveryConfig(config: PaymentRecoveryConfig): void {
  for (const [name, value] of Object.entries(recoveryTiming(config))) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`payment recovery ${name} must be a positive safe integer`)
    }
  }
  const store = config.store
  if (
    !store ||
    typeof store.createIfAbsent !== 'function' ||
    typeof store.get !== 'function' ||
    typeof store.compareAndSet !== 'function' ||
    typeof store.listDue !== 'function'
  ) {
    throw new Error('payment recovery store must provide atomic create, compare-and-set, and due scans')
  }
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}
