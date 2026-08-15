import {
  type AuthorizedRequest,
  releasePayment,
  settleAndRecord,
} from './dispatch'
import { assertMppChargeOperation } from './mpp-payment'
import {
  deserializePaymentOperation,
  PaymentRecoveryFenceError,
  recoveryTiming,
  updateOwnedPaymentRecovery,
  type PaymentRecoveryRecord,
} from './payment-recovery'
import type { AgentMeta, GatewayConfig, SandboxUsageReceipt } from './types'

interface RecoveryWorkerOptions {
  now?: number
  workerId?: string
}

export interface RecoverPaymentOptions extends RecoveryWorkerOptions {
  /** Process one requested row even when its normal retry time is later. */
  force?: boolean
  /** Exact receipt recovered from a durable protocol record, such as an A2A task. */
  usage?: SandboxUsageReceipt
}

/** Batch recovery never accepts receipt data because each receipt belongs to one row. */
export interface RecoverPaymentsOptions extends RecoveryWorkerOptions {
  limit?: number
}

export interface PaymentRecoveryRun {
  scanned: number
  reconciled: number
  deferred: number
  failed: number
}

/** Scan and reconcile due payment rows. Safe to run concurrently on many workers. */
export async function recoverPayments(
  config: GatewayConfig,
  options: RecoverPaymentsOptions = {},
): Promise<PaymentRecoveryRun> {
  const recovery = config.paymentRecovery
  if (!recovery) throw new Error('durable payment recovery is not configured')
  const now = options.now ?? Date.now()
  const limit = options.limit ?? 100
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new Error('payment recovery limit must be a positive safe integer')
  }
  const due = await recovery.store.listDue(now, limit)
  const run: PaymentRecoveryRun = { scanned: due.length, reconciled: 0, deferred: 0, failed: 0 }
  for (const candidate of due) {
    try {
      const result = await recoverPayment(candidate.id, config, {
        now,
        force: false,
        ...(options.workerId ? { workerId: options.workerId } : {}),
      })
      if (result?.state === 'reconciled') run.reconciled += 1
      else run.deferred += 1
    } catch {
      run.failed += 1
    }
  }
  return run
}

/** Reconcile one payment identity. Hosts can expose this through a private worker API. */
export async function recoverPayment(
  recoveryId: string,
  config: GatewayConfig,
  options: RecoverPaymentOptions = {},
): Promise<PaymentRecoveryRecord | undefined> {
  const recovery = config.paymentRecovery
  if (!recovery) throw new Error('durable payment recovery is not configured')
  const now = options.now ?? Date.now()
  const current = await recovery.store.get(recoveryId)
  if (!current || current.state === 'reconciled') return current
  if (!options.force && current.nextAttemptAt > now) return current

  const leased = await acquireLease(
    current.id,
    config,
    options.workerId ? `${options.workerId}:${randomId()}` : randomId(),
    now,
  )
  if (!leased) return recovery.store.get(recoveryId)
  const fenceId = requireFence(leased)

  try {
    const ready = options.usage
      ? await persistRecoveredUsage(leased, options.usage, config, now)
      : leased
    await reconcileLeased(ready, config, now)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    try {
      await updateOwnedPaymentRecovery(recovery.store, recoveryId, fenceId, (record) => ({
        ...record,
        attempts: record.attempts + 1,
        lastError: message,
        lease: undefined,
        nextAttemptAt: now + recoveryTiming(recovery).retryDelayMs,
      }), now)
    } catch (updateError) {
      if (!(updateError instanceof PaymentRecoveryFenceError)) throw updateError
    }
    throw error
  }
  return recovery.store.get(recoveryId)
}

async function reconcileLeased(
  record: PaymentRecoveryRecord,
  config: GatewayConfig,
  now: number,
): Promise<void> {
  if (record.payment.kind === 'mpp-charge' && !record.payment.operation) {
    record = await recoverUnknownMppCharge(record, config)
  }
  if (record.state === 'reconciled') return

  if (record.state === 'claiming') {
    if (record.payment.kind === 'x402') {
      if (!config.x402.paymentOperations) {
        throw new Error('x402 payment recovery is not configured')
      }
      const recovered = await config.x402.paymentOperations.reclaimPayment(record.payment.operationId)
      if (recovered.state !== 'released' && recovered.state !== 'reclaimed') {
        throw new Error(`ambiguous x402 claim recovered in state ${recovered.state}`)
      }
      await completeRecord(record, config)
      return
    }
    throw new Error('MPP charge confirmation remains unresolved')
  }

  if (record.state === 'claimed' && !record.workStarted) {
    await releaseRecoveredPayment(record, config, 'request ended before sandbox execution')
    return
  }

  if (record.state === 'releasing') {
    await releaseRecoveredPayment(record, config, record.reason ?? 'payment recovery release')
    return
  }

  if (
    record.state === 'executing' ||
    record.state === 'retained' ||
    record.state === 'settling' ||
    (record.state === 'claimed' && record.workStarted)
  ) {
    if (!record.usage && record.fallbackAt !== undefined && record.fallbackAt > now) {
      await updateOwnedPaymentRecovery(
        config.paymentRecovery!.store,
        record.id,
        requireFence(record),
        (current) => ({
          ...current,
          lease: undefined,
          nextAttemptAt: record.fallbackAt!,
        }),
        now,
      )
      return
    }
    await settleRecoveredPayment(record, config)
    return
  }

  throw new Error(`payment recovery cannot reconcile state ${record.state}`)
}

async function recoverUnknownMppCharge(
  record: PaymentRecoveryRecord,
  config: GatewayConfig,
): Promise<PaymentRecoveryRecord> {
  if (record.payment.kind !== 'mpp-charge') return record
  const method = record.payment.method
  const operationId = record.payment.operationId
  const lifecycle = config.mpp?.charge
  if (!lifecycle || lifecycle.protocolVersion !== 1) {
    throw new Error('MPP charge recovery is not configured')
  }
  const result = await lifecycle.recoverPayment(operationId)
  if (!('protocolVersion' in result)) {
    if (result.operationId !== operationId) {
      throw new Error('MPP recovery operation id mismatch')
    }
    if (result.state === 'not-found') {
      await completeRecord(record, config)
      return requireRecord(record.id, config)
    }
    throw new Error('MPP charge confirmation is still pending')
  }
  assertMppChargeOperation(
    result,
    {
      operationId,
      requestId: record.attribution.requestId,
      method,
    },
    ['confirmed', 'releasing', 'released'],
    false,
  )
  return updateOwnedPaymentRecovery(
    config.paymentRecovery!.store,
    record.id,
    requireFence(record),
    (current) => ({
      ...current,
      state: result.state === 'released'
        ? 'reconciled'
        : result.state === 'releasing'
          ? 'releasing'
          : current.workStarted
            ? 'executing'
            : 'claimed',
      payment: { kind: 'mpp-charge', method, operationId, operation: result },
      ...(result.state === 'released'
        ? { reconciledAt: Date.now(), nextAttemptAt: Number.MAX_SAFE_INTEGER, lease: undefined }
        : {}),
    }),
  )
}

async function releaseRecoveredPayment(
  record: PaymentRecoveryRecord,
  config: GatewayConfig,
  reason: string,
): Promise<void> {
  const authz = authorizedRequest(record)
  if (record.payment.kind === 'x402') {
    if (!record.payment.operation) {
      if (!config.x402.paymentOperations) throw new Error('x402 payment recovery is not configured')
      const recovered = await config.x402.paymentOperations.reclaimPayment(record.payment.operationId)
      if (recovered.state !== 'released' && recovered.state !== 'reclaimed') {
        throw new Error(`x402 release recovered in state ${recovered.state}`)
      }
      await completeRecord(record, config)
      return
    }
    authz.paymentOperation = deserializePaymentOperation(record.payment.operation)
    authz.paymentOperationAcquired = true
  } else {
    if (!record.payment.operation) throw new Error('MPP release operation is missing')
    authz.mppChargeOperation = record.payment.operation
  }
  await releasePayment(authz, config, reason)
}

async function settleRecoveredPayment(
  record: PaymentRecoveryRecord,
  config: GatewayConfig,
): Promise<void> {
  const authz = authorizedRequest(record)
  if (record.payment.kind === 'x402') {
    if (!record.payment.operation) throw new Error('x402 settlement operation is missing')
    authz.paymentOperation = deserializePaymentOperation(record.payment.operation)
    authz.paymentOperationAcquired = true
  } else {
    if (!record.payment.operation) throw new Error('MPP settlement operation is missing')
    authz.mppChargeOperation = record.payment.operation
  }

  const fallback = !record.usage
  const usage = record.usage ?? quotedCeilingUsage(record)
  await settleAndRecord(
    recoveryAgent(record),
    authz,
    usage,
    config,
    config.observer,
    {
      usageAlreadyRecorded: record.usageRecorded,
      settlementBasis: fallback ? 'quoted-ceiling' : record.settlementBasis ?? 'usage-receipt',
      ...(fallback && record.payment.kind === 'x402'
        ? { paymentAmount: BigInt(record.attribution.requiredAmount) }
        : {}),
    },
  )
}

function quotedCeilingUsage(record: PaymentRecoveryRecord): SandboxUsageReceipt {
  const amount = BigInt(record.attribution.requiredAmount)
  const providerCostUsd = baseUnitsToNumber(amount, record.attribution.currencyDecimals)
  if (!Number.isFinite(providerCostUsd) || providerCostUsd < 0) {
    throw new Error('quoted payment ceiling cannot be represented as USD')
  }
  return {
    inputTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd,
    budgetEnforced: true,
  }
}

function baseUnitsToNumber(amount: bigint, decimals: number): number {
  const digits = amount.toString().padStart(decimals + 1, '0')
  if (decimals === 0) return Number(digits)
  const split = digits.length - decimals
  return Number(`${digits.slice(0, split)}.${digits.slice(split)}`)
}

function authorizedRequest(record: PaymentRecoveryRecord): AuthorizedRequest {
  const attribution = record.attribution
  return {
    agent: recoveryAgent(record),
    consumerId: attribution.consumerId,
    paymentMethod: attribution.paymentMethod,
    keyInfo: null,
    userMessage: '[payment recovery]',
    rateLimitRemaining: undefined,
    requestId: attribution.requestId,
    startMs: attribution.startMs,
    maxOutputTokens: attribution.maxOutputTokens,
    executionBudget: attribution.executionBudget,
    requiredPaymentAmount: BigInt(attribution.requiredAmount),
    paymentPayload: null,
    paymentRecoveryId: record.id,
    paymentRecoveryFence: requireFence(record),
  }
}

function recoveryAgent(record: PaymentRecoveryRecord): AgentMeta {
  const attribution = record.attribution
  return {
    id: attribution.agentId,
    ownerId: '[payment recovery]',
    slug: attribution.agentSlug,
    pricePerTokenUsd: attribution.pricePerTokenUsd,
    platformFeePercent: attribution.platformFeePercent,
    sandboxEndpoint: null,
    remoteSandboxId: null,
    remoteBearerToken: null,
    enabled: true,
  }
}

async function acquireLease(
  id: string,
  config: GatewayConfig,
  workerId: string,
  now: number,
): Promise<PaymentRecoveryRecord | undefined> {
  const recovery = config.paymentRecovery!
  for (let attempt = 0; attempt < 16; attempt += 1) {
    const current = await recovery.store.get(id)
    if (!current || current.state === 'reconciled') return current
    if (current.lease && current.lease.expiresAt > now) return undefined
    const next: PaymentRecoveryRecord = {
      ...current,
      revision: current.revision + 1,
      lease: { id: workerId, expiresAt: now + recoveryTiming(recovery).leaseMs },
      updatedAt: now,
    }
    if (await recovery.store.compareAndSet(current, next)) return next
  }
  throw new Error(`payment recovery record ${id} changed too many times`)
}

async function completeRecord(record: PaymentRecoveryRecord, config: GatewayConfig): Promise<void> {
  const now = Date.now()
  await updateOwnedPaymentRecovery(
    config.paymentRecovery!.store,
    record.id,
    requireFence(record),
    (current) => ({
      ...current,
      state: 'reconciled',
      lease: undefined,
      lastError: undefined,
      nextAttemptAt: Number.MAX_SAFE_INTEGER,
      reconciledAt: now,
    }),
    now,
  )
}

async function requireRecord(id: string, config: GatewayConfig): Promise<PaymentRecoveryRecord> {
  const record = await config.paymentRecovery!.store.get(id)
  if (!record) throw new Error(`payment recovery record ${id} was not found`)
  return record
}

function randomId(): string {
  return globalThis.crypto.randomUUID()
}

function requireFence(record: PaymentRecoveryRecord): string {
  if (!record.lease?.id) throw new Error(`payment recovery record ${record.id} has no owner fence`)
  return record.lease.id
}

async function persistRecoveredUsage(
  record: PaymentRecoveryRecord,
  usage: SandboxUsageReceipt,
  config: GatewayConfig,
  now: number,
): Promise<PaymentRecoveryRecord> {
  assertRecoveryUsage(usage)
  return updateOwnedPaymentRecovery(
    config.paymentRecovery!.store,
    record.id,
    requireFence(record),
    (current) => {
      if (current.usage && !sameUsage(current.usage, usage)) {
        throw new Error('payment recovery receipt does not match persisted usage')
      }
      return {
        ...current,
        state: 'settling',
        workStarted: true,
        usage,
        settlementBasis: 'usage-receipt',
        nextAttemptAt: now,
      }
    },
    now,
  )
}

function assertRecoveryUsage(usage: SandboxUsageReceipt): void {
  for (const [name, value] of [
    ['inputTokens', usage.inputTokens],
    ['outputTokens', usage.outputTokens],
    ['reasoningTokens', usage.reasoningTokens],
    ['toolTokens', usage.toolTokens],
    ['toolCallCount', usage.toolCallCount],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`payment recovery ${name} must be a non-negative safe integer`)
    }
  }
  if (!Number.isFinite(usage.providerCostUsd) || usage.providerCostUsd < 0) {
    throw new Error('payment recovery providerCostUsd must be finite and non-negative')
  }
  if (usage.budgetEnforced !== true) {
    throw new Error('payment recovery receipt must confirm budget enforcement')
  }
}

function sameUsage(left: SandboxUsageReceipt, right: SandboxUsageReceipt): boolean {
  return left.inputTokens === right.inputTokens &&
    left.outputTokens === right.outputTokens &&
    left.reasoningTokens === right.reasoningTokens &&
    left.toolTokens === right.toolTokens &&
    left.toolCallCount === right.toolCallCount &&
    left.providerCostUsd === right.providerCostUsd &&
    left.budgetEnforced === right.budgetEnforced
}
