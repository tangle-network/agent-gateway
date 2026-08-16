import {
  PAYMENT_RECOVERY_VERSION,
  PaymentRecoveryFenceError,
  PaymentRecoveryReplayError,
  recoveryTiming,
  serializePaymentOperation,
  updateOwnedPaymentRecovery,
  type PaymentRecoveryRecord,
  type PaymentRecoveryTarget,
  type PaymentSettlementBasis,
} from './payment-recovery'
import type { GatewayConfig, SandboxUsageReceipt } from './types'
import type {
  AuthorizedRequest,
  PaymentClaimHooks,
} from './dispatch-types'

export function paymentAuthorizationContext(authz: AuthorizedRequest) {
  return {
    requestId: authz.requestId,
    agentId: authz.agent.id,
    requiredAmount: authz.requiredPaymentAmount,
    maxOutputTokens: authz.maxOutputTokens,
    executionBudget: authz.executionBudget,
  }
}

export async function preparePaymentRecovery(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  payment: PaymentRecoveryTarget,
  hooks: PaymentClaimHooks,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery) throw new Error('durable payment recovery is not configured')
  const now = Date.now()
  const fenceId = globalThis.crypto.randomUUID()
  const leaseExpiresAt = now + recoveryTiming(recovery).staleRequestMs
  const record: PaymentRecoveryRecord = {
    version: PAYMENT_RECOVERY_VERSION,
    id: payment.operationId,
    revision: 0,
    state: 'claiming',
    payment,
    attribution: {
      requestId: authz.requestId,
      agentId: authz.agent.id,
      agentSlug: authz.agent.slug,
      consumerId: authz.consumerId,
      paymentMethod: authz.paymentMethod,
      startMs: authz.startMs,
      pricePerTokenUsd: authz.agent.pricePerTokenUsd,
      platformFeePercent: authz.agent.platformFeePercent,
      requiredAmount: authz.requiredPaymentAmount.toString(),
      currencyDecimals: config.x402.currencyDecimals ?? 6,
      maxOutputTokens: authz.maxOutputTokens,
      executionBudget: authz.executionBudget,
    },
    workStarted: false,
    usageRecorded: false,
    attempts: 0,
    nextAttemptAt: leaseExpiresAt,
    lease: { id: fenceId, expiresAt: leaseExpiresAt },
    createdAt: now,
    updatedAt: now,
  }
  if (!await recovery.store.createIfAbsent(record)) {
    if ((await recovery.store.get(record.id))?.state === 'reconciled') {
      throw new PaymentRecoveryReplayError(record.id)
    }
    throw new Error('payment recovery identity was already claimed')
  }
  authz.paymentRecoveryId = record.id
  authz.paymentRecoveryFence = fenceId
  await hooks.onRecoveryPrepared?.(record.id)
}

export async function markRecoveryClaimed(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId) return
  const fenceId = requirePaymentRecoveryFence(authz)
  const now = Date.now()
  const leaseExpiresAt = now + recoveryTiming(recovery).staleRequestMs
  await updateRecovery(authz, config, (record) => ({
    ...record,
    state: 'claimed',
    payment: recoveryTarget(authz, record.payment),
    lease: { id: fenceId, expiresAt: leaseExpiresAt },
    nextAttemptAt: leaseExpiresAt,
  }), now)
}

export function requirePaymentRecoveryFence(authz: AuthorizedRequest): string {
  if (!authz.paymentRecoveryFence) {
    throw new Error('payment recovery fence is unavailable')
  }
  return authz.paymentRecoveryFence
}

async function updateRecovery(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  update: (record: PaymentRecoveryRecord) => PaymentRecoveryRecord,
  now = Date.now(),
): Promise<PaymentRecoveryRecord | undefined> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId) return undefined
  return updateOwnedPaymentRecovery(
    recovery.store,
    authz.paymentRecoveryId,
    requirePaymentRecoveryFence(authz),
    update,
    now,
  )
}

export function recoveryTarget(
  authz: AuthorizedRequest,
  current: PaymentRecoveryTarget,
): PaymentRecoveryTarget {
  if (authz.paymentOperation) {
    return {
      kind: 'x402',
      operationId: authz.paymentOperation.operationId,
      operation: serializePaymentOperation(authz.paymentOperation),
    }
  }
  if (authz.mppChargeOperation) {
    return {
      kind: 'mpp-charge',
      method: authz.mppChargeOperation.method,
      operationId: authz.mppChargeOperation.operationId,
      operation: authz.mppChargeOperation,
    }
  }
  return current
}

export async function relinquishPaymentRecovery(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  nextAttemptAt: number,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId || !authz.paymentRecoveryFence) return
  try {
    await updateOwnedPaymentRecovery(
      recovery.store,
      authz.paymentRecoveryId,
      authz.paymentRecoveryFence,
      (record) => ({ ...record, lease: undefined, nextAttemptAt }),
    )
  } catch (error) {
    if (!(error instanceof PaymentRecoveryFenceError)) throw error
  }
}

export async function markRecoveryReleasing(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  reason: string,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId) return
  await updateRecovery(authz, config, (record) => ({
    ...record,
    state: 'releasing',
    payment: recoveryTarget(authz, record.payment),
    reason,
    nextAttemptAt: Date.now(),
  }))
}

export async function markRecoveryReconciled(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId) return
  const now = Date.now()
  await updateRecovery(authz, config, (record) => ({
    ...record,
    state: 'reconciled',
    payment: recoveryTarget(authz, record.payment),
    lease: undefined,
    lastError: undefined,
    nextAttemptAt: Number.MAX_SAFE_INTEGER,
    reconciledAt: now,
  }), now)
}

export function assertX402V1SettlementSafe(authz: AuthorizedRequest, config: GatewayConfig): void {
  if (
    authz.paymentMethod === 'x402' &&
    config.x402.paymentProtocolVersion !== 2 &&
    !config.x402.demoMode &&
    config.settlePayment
  ) {
    throw new Error(
      'production x402 version 1 cannot use settlePayment; ' +
        'use paymentProtocolVersion: 2 with paymentOperations',
    )
  }
}

export async function markRecoverySettling(
  authz: AuthorizedRequest,
  usage: SandboxUsageReceipt,
  settlementBasis: PaymentSettlementBasis,
  config: GatewayConfig,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId) return
  await updateRecovery(authz, config, (record) => {
    const next: PaymentRecoveryRecord = {
      ...record,
      state: 'settling',
      payment: recoveryTarget(authz, record.payment),
      workStarted: true,
      settlementBasis,
      nextAttemptAt: Date.now(),
    }
    // A quoted-ceiling settlement has no provider receipt. Keep the durable
    // basis and original amount, then rebuild the synthetic accounting input
    // on each retry instead of persisting a lossy floating-point surrogate.
    if (settlementBasis !== 'quoted-ceiling' || record.usage !== undefined) {
      next.usage = usage
    } else {
      delete next.usage
    }
    return next
  })
}

export async function markRecoveryUsageRecorded(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  const recovery = config.paymentRecovery
  if (!recovery || !authz.paymentRecoveryId) return
  await updateRecovery(authz, config, (record) => ({
    ...record,
    usageRecorded: true,
  }))
}
