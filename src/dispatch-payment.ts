import {
  assertMppChargeOperation,
  mppPaymentOperationId,
} from './mpp-payment'
import {
  ApiKeyRequestClaimUnavailableError,
  ApiKeyRequestLimitExceededError,
} from './api-keys'
import { claimStoredNonce, nonceTtlSeconds, type NonceStore } from './nonce-store'
import {
  paymentNonceKey,
  type PaymentOperation,
  type PaymentOperationRecoveryResult,
} from './payment-operations'
import { recoveryTiming, updateOwnedPaymentRecovery } from './payment-recovery'
import type {
  AuthorizedRequest,
  GatewayState,
  PaymentClaimHooks,
} from './dispatch-types'
import {
  assertX402V1SettlementSafe,
  markRecoveryClaimed,
  markRecoveryReconciled,
  markRecoveryReleasing,
  paymentAuthorizationContext,
  preparePaymentRecovery,
  recoveryTarget,
  relinquishPaymentRecovery,
  requirePaymentRecoveryFence,
} from './dispatch-payment-recovery'
import type { GatewayConfig } from './types'

/** Claim payment ownership after every request guard has accepted the call. */
export async function claimPayment(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  state: GatewayState,
  hooks: PaymentClaimHooks = {},
): Promise<void> {
  assertX402V1SettlementSafe(authz, config)
  if (authz.paymentMethod === 'apikey') {
    if (!authz.keyInfo) {
      throw new ApiKeyRequestClaimUnavailableError('Verified API key identity is unavailable')
    }
    const claimRequest = config.claimApiKeyRequest
    if (!claimRequest) {
      if (authz.keyInfo?.dailyLimit !== undefined) {
        throw new ApiKeyRequestClaimUnavailableError(
          'API key request limits are not configured',
        )
      }
    } else {
      let claim
      try {
        claim = await claimRequest({
          keyInfo: authz.keyInfo,
          requestId: authz.requestId,
          requestedAt: new Date(authz.startMs),
        })
      } catch (error) {
        if (
          error instanceof ApiKeyRequestClaimUnavailableError ||
          error instanceof ApiKeyRequestLimitExceededError
        ) throw error
        throw new ApiKeyRequestClaimUnavailableError(
          'API key request limits could not be checked',
          { cause: error },
        )
      }
      assertApiKeyRequestClaim(claim)
      if (!claim.allowed) throw new ApiKeyRequestLimitExceededError(claim)
      authz.rateLimitRemaining = Math.min(
        authz.rateLimitRemaining ?? claim.minuteRemaining,
        claim.minuteRemaining,
      )
    }
  } else if (authz.paymentMethod === 'x402' && authz.paymentPayload) {
    const context = paymentAuthorizationContext(authz)
    if (config.x402.paymentProtocolVersion === 2) {
      await preparePaymentRecovery(authz, config, {
        kind: 'x402',
        operationId: `x402:${paymentNonceKey(authz.paymentPayload)}`,
      }, hooks)
    }
    let operation: PaymentOperation | undefined
    if (config.x402.authorizePayment) {
      if (config.x402.paymentProtocolVersion !== 2 && !config.x402.demoMode) {
        throw new Error(
          'production x402 version 1 cannot use authorizePayment; ' +
            'use paymentProtocolVersion: 2 with paymentOperations',
        )
      }
      // Version 1 has no durable operation to release if another request wins
      // the shared nonce while this callback is still running. This callback
      // remains only for explicit demo-mode compatibility; production callers
      // must use the durable version 2 operation lifecycle.
      const legacyClaimed = config.x402.paymentProtocolVersion !== 2 && authz.paymentNonceKey
        ? await claimPaymentNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload)
        : undefined
      if (legacyClaimed === false) throw new Error('payment nonce was already consumed')
      const result = await config.x402.authorizePayment(authz.paymentPayload, context)
      if (!result) throw new Error('payment authorization was rejected')
      if (typeof result !== 'boolean') {
        operation = result
      }
      else if (config.x402.paymentProtocolVersion === 2) {
        throw new Error('version 2 payment authorization did not return an operation')
      } else if (authz.paymentNonceKey && legacyClaimed === undefined) {
        const claimed = await claimPaymentNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload)
        if (!claimed) throw new Error('payment nonce was already consumed')
      }
    } else if (config.x402.paymentOperations) {
      operation = await config.x402.paymentOperations.claimPayment(authz.paymentPayload, context)
    } else if (authz.paymentNonceKey) {
      const claimed = await claimPaymentNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload)
      if (!claimed) throw new Error('payment nonce was already consumed')
    }
    if (operation && operation.protocolVersion !== 2) {
      throw new Error('payment operation protocol version mismatch')
    }
    if (
      operation &&
      config.x402.paymentProtocolVersion === 2 &&
      operation.operationId !== authz.paymentRecoveryId
    ) {
      throw new Error('x402 payment operation identity mismatch')
    }
    if (operation && !config.x402.paymentOperations) {
      throw new Error('durable payment operations are required to settle a claimed operation')
    }
    if (operation) {
      authz.paymentOperationAcquired = operation.acquiredByRequestId === context.requestId
      if (!authz.paymentOperationAcquired) {
        throw new Error('payment operation was already claimed')
      }
      // Attach owned state before the shared nonce claim. If that claim fails,
      // the caller can persist release recovery after an ambiguous refund.
      authz.paymentOperation = operation
      await markRecoveryClaimed(authz, config)
    }
    if (operation && authz.paymentNonceKey) {
      const claimed = await claimPaymentNonce(
        state.nonceStore,
        authz.paymentNonceKey,
        authz.paymentPayload,
        `${operation.operationId}:${context.requestId}`,
      )
      if (!claimed) {
        await releaseAfterNonceConflict(authz, config)
        throw new Error('payment nonce was already consumed')
      }
    }
  } else if (authz.paymentMethod === 'mpp') {
    if (!authz.paymentNonceKey) {
      throw new Error('MPP payment has no replay identity')
    }
    const mppMethod = (authz.mppMethod ?? config.mpp?.method ?? 'blueprintevm').toLowerCase()
    const durablePayload = durableMppPaymentPayload(authz.paymentPayload)
    // Only BlueprinTEVM carries x402 authorization fields. Other MPP methods
    // use the isolated immediate-charge lifecycle below.
    if (mppMethod === 'blueprintevm' && durablePayload && config.x402.paymentOperations) {
      const context = paymentAuthorizationContext(authz)
      await preparePaymentRecovery(authz, config, {
        kind: 'x402',
        operationId: `x402:${paymentNonceKey(durablePayload)}`,
      }, hooks)
      const operation = await config.x402.paymentOperations.claimPayment(durablePayload, context)
      if (operation.protocolVersion !== 2) throw new Error('payment operation protocol version mismatch')
      if (operation.operationId !== authz.paymentRecoveryId) {
        throw new Error('x402 payment operation identity mismatch')
      }
      if (operation.acquiredByRequestId !== context.requestId) {
        throw new Error('payment operation was already claimed')
      }
      authz.paymentPayload = durablePayload
      authz.paymentOperation = operation
      authz.paymentOperationAcquired = true
      await markRecoveryClaimed(authz, config)
      const claimed = await claimPaymentNonce(
        state.nonceStore,
        authz.paymentNonceKey,
        durablePayload,
        `${operation.operationId}:${context.requestId}`,
      )
      if (!claimed) {
        await releaseAfterNonceConflict(authz, config)
        throw new Error('payment nonce was already consumed')
      }
    } else if (mppMethod === 'blueprintevm') {
      const claimed = await claimPaymentNonce(state.nonceStore, authz.paymentNonceKey, authz.paymentPayload ?? {})
      if (!claimed) throw new Error('payment nonce was already consumed')
    } else {
      const lifecycle = config.mpp?.charge
      if (!lifecycle || lifecycle.protocolVersion !== 1) {
        throw new Error('MPP charge lifecycle is not configured')
      }
      if (!authz.mppCredential) throw new Error('MPP payment credential is unavailable')
      if (!authz.mppPaymentIdentity) throw new Error('MPP payment identity is unavailable')
      const operationId = await mppPaymentOperationId(mppMethod, authz.mppPaymentIdentity)
      await preparePaymentRecovery(authz, config, {
        kind: 'mpp-charge',
        method: mppMethod,
        operationId,
      }, hooks)
      const claimed = await claimPaymentNonce(
        state.nonceStore,
        authz.paymentNonceKey,
        authz.paymentPayload ?? {},
        `${operationId}:${authz.requestId}`,
      )
      if (!claimed) {
        await markRecoveryReconciled(authz, config)
        throw new Error('payment nonce was already consumed')
      }
      const operation = await lifecycle.confirmPayment({
        operationId,
        requestId: authz.requestId,
        agentId: authz.agent.id,
        consumerId: authz.consumerId,
        method: mppMethod,
        credential: authz.mppCredential,
        amount: authz.requiredPaymentAmount,
        currencyDecimals: config.x402.currencyDecimals ?? 6,
      })
      assertMppChargeOperation(
        operation,
        { operationId, requestId: authz.requestId, method: mppMethod },
        ['confirmed'],
        false,
      )
      authz.mppChargeOperation = operation
      await markRecoveryClaimed(authz, config)
      assertMppChargeOperation(
        operation,
        { operationId, requestId: authz.requestId, method: mppMethod },
        ['confirmed'],
      )
    }
  }

  try {
    await state.obs?.onPaymentVerified?.(
      {
        requestId: authz.requestId,
        agentSlug: authz.agent.slug,
        startMs: authz.startMs,
      },
      {
        method: authz.paymentMethod,
        consumerId: authz.consumerId,
        keyId: authz.keyInfo?.keyId,
      },
    )
  } catch (error) {
    // Observability must not turn a durable claim into a stranded payment.
    console.error(
      '[agent-gateway] payment observer failed for ' + authz.requestId + ':',
      error instanceof Error ? error.message : String(error),
    )
  }
}

function assertApiKeyRequestClaim(
  claim: import('./types').ApiKeyRequestClaimResult,
): void {
  if (typeof claim.allowed !== 'boolean') {
    throw new ApiKeyRequestClaimUnavailableError('API key request claim is invalid')
  }
  for (const [name, value] of [
    ['minuteRemaining', claim.minuteRemaining],
    ['dailyRemaining', claim.dailyRemaining],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new ApiKeyRequestClaimUnavailableError(`API key request claim ${name} is invalid`)
    }
  }
  for (const [name, value] of [
    ['minuteResetAt', claim.minuteResetAt],
    ['dailyResetAt', claim.dailyResetAt],
  ] as const) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new ApiKeyRequestClaimUnavailableError(`API key request claim ${name} is invalid`)
    }
  }
  if (!claim.allowed && claim.reason !== 'minute' && claim.reason !== 'daily') {
    throw new ApiKeyRequestClaimUnavailableError('API key request claim reason is invalid')
  }
}

/** Release an owned operation when execution cannot produce a valid receipt. */
export async function releasePayment(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  reason: string,
): Promise<void> {
  const ownsX402 = authz.paymentOperation &&
    authz.paymentOperationAcquired === true &&
    config.x402.paymentOperations
  const ownsMpp = authz.mppChargeOperation && config.mpp?.charge
  if (!ownsX402 && !ownsMpp) {
    await relinquishPaymentRecovery(authz, config, Date.now())
    return
  }
  let reconciled = false
  try {
    await markRecoveryReleasing(authz, config, reason)
    if (ownsX402) {
      authz.paymentOperation = await config.x402.paymentOperations!.releasePayment(
        authz.paymentOperation!,
        reason,
      )
    } else {
      const operation = await config.mpp!.charge!.releasePayment(authz.mppChargeOperation!, reason)
      assertMppChargeOperation(
        operation,
        {
          operationId: authz.mppChargeOperation!.operationId,
          requestId: authz.requestId,
          method: authz.mppChargeOperation!.method,
        },
        ['released'],
        false,
      )
      authz.mppChargeOperation = operation
    }
    await markRecoveryReconciled(authz, config)
    reconciled = true
  } finally {
    if (!reconciled) {
      try {
        await relinquishPaymentRecovery(authz, config, Date.now())
      } catch {
        // Preserve the original provider or metadata error. A later worker
        // retry still has the durable row when cleanup itself is unavailable.
      }
    }
  }
}

/** Mark a durable reservation active immediately before sandbox execution. */
export async function beginPaymentExecution(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  if (authz.paymentOperation && authz.paymentOperationAcquired === true && config.x402.paymentOperations) {
    authz.paymentOperation = await config.x402.paymentOperations.beginPaymentExecution(authz.paymentOperation)
  }
}

/** Persist the sandbox handoff immediately before the adapter call. */
export async function markPaymentExecutionStarted(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  await updateExecutionLease(authz, config, true)
}

/** Renew the live execution lease while a provider stream is still open. */
export async function renewPaymentExecution(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  await updateExecutionLease(authz, config, false)
}

async function updateExecutionLease(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  markStarted: boolean,
): Promise<void> {
  if (!authz.paymentRecoveryId) return
  const recovery = config.paymentRecovery
  if (!recovery) throw new Error('durable payment recovery is not configured')
  const fenceId = requirePaymentRecoveryFence(authz)
  const now = Date.now()
  const fallbackAt = now + recoveryTiming(recovery).receiptTimeoutMs
  await updateOwnedPaymentRecovery(recovery.store, authz.paymentRecoveryId, fenceId, (record) => ({
    ...record,
    state: 'executing',
    ...(markStarted
      ? { payment: recoveryTarget(authz, record.payment), workStarted: true }
      : {}),
    fallbackAt,
    lease: { id: fenceId, expiresAt: fallbackAt },
    nextAttemptAt: fallbackAt,
  }), now)
}

/**
 * Release only when no sandbox work was observed. Once output or a receipt
 * exists, retain the owner for settlement or background recovery.
 */
export async function releasePaymentAfterFailure(
  authz: AuthorizedRequest,
  config: GatewayConfig,
  reason: string,
  workObserved: boolean,
): Promise<void> {
  if (workObserved) {
    const recovery = config.paymentRecovery
    if (recovery && authz.paymentRecoveryId) {
      const fenceId = requirePaymentRecoveryFence(authz)
      await updateOwnedPaymentRecovery(recovery.store, authz.paymentRecoveryId, fenceId, (record) => {
        if (record.state === 'settling' && record.usage) {
          return { ...record, lease: undefined, nextAttemptAt: Date.now() }
        }
        const fallbackAt = record.fallbackAt ??
          Date.now() + recoveryTiming(recovery).receiptTimeoutMs
        return {
          ...record,
          state: 'retained',
          payment: recoveryTarget(authz, record.payment),
          workStarted: true,
          fallbackAt,
          reason,
          lease: undefined,
          nextAttemptAt: fallbackAt,
        }
      })
    }
    if (authz.paymentOperation && authz.paymentOperationAcquired === true && config.x402.paymentOperations) {
      authz.paymentOperation = await config.x402.paymentOperations.retainPayment(authz.paymentOperation, reason)
    }
    console.error(
      `[agent-gateway] retaining payment ownership after sandbox work for ${authz.requestId}: ${reason}`,
    )
    return
  }
  await releasePayment(authz, config, reason)
}

export async function reclaimPayment(
  operationId: string,
  config: GatewayConfig,
): Promise<PaymentOperationRecoveryResult> {
  if (!config.x402.paymentOperations) throw new Error('durable payment operations are not configured')
  return config.x402.paymentOperations.reclaimPayment(operationId)
}

async function releaseAfterNonceConflict(
  authz: AuthorizedRequest,
  config: GatewayConfig,
): Promise<void> {
  try {
    await releasePayment(authz, config, 'shared payment nonce was already owned')
  } catch (releaseError) {
    console.error(
      `[agent-gateway] payment release failed for ${authz.requestId}:`,
      releaseError instanceof Error ? releaseError.message : String(releaseError),
    )
  }
}

async function claimPaymentNonce(
  nonceStore: NonceStore,
  nonceKey: string,
  payload: Record<string, unknown>,
  ownerId?: string,
): Promise<boolean> {
  const expiry = payload.expiry === undefined
    ? BigInt(Math.floor(Date.now() / 1000) + 3600)
    : BigInt(String(payload.expiry))
  const ttl = nonceTtlSeconds(expiry)
  if (ttl === undefined) return false
  return claimStoredNonce(nonceStore, nonceKey, ttl, ownerId)
}

function durableMppPaymentPayload(
  payload: Record<string, unknown> | null,
): Record<string, unknown> | undefined {
  if (!payload) return undefined
  const commitment = payload.commitment ?? payload.from
  const amount = payload.amount ?? payload.value
  const nonce = payload.nonce
  if (typeof commitment !== 'string' || commitment.length === 0) return undefined
  if (amount === undefined || nonce === undefined) return undefined
  const amountText = String(amount)
  const nonceText = String(nonce)
  if (!/^\d+$/.test(amountText) || !/^\d+$/.test(nonceText)) return undefined
  const expiryText = payload.expiry === undefined
    ? String(Math.floor(Date.now() / 1000) + 3600)
    : String(payload.expiry)
  if (!/^\d+$/.test(expiryText)) return undefined
  return {
    ...payload,
    commitment,
    amount: amountText,
    nonce: nonceText,
    expiry: expiryText,
  }
}
