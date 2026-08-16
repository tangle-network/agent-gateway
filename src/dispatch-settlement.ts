import { type GatewayObserver, type RequestContext } from './observer'
import { actualX402Amount } from './dispatch-pricing'
import {
  assertX402V1SettlementSafe,
  markRecoveryReconciled,
  markRecoverySettling,
  markRecoveryUsageRecorded,
} from './dispatch-payment-recovery'
import type {
  AuthorizedRequest,
  SettleAndRecordOptions,
} from './dispatch-types'
import type { AgentMeta, GatewayConfig, SandboxUsageReceipt } from './types'

/**
 * Record usage, settle payment, and invoke the observer. Both wire formats
 * call this once their stream has drained, so settlement happens exactly once
 * per request regardless of protocol.
 */
export async function settleAndRecord(
  agent: AgentMeta,
  authz: AuthorizedRequest,
  usage: SandboxUsageReceipt,
  config: GatewayConfig,
  obs: GatewayObserver | undefined,
  options: SettleAndRecordOptions = {},
): Promise<void> {
  assertX402V1SettlementSafe(authz, config)
  const settlementBasis = options.settlementBasis ?? 'usage-receipt'
  await markRecoverySettling(authz, usage, settlementBasis, config)
  if (options.usageAlreadyRecorded) await markRecoveryUsageRecorded(authz, config)
  const tokenCost = (
    usage.inputTokens + usage.outputTokens + usage.reasoningTokens + usage.toolTokens
  ) * agent.pricePerTokenUsd
  const totalCost = Math.max(tokenCost, usage.providerCostUsd)
  const ownerEarned = totalCost * (1 - agent.platformFeePercent)
  const platformFee = totalCost * agent.platformFeePercent
  const usageEvent = {
    requestId: authz.requestId,
    agentId: agent.id,
    agentSlug: agent.slug,
    consumerId: authz.consumerId,
    paymentMethod: authz.paymentMethod,
    inputTokens: usage.inputTokens,
    outputTokens: usage.outputTokens,
    reasoningTokens: usage.reasoningTokens,
    toolTokens: usage.toolTokens,
    toolCallCount: usage.toolCallCount,
    providerCostUsd: usage.providerCostUsd,
    totalCostUsd: totalCost,
    ownerEarnedUsd: ownerEarned,
    platformFeeUsd: platformFee,
    durationMs: Date.now() - authz.startMs,
    settlementBasis,
  }
  const ctx: RequestContext = {
    requestId: authz.requestId,
    agentSlug: agent.slug,
    startMs: authz.startMs,
  }
  try {
    if (authz.paymentOperation && config.x402.paymentOperations) {
      const amount = options.paymentAmount ?? actualX402Amount(
        agent.pricePerTokenUsd,
        usage.inputTokens,
        usage.outputTokens,
        usage.reasoningTokens,
        usage.toolTokens,
        config.x402.currencyDecimals,
        usage.providerCostUsd,
      )
      if (options.paymentAlreadySettled) {
        if (
          authz.paymentOperation.state !== 'settled' ||
          authz.paymentOperation.settledAmount !== amount
        ) {
          throw new Error('authoritative payment state does not match finalization')
        }
      } else {
        authz.paymentOperation = await config.x402.paymentOperations.settlePayment(
          authz.paymentOperation,
          { amount, totalCostUsd: totalCost, usage, basis: settlementBasis },
        )
      }
      // Durable settlement happens first. If attribution storage is
      // unavailable, recovery must never refund delivered work.
      if (!options.usageAlreadyRecorded) {
        await config.recordUsage(usageEvent)
        await options.onUsageRecorded?.()
        await markRecoveryUsageRecorded(authz, config)
      }
    } else if (authz.mppChargeOperation) {
      // Generic MPP charge methods confirm before the response. Finalization
      // records attribution only; it never invokes the legacy settlement hook.
      if (!options.usageAlreadyRecorded) {
        await config.recordUsage(usageEvent)
        await options.onUsageRecorded?.()
        await markRecoveryUsageRecorded(authz, config)
      }
    } else {
      // Legacy adapters retain attribution-before-charge because their
      // settlement callback may resolve that usage row.
      if (!options.usageAlreadyRecorded) {
        await config.recordUsage(usageEvent)
        await options.onUsageRecorded?.()
      }
      if (config.settlePayment) {
        await config.settlePayment(
          {
            method: authz.paymentMethod,
            consumerId: authz.consumerId,
            requestId: authz.requestId,
          },
          totalCost,
        )
      }
    }
    await markRecoveryReconciled(authz, config)
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    console.error(`[agent-gateway] settlement failed for ${authz.consumerId}: ${msg}`)
    await obs?.onSettlementError?.(ctx, {
      consumerId: authz.consumerId,
      method: authz.paymentMethod,
      errorMessage: msg,
    })
    throw err
  }
  try {
    await obs?.onRequestComplete?.(ctx, usageEvent)
  } catch (error) {
    console.error(
      `[agent-gateway] completion observer failed for ${authz.requestId}:`,
      error instanceof Error ? error.message : String(error),
    )
  }
}

export type { SettleAndRecordOptions }
