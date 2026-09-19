import { apiKeySettlementCostCents, ApiKeyRequestClaimUnavailableError } from './api-keys'
import type { AuthorizedRequest } from './dispatch-types'
import type { GatewayConfig, GatewaySandboxContext, SandboxBox, SandboxExecutionBudget } from './types'

export function apiKeyReservationQuote(authz: AuthorizedRequest, config: GatewayConfig): number | undefined {
  const capped = authz.keyInfo?.spendingLimitCents !== undefined && authz.keyInfo.spendingLimitCents !== null
  if (!capped) return undefined
  if (!config.apiKeyReservationLifecycle) throw new ApiKeyRequestClaimUnavailableError('API key spending reservations are not configured')
  const budget = authz.executionBudget
  return apiKeySettlementCostCents(Math.max(
    (budget.maxInputTokens + budget.maxOutputTokens) * authz.agent.pricePerTokenUsd,
    budget.maxProviderCostUsd,
  ))
}

export async function prepareApiKeyPrompt(
  box: SandboxBox, message: string, consumerId: string, systemPrompt: string | undefined,
  executionBudget: SandboxExecutionBudget, signal: AbortSignal, sessionId?: string,
  context?: GatewaySandboxContext,
) {
  const promptOptions = { sessionId: sessionId ?? `consumer:${consumerId}`, systemPrompt,
    maxOutputTokens: executionBudget.maxOutputTokens, executionBudget, signal }
  if (!context?.apiKeyReservation) return { promptOptions, prepared: undefined }
  if (!box.prepareBudgetedPrompt) throw new ApiKeyBudgetUnsupportedError('This execution adapter cannot enforce API key budgets')
  const prepared = await box.prepareBudgetedPrompt(message, promptOptions)
  if (prepared?.status === 'unsupported') throw new ApiKeyBudgetUnsupportedError(prepared.reason)
  if (prepared?.status !== 'prepared' || typeof prepared.start !== 'function') {
    throw new ApiKeyBudgetUnsupportedError('Execution adapter returned invalid budget preparation')
  }
  return { promptOptions, prepared }
}

export class ApiKeyBudgetUnsupportedError extends Error {
  readonly code = 'api_key.execution_budget_unsupported'
  constructor(message: string) { super(message); this.name = 'ApiKeyBudgetUnsupportedError' }
}

export function assertApiKeyRequestClaim(
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
  if (!claim.allowed && claim.reason !== 'minute' && claim.reason !== 'daily' && claim.reason !== 'spending') {
    throw new ApiKeyRequestClaimUnavailableError('API key request claim reason is invalid')
  }
}

