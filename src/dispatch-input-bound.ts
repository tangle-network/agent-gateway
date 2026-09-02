import type { Context } from 'hono'

import { maximumBillableInputTokens, requiredX402Amount } from './dispatch-pricing'
import type { GatewayState } from './dispatch-types'
import type {
  AgentMeta,
  ApiKeyInfo,
  ChatMessage,
  GatewayConfig,
  PaymentMethod,
  SandboxExecutionBudget,
} from './types'
import { isX402AuthEnabled } from './verify'

export interface InputQuote {
  messageInputBound: number
  maxInputTokens: number
  executionBudget: SandboxExecutionBudget
  requiredPaymentAmount: bigint
}

function quote(
  agent: AgentMeta,
  config: GatewayConfig,
  state: GatewayState,
  maxOutputTokens: number,
  inputTokens: number,
  messageInputBound: number,
): InputQuote {
  const maxProviderCostUsd = state.maxProviderCostUsd ??
    (inputTokens + maxOutputTokens + state.maxReasoningTokens + state.maxToolTokens) * agent.pricePerTokenUsd
  const executionBudget: SandboxExecutionBudget = {
    maxInputTokens: inputTokens,
    maxOutputTokens,
    maxReasoningTokens: state.maxReasoningTokens,
    maxToolTokens: state.maxToolTokens,
    maxToolCalls: state.maxToolCalls,
    maxProviderCostUsd,
  }
  const requiredPaymentAmount = requiredX402Amount(
    agent.pricePerTokenUsd,
    inputTokens,
    maxOutputTokens,
    config.x402.currencyDecimals,
    state.maxReasoningTokens,
    state.maxToolTokens,
    maxProviderCostUsd,
  )
  return {
    messageInputBound,
    maxInputTokens: inputTokens,
    executionBudget,
    requiredPaymentAmount,
  }
}

function invalidBound(c: Context, message: string): Response {
  return c.json(
    {
      error: {
        message,
        type: 'server_error',
        code: 'invalid_input_token_bound',
      },
    },
    503,
  )
}

function invalidPaymentConfiguration(c: Context): Response {
  return c.json(
    {
      error: {
        message: 'Agent payment configuration is invalid',
        type: 'server_error',
        code: 'invalid_payment_configuration',
      },
    },
    503,
  )
}

function unavailableBound(c: Context): Response {
  return c.json(
    {
      error: {
        message: 'Agent input token bound is unavailable',
        type: 'server_error',
        code: 'input_token_bound_unavailable',
      },
    },
    503,
  )
}

export function initialInputQuote(
  c: Context,
  agent: AgentMeta,
  config: GatewayConfig,
  state: GatewayState,
  maxOutputTokens: number,
  messages: ChatMessage[],
): InputQuote | Response {
  const messageInputBound = maximumBillableInputTokens(agent, messages)
  const staticInputBound = config.unauthenticatedInputTokenBound
  if (
    staticInputBound !== undefined &&
    (!Number.isSafeInteger(staticInputBound) || staticInputBound < 0)
  ) {
    return invalidBound(c, 'Agent unauthenticated input token bound is invalid')
  }
  if (config.inputTokenBound && isX402AuthEnabled(config) && staticInputBound === undefined) {
    return invalidBound(c, 'Agent input token bound requires an unauthenticated conservative bound')
  }
  const maxInputTokens = Math.max(messageInputBound, staticInputBound ?? 0)
  try {
    return quote(agent, config, state, maxOutputTokens, maxInputTokens, messageInputBound)
  } catch {
    return invalidPaymentConfiguration(c)
  }
}

function paymentAmount(payload: Record<string, unknown> | null): bigint | undefined {
  const raw = payload?.amount ?? payload?.value
  if (raw === undefined) return undefined
  try {
    return BigInt(String(raw))
  } catch {
    return undefined
  }
}

export async function resolveAuthorizedInputQuote(
  c: Context,
  agent: AgentMeta,
  config: GatewayConfig,
  state: GatewayState,
  maxOutputTokens: number,
  messages: readonly ChatMessage[],
  requestId: string,
  threadId: string | undefined,
  consumerId: string,
  paymentMethod: PaymentMethod,
  keyInfo: ApiKeyInfo | null,
  paymentPayload: Record<string, unknown> | null,
  mppMethod: string | undefined,
  initial: InputQuote,
): Promise<InputQuote | Response> {
  if (!config.inputTokenBound) return initial

  let configuredBound: number
  try {
    configuredBound = await config.inputTokenBound({
      agent,
      messages,
      requestId,
      ...(threadId ? { threadId } : {}),
      consumerId,
      paymentMethod,
      ...(keyInfo?.keyId ? { keyId: keyInfo.keyId } : {}),
      ...(keyInfo?.ownerId ? { ownerId: keyInfo.ownerId } : {}),
    })
  } catch {
    return unavailableBound(c)
  }
  if (!Number.isSafeInteger(configuredBound) || configuredBound < initial.messageInputBound) {
    return invalidBound(c, 'Agent input token bound is invalid')
  }

  let resolved: InputQuote
  try {
    resolved = quote(
      agent,
      config,
      state,
      maxOutputTokens,
      configuredBound,
      initial.messageInputBound,
    )
  } catch {
    return invalidPaymentConfiguration(c)
  }

  const requiresAmountCheck = paymentMethod === 'x402' ||
    (paymentMethod === 'mpp' && mppMethod === 'blueprintevm')
  if (!requiresAmountCheck) return resolved

  const signedAmount = paymentAmount(paymentPayload)
  if (signedAmount !== undefined && signedAmount >= resolved.requiredPaymentAmount) return resolved

  const headers: Record<string, string> = { 'X-Request-Id': requestId }
  if (paymentMethod === 'x402') {
    headers['X-Payment-Required'] = 'spendauth'
  } else if (config.mpp) {
    headers['WWW-Authenticate'] =
      `Payment realm="${config.mpp.realm}", method="${config.mpp.method ?? 'blueprintevm'}"`
  }
  return c.json(
    {
      error: {
        message: 'Payment does not cover the complete provider input bound',
        type: 'payment_required',
        code: 'insufficient_payment',
        required_amount: resolved.requiredPaymentAmount.toString(),
        currency_decimals: config.x402.currencyDecimals ?? 6,
        max_output_tokens: maxOutputTokens,
      },
    },
    { status: 402, headers },
  )
}
