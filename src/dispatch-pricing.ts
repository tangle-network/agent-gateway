import type { AgentMeta, ChatMessage } from './types'

function decimalFraction(value: number): { numerator: bigint; denominator: bigint } {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error('agent pricePerTokenUsd must be a finite non-negative number')
  }
  const [mantissa, exponentText] = value.toString().toLowerCase().split('e')
  const exponent = exponentText ? Number(exponentText) : 0
  const [whole, fraction = ''] = mantissa.split('.')
  let numerator = BigInt(`${whole}${fraction}`)
  let scale = fraction.length - exponent
  if (scale < 0) {
    numerator *= 10n ** BigInt(-scale)
    scale = 0
  }
  return { numerator, denominator: 10n ** BigInt(scale) }
}

function amountForTokens(
  pricePerTokenUsd: number,
  tokenCount: number,
  currencyDecimals: number,
  providerCostUsd: number,
): bigint {
  const { numerator, denominator } = decimalFraction(pricePerTokenUsd)
  const scaled = BigInt(tokenCount) * numerator * 10n ** BigInt(currencyDecimals)
  const tokenAmount = (scaled + denominator - 1n) / denominator
  const provider = providerCostUsd === 0
    ? { numerator: 0n, denominator: 1n }
    : decimalFraction(providerCostUsd)
  const providerScaled = provider.numerator * 10n ** BigInt(currencyDecimals)
  const providerAmount = (providerScaled + provider.denominator - 1n) / provider.denominator
  return tokenAmount > providerAmount ? tokenAmount : providerAmount
}

/** Exact base-unit reservation required to cover the request's token ceiling. */
export function requiredX402Amount(
  pricePerTokenUsd: number,
  inputTokens: number,
  maxOutputTokens: number,
  currencyDecimals = 6,
  maxReasoningTokens = 0,
  maxToolTokens = 0,
  maxProviderCostUsd = 0,
): bigint {
  if (!Number.isSafeInteger(inputTokens) || inputTokens < 0) {
    throw new Error('input token estimate must be a non-negative safe integer')
  }
  if (!Number.isSafeInteger(maxOutputTokens) || maxOutputTokens <= 0) {
    throw new Error('max output tokens must be a positive safe integer')
  }
  if (!Number.isInteger(currencyDecimals) || currencyDecimals < 0 || currencyDecimals > 18) {
    throw new Error('x402 currencyDecimals must be an integer between 0 and 18')
  }
  for (const [name, value] of [
    ['maxReasoningTokens', maxReasoningTokens],
    ['maxToolTokens', maxToolTokens],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a non-negative safe integer`)
  }
  if (!Number.isFinite(maxProviderCostUsd) || maxProviderCostUsd < 0) {
    throw new Error('maxProviderCostUsd must be finite and non-negative')
  }
  const tokenCount = inputTokens + maxOutputTokens + maxReasoningTokens + maxToolTokens
  if (!Number.isSafeInteger(tokenCount)) throw new Error('token budget exceeds safe integer range')
  return amountForTokens(pricePerTokenUsd, tokenCount, currencyDecimals, maxProviderCostUsd)
}

export function actualX402Amount(
  pricePerTokenUsd: number,
  inputTokens: number,
  outputTokens: number,
  reasoningTokens: number,
  toolTokens: number,
  currencyDecimals = 6,
  providerCostUsd = 0,
): bigint {
  return amountForTokens(
    pricePerTokenUsd,
    inputTokens + outputTokens + reasoningTokens + toolTokens,
    currencyDecimals,
    providerCostUsd,
  )
}

/** Token estimate matching the existing chat-completions handler (4 chars ≈ 1 token). */
export function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4)
}

/** Include the host-owned system prompt because the provider bills it too. */
export function estimateBillableInputTokens(agent: AgentMeta, userMessage: string): number {
  return estimateTokens(userMessage) + estimateTokens(agent.systemPrompt ?? '')
}

/** A tokenizer cannot emit more tokens than the UTF-8 bytes it consumes. */
export function maximumBillableInputTokens(agent: AgentMeta, userMessage: string): number
export function maximumBillableInputTokens(agent: AgentMeta, messages: readonly ChatMessage[]): number
export function maximumBillableInputTokens(
  agent: AgentMeta,
  userMessageOrMessages: string | readonly ChatMessage[],
): number {
  const encoder = new TextEncoder()
  const prompt = typeof userMessageOrMessages === 'string'
    ? userMessageOrMessages
    : JSON.stringify(userMessageOrMessages)
  return encoder.encode(prompt).byteLength + encoder.encode(agent.systemPrompt ?? '').byteLength
}
