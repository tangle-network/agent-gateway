import { redactSystemPromptFromOutput } from './filter'
import type { A2ADispatchEvent } from './dispatch-types'
import {
  estimateTokens,
  maximumBillableInputTokens,
} from './dispatch-pricing'
import type {
  AgentMeta,
  GatewayConfig,
  SandboxExecutionBudget,
  SandboxStreamEvent,
  SandboxUsageReceipt,
} from './types'

export async function* dispatchSandboxStream(
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  config: GatewayConfig,
  signal?: AbortSignal,
  sessionId?: string,
  maxOutputTokens?: number,
): AsyncIterable<string> {
  for await (const event of dispatchSandboxStreamRich(
    agent,
    userMessage,
    consumerId,
    config,
    signal,
    sessionId,
    maxOutputTokens,
  )) {
    if (event.kind === 'text') yield event.delta
  }
}

/**
 * Like `dispatchSandboxStream` but yields a discriminated union so callers can
 * react to `input-required` signals from the sandbox. The sandbox opts in by
 * emitting `{ type: 'input-required', data: { inputRequired: { prompt? } } }`
 * (or by setting `data.inputRequired` on any event); sandboxes that don't emit
 * such events see identical behavior.
 *
 * `sessionId` defaults to `consumer:<id>` matching the existing single-turn
 * path; multi-turn continuations pass an explicit `taskId` so the sandbox can
 * keep per-task conversation memory.
 */
export async function* dispatchSandboxStreamRich(
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  config: GatewayConfig,
  signal?: AbortSignal,
  sessionId?: string,
  maxOutputTokens?: number,
  onExecutionStart?: () => Promise<void>,
  requiresReceipt = config.x402.paymentOperations !== undefined,
  onSandboxStart?: () => void | Promise<void>,
  maxInputTokens?: number,
  onExecutionHeartbeat?: () => Promise<void>,
): AsyncIterable<A2ADispatchEvent> {
  if (signal?.aborted) return
  const box = await config.getSandbox(agent)
  if (signal?.aborted) return
  const outputLimit = maxOutputTokens ?? config.defaultOutputTokens ?? 1024
  if (!Number.isSafeInteger(outputLimit) || outputLimit <= 0) {
    throw new Error('max output tokens must be a positive safe integer')
  }
  let outputBytes = 0
  // Bound untrusted adapters while the final receipt is pending. The receipt
  // remains authoritative for token count, so over-limit output is never sent.
  const maxOutputBytes = outputLimit * 4
  if (!Number.isSafeInteger(maxOutputBytes)) {
    throw new Error('max output token bound exceeds safe integer range')
  }
  const encoder = new TextEncoder()
  let usageParts: Partial<SandboxUsageReceipt> = {}
  let observedReasoningTokens = 0
  let observedToolTokens = 0
  let observedToolCalls = 0
  let legacyOutputText = ''
  const executionController = new AbortController()
  const forwardAbort = () => executionController.abort()
  if (signal?.aborted) return
  signal?.addEventListener('abort', forwardAbort, { once: true })
  const executionBudget: SandboxExecutionBudget = {
    maxInputTokens: maxInputTokens ?? maximumBillableInputTokens(agent, userMessage),
    maxOutputTokens: outputLimit,
    maxReasoningTokens: config.executionBudget?.maxReasoningTokens ?? outputLimit,
    maxToolTokens: config.executionBudget?.maxToolTokens ?? outputLimit,
    maxToolCalls: config.executionBudget?.maxToolCalls ?? 8,
    maxProviderCostUsd: config.executionBudget?.maxProviderCostUsd ?? (
      (maxInputTokens ?? maximumBillableInputTokens(agent, userMessage)) + outputLimit +
        (config.executionBudget?.maxReasoningTokens ?? outputLimit) +
        (config.executionBudget?.maxToolTokens ?? outputLimit)
    ) * agent.pricePerTokenUsd,
  }
  if (executionController.signal.aborted) return
  await onExecutionStart?.()
  if (executionController.signal.aborted) return
  let heartbeatError: unknown
  let heartbeatInFlight: Promise<void> | undefined
  let heartbeatTimer: ReturnType<typeof setInterval> | undefined
  let iterator: AsyncIterator<SandboxStreamEvent> | undefined
  try {
    // This durable handoff is after sandbox acquisition and immediately before
    // the adapter call that may start paid work.
    await onSandboxStart?.()
    const promptStream = box.streamPrompt(userMessage, {
      sessionId: sessionId ?? `consumer:${consumerId}`,
      systemPrompt: agent.systemPrompt,
      maxOutputTokens: outputLimit,
      executionBudget,
      signal: executionController.signal,
    })
    iterator = promptStream[Symbol.asyncIterator]()
    const heartbeatMs = onExecutionHeartbeat
      ? Math.max(100, Math.min(
          Math.floor((config.paymentRecovery?.receiptTimeoutMs ?? 5 * 60_000) / 3),
          5_000,
        ))
      : 0
    if (onExecutionHeartbeat) {
      heartbeatTimer = setInterval(() => {
        if (heartbeatInFlight || heartbeatError !== undefined) return
        heartbeatInFlight = onExecutionHeartbeat()
          .catch((error: unknown) => {
            heartbeatError = error
            executionController.abort()
          })
          .finally(() => {
            heartbeatInFlight = undefined
          })
      }, heartbeatMs)
    }
    while (true) {
      const next = await readSandboxEvent(iterator, executionController.signal)
      if (next === ABORTED_SANDBOX_READ) {
        if (heartbeatError !== undefined) throw heartbeatError
        return
      }
      if (next.done) break
      const event = next.value
      if (event.data?.usage) usageParts = mergeUsage(usageParts, event.data.usage)
      if (event.data?.reasoning?.tokens !== undefined) {
        observedReasoningTokens += nonNegativeSafeInteger(event.data.reasoning.tokens, 'reasoning tokens')
        yield { kind: 'activity' }
      }
      if (event.data?.tool) {
        observedToolCalls += 1
        observedToolTokens +=
          nonNegativeSafeInteger(event.data.tool.inputTokens ?? 0, 'tool input tokens') +
          nonNegativeSafeInteger(event.data.tool.outputTokens ?? 0, 'tool output tokens')
        yield { kind: 'activity' }
      }
      enforceUsageBudget(withObservedUsage(
        usageParts,
        observedReasoningTokens,
        observedToolTokens,
        observedToolCalls,
      ), executionBudget)
      if (
        event.type === 'message.part.updated' &&
        event.data?.part?.type === 'text' &&
        event.data.delta
      ) {
        const remainingBytes = maxOutputBytes - outputBytes
        if (remainingBytes <= 0) throw new Error('sandbox exceeded max output tokens')
        const bounded = truncateUtf8(event.data.delta, remainingBytes, encoder)
        if (bounded.truncated) {
          yield { kind: 'activity' }
          throw new Error('sandbox exceeded max output tokens')
        }
        outputBytes += bounded.bytes
        legacyOutputText += bounded.text
        yield { kind: 'activity' }
        yield { kind: 'text', delta: redactSystemPromptFromOutput(bounded.text, agent.systemPrompt) }
        continue
      }
      if (event.type === 'input-required' || event.data?.inputRequired) {
        const usage = completeUsage(
          usageParts,
          observedReasoningTokens,
          observedToolTokens,
          observedToolCalls,
          userMessage,
          legacyOutputText,
          executionBudget,
          requiresReceipt,
        )
        yield { kind: 'input-required', prompt: event.data?.inputRequired?.prompt }
        // Terminal for the sandbox stream — sandbox SHOULD stop emitting until
        // the gateway dispatches a continuation message with the new user input.
        yield { kind: 'usage', usage }
        return
      }
    }
    const usage = completeUsage(
      usageParts,
      observedReasoningTokens,
      observedToolTokens,
      observedToolCalls,
      userMessage,
      legacyOutputText,
      executionBudget,
      requiresReceipt,
    )
    yield { kind: 'usage', usage }
  } finally {
    if (heartbeatTimer !== undefined) clearInterval(heartbeatTimer)
    const pendingHeartbeat = heartbeatInFlight
    if (pendingHeartbeat) await pendingHeartbeat
    signal?.removeEventListener('abort', forwardAbort)
    if (iterator) await closeSandboxIterator(iterator)
    if (heartbeatError !== undefined && !signal?.aborted) throw heartbeatError
  }
}

const ABORTED_SANDBOX_READ = Symbol('aborted-sandbox-read')

async function readSandboxEvent(
  iterator: AsyncIterator<SandboxStreamEvent>,
  signal?: AbortSignal,
): Promise<IteratorResult<SandboxStreamEvent> | typeof ABORTED_SANDBOX_READ> {
  if (!signal) return iterator.next()
  if (signal.aborted) return ABORTED_SANDBOX_READ
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener('abort', onAbort)
      resolve(ABORTED_SANDBOX_READ)
    }
    signal.addEventListener('abort', onAbort, { once: true })
    iterator.next().then(
      (result) => {
        signal.removeEventListener('abort', onAbort)
        resolve(result)
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort)
        reject(error)
      },
    )
  })
}

const SANDBOX_CLEANUP_TIMEOUT_MS = 50

async function closeSandboxIterator(iterator: AsyncIterator<SandboxStreamEvent>): Promise<void> {
  let closing: PromiseLike<unknown> | undefined
  try {
    const result = iterator.return?.()
    if (result) closing = Promise.resolve(result)
  } catch {
    return
  }
  if (!closing) return
  let timeout: ReturnType<typeof setTimeout> | undefined
  try {
    await Promise.race([
      Promise.resolve(closing).catch(() => undefined),
      new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, SANDBOX_CLEANUP_TIMEOUT_MS)
      }),
    ])
  } finally {
    if (timeout !== undefined) clearTimeout(timeout)
  }
}

function mergeUsage(
  current: Partial<SandboxUsageReceipt>,
  update: Partial<SandboxUsageReceipt>,
): Partial<SandboxUsageReceipt> {
  const merged = { ...current, ...update }
  for (const key of ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount', 'providerCostUsd'] as const) {
    const value = update[key]
    if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
      throw new Error(`sandbox usage field ${key} is invalid`)
    }
    if (value !== undefined && current[key] !== undefined) {
      // Usage events are cumulative receipts. Never let a later partial or
      // final event erase spend observed earlier in the same execution.
      merged[key] = Math.max(current[key]!, value)
    }
  }
  if (current.budgetEnforced === false || update.budgetEnforced === false) {
    merged.budgetEnforced = false
  }
  return merged
}

function withObservedUsage(
  usage: Partial<SandboxUsageReceipt>,
  reasoningTokens: number,
  toolTokens: number,
  toolCallCount: number,
): Partial<SandboxUsageReceipt> {
  return {
    ...usage,
    ...(usage.reasoningTokens !== undefined || reasoningTokens > 0
      ? { reasoningTokens: Math.max(usage.reasoningTokens ?? 0, reasoningTokens) }
      : {}),
    ...(usage.toolTokens !== undefined || toolTokens > 0
      ? { toolTokens: Math.max(usage.toolTokens ?? 0, toolTokens) }
      : {}),
    ...(usage.toolCallCount !== undefined || toolCallCount > 0
      ? { toolCallCount: Math.max(usage.toolCallCount ?? 0, toolCallCount) }
      : {}),
  }
}

function nonNegativeSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`sandbox ${name} is invalid`)
  }
  return value
}

function enforceUsageBudget(
  usage: Partial<SandboxUsageReceipt>,
  budget: SandboxExecutionBudget,
): void {
  if (usage.inputTokens !== undefined && usage.inputTokens > budget.maxInputTokens) {
    throw new Error('sandbox exceeded max input tokens')
  }
  if (usage.outputTokens !== undefined && usage.outputTokens > budget.maxOutputTokens) {
    throw new Error('sandbox exceeded max output tokens')
  }
  if (usage.reasoningTokens !== undefined && usage.reasoningTokens > budget.maxReasoningTokens) {
    throw new Error('sandbox exceeded max reasoning tokens')
  }
  if (usage.toolTokens !== undefined && usage.toolTokens > budget.maxToolTokens) {
    throw new Error('sandbox exceeded max tool tokens')
  }
  if (usage.toolCallCount !== undefined && usage.toolCallCount > budget.maxToolCalls) {
    throw new Error('sandbox exceeded max tool calls')
  }
  if (usage.providerCostUsd !== undefined && usage.providerCostUsd > budget.maxProviderCostUsd) {
    throw new Error('sandbox exceeded max provider cost')
  }
}

function finalizeUsage(
  parts: Partial<SandboxUsageReceipt>,
  budget: SandboxExecutionBudget,
): SandboxUsageReceipt {
  const fields = ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount', 'providerCostUsd', 'budgetEnforced'] as const
  if (fields.some((field) => parts[field] === undefined)) {
    throw new Error('sandbox did not provide a complete usage receipt')
  }
  const usage = parts as SandboxUsageReceipt
  for (const field of ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount'] as const) {
    if (!Number.isSafeInteger(usage[field]) || usage[field] < 0) {
      throw new Error(`sandbox usage field ${field} is invalid`)
    }
  }
  if (!Number.isFinite(usage.providerCostUsd) || usage.providerCostUsd < 0) {
    throw new Error('sandbox usage provider cost is invalid')
  }
  if (typeof usage.budgetEnforced !== 'boolean') {
    throw new Error('sandbox usage budget flag is invalid')
  }
  if (!Number.isSafeInteger(
    usage.inputTokens + usage.outputTokens + usage.reasoningTokens + usage.toolTokens,
  )) {
    throw new Error('sandbox usage token total exceeds safe integer range')
  }
  enforceUsageBudget(usage, budget)
  if (!usage.budgetEnforced) throw new Error('sandbox did not enforce the execution budget')
  return usage
}

function completeUsage(
  parts: Partial<SandboxUsageReceipt>,
  reasoningTokens: number,
  toolTokens: number,
  toolCallCount: number,
  userMessage: string,
  outputText: string,
  budget: SandboxExecutionBudget,
  requiresReceipt: boolean,
): SandboxUsageReceipt {
  const observed = withObservedUsage(parts, reasoningTokens, toolTokens, toolCallCount)
  if (
    !requiresReceipt &&
    Object.keys(parts).length === 0 &&
    reasoningTokens === 0 &&
    toolTokens === 0 &&
    toolCallCount === 0
  ) {
    // Preserve the pre-receipt SandboxBox contract for legacy API-key
    // adapters. Durable payment operations must use provider-enforced usage.
    return {
      inputTokens: estimateTokens(userMessage),
      outputTokens: estimateTokens(outputText),
      reasoningTokens: 0,
      toolTokens: 0,
      toolCallCount: 0,
      providerCostUsd: 0,
      budgetEnforced: false,
    }
  }
  return finalizeUsage(observed, budget)
}

function truncateUtf8(
  value: string,
  maxBytes: number,
  encoder: TextEncoder,
): { text: string; bytes: number; truncated: boolean } {
  const bytes = encoder.encode(value).byteLength
  if (bytes <= maxBytes) return { text: value, bytes, truncated: false }
  let text = ''
  let used = 0
  for (const character of value) {
    const characterBytes = encoder.encode(character).byteLength
    if (used + characterBytes > maxBytes) break
    text += character
    used += characterBytes
  }
  return { text, bytes: used, truncated: true }
}
