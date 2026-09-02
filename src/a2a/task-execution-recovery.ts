import {
  maximumBillableInputTokens,
  type AuthorizedRequest,
} from '../dispatch'
import { redactSystemPromptFromOutput } from '../filter'
import type {
  AgentMeta,
  ChatMessage,
  GatewayConfig,
  GatewaySandboxContext,
  SandboxExecutionBudget,
  SandboxPromptResult,
  SandboxStreamEvent,
  SandboxUsageReceipt,
} from '../types'
import {
  attachDetachedExecution,
  dispatchDetachedExecution,
  hasDetachedSandbox,
  type DetachedExecutionIdentity,
} from './detached-sandbox'
import {
  buildFinalizationRecord,
  recoverFinalizationIfNeeded,
  withFinalizationRecord,
  type TaskFinalizationDependencies,
} from './task-finalization'
import {
  readDetachedExecutionIdentity,
} from './execution-fence'
import { readPaymentRecoveryMarker, releaseTaskPayment } from './payment-recovery'
import {
  clearTaskSubmission,
  readTaskOrigin,
  readTaskSubmission,
} from './task-submission-recovery'
import type { Task } from './types'
import {
  compareAndSetTask,
  isTerminal,
  type TaskStateStore,
  withStatus,
} from './task-state'
import { extractTextFromMessage, responseTextToArtifact } from './translate'
export interface TaskExecutionSource {
  reference: DetachedExecutionIdentity
  events: (options?: { since?: string; signal?: AbortSignal }) => AsyncIterable<SandboxStreamEvent>
  result: () => Promise<SandboxPromptResult>
  interrupt: () => Promise<{ cancelled: boolean }>
  redispatch: () => Promise<void>
  authz: AuthorizedRequest
  agent: AgentMeta
}
export interface TaskExecutionRecoveryDependencies {
  taskStore: TaskStateStore
  config: GatewayConfig
  payment: import('./payment-recovery').PaymentRecoveryDependencies
  finalization: TaskFinalizationDependencies
  deliverPush: (task: Task) => Promise<void>
}
export async function getTaskExecution(
  task: Task,
  requestedAgentSlug: string,
  deps: TaskExecutionRecoveryDependencies,
): Promise<TaskExecutionSource | undefined> {
  const identity = readDetachedExecutionIdentity(task)
  if (!identity) return undefined
  const origin = readTaskOrigin(task)
  const submission = readTaskSubmission(task)
  const agentSlug = origin?.agentSlug ?? submission?.agentSlug ?? requestedAgentSlug
  const agent = await deps.config.resolveAgent(agentSlug)
  if (!agent || !agent.enabled) throw new Error('A2A recovery agent is unavailable')
  if (origin && origin.agentId !== agent.id) {
    throw new Error('A2A execution marker belongs to another agent')
  }
  if (submission && submission.agentId !== agent.id) {
    throw new Error('A2A submission marker belongs to another agent')
  }
  const box = await deps.config.getSandbox(agent, buildSandboxContext(task, submission))
  if (!hasDetachedSandbox(box)) {
    throw new Error('A2A detached sandbox controls are unavailable')
  }
  if (box.id !== identity.environmentId) {
    throw new Error('A2A execution marker belongs to another sandbox')
  }
  const authz = buildRecoveredAuthz(task, agent, submission, deps.config)
  const execution = attachDetachedExecution(box, identity)
  return {
    reference: identity,
    events: execution.events,
    result: execution.result,
    interrupt: execution.interrupt,
    redispatch: async () => {
      await dispatchDetachedExecution(
        box,
        agent,
        authz.userMessage,
        authz.maxOutputTokens,
        authz.executionBudget,
        identity,
      )
    },
    authz,
    agent,
  }
}
export async function reconcileDetachedTask(
  task: Task,
  requestedAgentSlug: string,
  deps: TaskExecutionRecoveryDependencies,
): Promise<Task> {
  if (isTerminal(task.status.state) || task.status.state === 'input-required') return task
  const source = await getTaskExecution(task, requestedAgentSlug, deps)
  if (!source) return task
  let result: SandboxPromptResult
  try {
    result = await source.result()
  } catch (firstError) {
    // A worker can stop after the marker write but before dispatch admission.
    // The stable turn key makes this retry either admit once or return false.
    try {
      await source.redispatch()
      result = await source.result()
    } catch {
      throw firstError
    }
  }
  if (result.executionId !== undefined && result.executionId !== source.reference.executionId) {
    throw new Error('sandbox detached result returned a different execution')
  }
  const current = await deps.taskStore.get(task.id) ?? task
  if (isTerminal(current.status.state) || current.status.state === 'input-required') return current
  if (!result.success && !isInputRequiredResult(result)) {
    const released = await releaseTaskPayment(
      source.authz,
      current,
      deps.payment,
      result.error ?? 'sandbox detached execution failed',
      Boolean(result.response),
    )
    const failedBase = clearTaskSubmission(released)
    const failed: Task = {
      ...withStatus(failedBase, 'failed'),
      metadata: {
        ...(failedBase.metadata ?? {}),
        gatewayExecutionRecovery: {
          error: result.error ?? 'sandbox detached execution failed',
        },
      },
    }
    if (await compareAndSetTask(deps.taskStore, released, failed)) {
      await deps.deliverPush(failed)
      return failed
    }
    return await deps.taskStore.get(task.id) ?? released
  }
  const usage = resultUsage(result, source.authz)
  const response = redactSystemPromptFromOutput(result.response ?? '', source.agent.systemPrompt)
  const inputRequired = isInputRequiredResult(result)
  const finalization = buildFinalizationRecord(
    source.authz,
    usage,
    response ? responseTextToArtifact(response, `${task.id}-artifact-0`) : current.artifacts?.[0] ?? null,
    inputRequired,
    resultQuestionPrompt(result),
    inputRequired ? 'input-required' : 'completed',
  )
  const expiredFinalization = {
    ...finalization,
    lease: { ...finalization.lease, expiresAt: 0 },
  }
  const finalizing = withFinalizationRecord(current, expiredFinalization)
  if (!await compareAndSetTask(deps.taskStore, current, finalizing)) {
    return await deps.taskStore.get(task.id) ?? current
  }
  return recoverFinalizationIfNeeded(finalizing, deps.finalization, source.agent.slug)
}
function buildSandboxContext(
  task: Task,
  submission: ReturnType<typeof readTaskSubmission>,
): GatewaySandboxContext {
  const paymentMethod = submission?.paymentMethod ?? 'apikey'
  return {
    consumerId: submission?.consumerId ?? 'recovered-a2a-task',
    paymentMethod,
    keyInfo: submission?.keyId
      ? { keyId: submission.keyId, consumerId: submission.consumerId }
      : null,
    requestId: submission?.requestId ?? task.id,
    messages: taskHistoryAsChatMessages(task),
    ...(submission?.threadId ? { threadId: submission.threadId } : {}),
  }
}
function buildRecoveredAuthz(
  task: Task,
  agent: AgentMeta,
  submission: ReturnType<typeof readTaskSubmission>,
  config: GatewayConfig,
): AuthorizedRequest {
  const userMessage = latestUserMessage(task)
  const maxOutputTokens = submission?.maxOutputTokens ?? config.defaultOutputTokens ?? 1024
  const maxInputTokens = maximumBillableInputTokens(agent, userMessage)
  const executionBudget = submission?.executionBudget ?? {
    maxInputTokens,
    maxOutputTokens,
    maxReasoningTokens: config.executionBudget?.maxReasoningTokens ?? maxOutputTokens,
    maxToolTokens: config.executionBudget?.maxToolTokens ?? maxOutputTokens,
    maxToolCalls: config.executionBudget?.maxToolCalls ?? 8,
    maxProviderCostUsd: config.executionBudget?.maxProviderCostUsd ?? (
      (maxInputTokens + maxOutputTokens +
        (config.executionBudget?.maxReasoningTokens ?? maxOutputTokens) +
        (config.executionBudget?.maxToolTokens ?? maxOutputTokens)) * agent.pricePerTokenUsd
    ),
  } satisfies SandboxExecutionBudget
  const paymentRecovery = readPaymentRecoveryMarker(task)
  return {
    agent,
    consumerId: submission?.consumerId ?? 'recovered-a2a-task',
    paymentMethod: submission?.paymentMethod ?? 'apikey',
    keyInfo: null,
    userMessage,
    rateLimitRemaining: undefined,
    requestId: submission?.requestId ?? task.id,
    messages: taskHistoryAsChatMessages(task),
    startMs: Date.parse(task.status.timestamp) || Date.now(),
    maxOutputTokens,
    executionBudget,
    requiredPaymentAmount: 0n,
    paymentPayload: null,
    ...(paymentRecovery ? { paymentRecoveryId: paymentRecovery.id } : {}),
  }
}
function resultUsage(
  result: SandboxPromptResult,
  authz: AuthorizedRequest,
): SandboxUsageReceipt {
  const usage = result.usage as Partial<SandboxUsageReceipt> | undefined
  if (usage && hasCompleteUsage(usage)) {
    if ((authz.paymentMethod === 'x402' || authz.paymentMethod === 'mpp') && !usage.budgetEnforced) {
      throw new Error('sandbox detached result did not enforce the execution budget')
    }
    return usage
  }
  if (authz.paymentMethod === 'x402' || authz.paymentMethod === 'mpp') {
    throw new Error('sandbox detached result did not provide a complete usage receipt')
  }
  const response = result.response ?? ''
  return {
    inputTokens: maximumBillableInputTokens(authz.agent, authz.userMessage),
    outputTokens: Math.ceil(response.length / 4),
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd: 0,
    budgetEnforced: false,
  }
}
function hasCompleteUsage(usage: Partial<SandboxUsageReceipt>): usage is SandboxUsageReceipt {
  return [
    usage.inputTokens,
    usage.outputTokens,
    usage.reasoningTokens,
    usage.toolTokens,
    usage.toolCallCount,
  ].every((value) => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) &&
    typeof usage.providerCostUsd === 'number' && Number.isFinite(usage.providerCostUsd) &&
    usage.providerCostUsd >= 0 && typeof usage.budgetEnforced === 'boolean'
}
function resultQuestionPrompt(result: SandboxPromptResult): string | undefined {
  const question = result.question as unknown as { prompt?: unknown } | undefined
  return typeof question?.prompt === 'string' ? question.prompt : undefined
}

function isInputRequiredResult(result: SandboxPromptResult): boolean {
  return result.status === 'awaiting_question' ||
    result.status === 'awaiting_interaction' ||
    result.status === 'blocked_on_approval' ||
    result.status === 'awaiting_plan_decision'
}
function latestUserMessage(task: Task): string {
  const message = [...(task.history ?? [])].reverse().find((entry) => entry.role === 'user')
  if (!message) return ''
  const extracted = extractTextFromMessage(message)
  return 'error' in extracted ? '' : extracted.text
}

function taskHistoryAsChatMessages(task: Task): ChatMessage[] {
  return (task.history ?? []).flatMap((message) => {
    const extracted = extractTextFromMessage(message)
    if ('error' in extracted) return []
    return [{
      role: message.role === 'agent' ? 'assistant' : 'user',
      content: extracted.text,
    }]
  })
}
