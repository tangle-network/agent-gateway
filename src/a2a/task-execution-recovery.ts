import { maximumBillableInputTokens } from '../dispatch'
import type { GatewayConfig, SandboxPromptResult, SandboxUsageReceipt } from '../types'
import {
  clearTaskExecution,
  hasActiveTaskExecution,
  hasExpiredTaskExecution,
  hasMalformedTaskExecution,
  inspectTaskExecution,
  readTaskExecutionReference,
} from './execution-fence'
import { getTaskExecutionSource } from './detached-sandbox'
import {
  buildFinalizationRecord,
  isTaskFinalizing,
  recoverFinalizationIfNeeded,
  withFinalizationRecord,
} from './task-finalization'
import {
  readPaymentRecoveryMarker,
} from './payment-recovery'
import { extractTextFromMessage, responseTextToArtifact } from './translate'
import { readTaskOrigin, readTaskSubmission } from './task-submission-recovery'
import type { TaskLifecycle } from './task-lifecycle'
import type { TaskStore } from './task-store'
import type { Task } from './types'
import { agentMessage, compareAndSetTask, isTerminal, withStatus } from './task-state'

export interface TaskExecutionRecoveryDependencies {
  config: GatewayConfig
  taskStore: TaskStore
  createLifecycle: () => TaskLifecycle
  deliverPush: (task: Task) => Promise<void>
}

export async function reconcileTaskExecution(
  task: Task,
  deps: TaskExecutionRecoveryDependencies,
  requestedAgentSlug: string,
  allowActive = false,
): Promise<Task> {
  if (
    isTerminal(task.status.state) ||
    task.status.state === 'input-required' ||
    (hasActiveTaskExecution(task) && !allowActive)
  ) return task
  const source = await getTaskExecutionSource(
    task,
    { config: deps.config, taskStore: deps.taskStore },
    requestedAgentSlug,
  )
  if (!source) return task
  const result = await source.result()
  if (result.executionId !== undefined && result.executionId !== source.reference.executionId) {
    throw new Error('A2A task execution result does not match its stored execution')
  }
  const current = await deps.taskStore.get(task.id) ?? task
  if (
    isTerminal(current.status.state) ||
    current.status.state === 'input-required' ||
    isTaskFinalizing(current)
  ) return current
  if (readTaskExecutionReference(current)?.executionId !== source.reference.executionId) return current

  const state = taskStateFromSandboxResult(result)
  if (state === 'working') return current
  const paymentRecoveryId = readPaymentRecoveryMarker(current)?.id
  if (state === 'failed' || state === 'canceled') {
    const next = withStatus(
      current,
      state,
      result.error ? agentMessage(current, result.error) : undefined,
    )
    if (await compareAndSetTask(deps.taskStore, current, next)) {
      await deps.deliverPush(next)
      return next
    }
    return await deps.taskStore.get(task.id) ?? next
  }
  const receipt = promptResultReceipt(result)
  if (paymentRecoveryId) {
    if (!deps.config.paymentRecovery) throw new Error('A2A durable payment recovery is unavailable')
    const recovered = await deps.createLifecycle().payment.recoverDurablePayment(paymentRecoveryId, {
      force: true,
      usage: receipt,
    })
    if (recovered?.state !== 'reconciled') return current
  }

  const inspection = inspectTaskExecution(current)
  if (inspection.state !== 'valid') return current
  const agent = await deps.config.resolveAgent(readTaskOrigin(current)?.agentSlug ?? requestedAgentSlug)
  if (!agent || !agent.enabled) throw new Error('A2A task execution agent is unavailable')
  const submission = readTaskSubmission(current)
  if (!submission || submission.requestId !== inspection.marker.requestId || submission.agentId !== agent.id) {
    throw new Error(`A2A task '${current.id}' execution context is unavailable`)
  }
  const latestUserMessage = [...(current.history ?? [])]
    .reverse()
    .find((message) => message.role === 'user')
  const extracted = latestUserMessage ? extractTextFromMessage(latestUserMessage) : null
  const userMessage = extracted && !('error' in extracted) ? extracted.text : '[recovered A2A task]'
  const maxOutputTokens = deps.config.maxOutputTokens ?? deps.config.defaultOutputTokens ?? 1024
  const maxReasoningTokens = deps.config.executionBudget?.maxReasoningTokens ?? maxOutputTokens
  const maxToolTokens = deps.config.executionBudget?.maxToolTokens ?? maxOutputTokens
  const maxToolCalls = deps.config.executionBudget?.maxToolCalls ?? 8
  const maxInputTokens = maximumBillableInputTokens(agent, userMessage)
  const executionBudget = {
    maxInputTokens,
    maxOutputTokens,
    maxReasoningTokens,
    maxToolTokens,
    maxToolCalls,
    maxProviderCostUsd: deps.config.executionBudget?.maxProviderCostUsd ?? (
      (maxInputTokens + maxOutputTokens + maxReasoningTokens + maxToolTokens) * agent.pricePerTokenUsd
    ),
  }
  const parsedStartMs = Date.parse(current.status.timestamp)
  const authz = {
    agent,
    consumerId: submission.consumerId,
    paymentMethod: 'apikey' as const,
    keyInfo: null,
    userMessage,
    rateLimitRemaining: undefined,
    requestId: inspection.marker.requestId,
    startMs: Number.isFinite(parsedStartMs) ? parsedStartMs : Date.now(),
    maxOutputTokens,
    executionBudget,
    requiredPaymentAmount: 0n,
    paymentPayload: null,
    ...(paymentRecoveryId ? { paymentRecoveryId } : {}),
  }
  const text = result.response ? source.translateText(result.response) : ''
  const finalization = buildFinalizationRecord(
    authz,
    receipt,
    text ? responseTextToArtifact(text, `${current.id}-artifact-0`) : current.artifacts?.[0] ?? null,
    state === 'input-required',
    result.question ?? result.error,
    state === 'input-required' ? 'input-required' : 'completed',
  )
  const finalizingTask = withFinalizationRecord(current, finalization)
  if (!await compareAndSetTask(deps.taskStore, current, finalizingTask)) {
    return await deps.taskStore.get(task.id) ?? current
  }
  return recoverFinalizationIfNeeded(
    finalizingTask,
    deps.createLifecycle().finalization,
    agent.slug,
    { force: true },
  )
}

function taskStateFromSandboxResult(result: SandboxPromptResult): Task['status']['state'] {
  const status = result.status.toLowerCase()
  if (result.success || status === 'success' || status === 'completed') return 'completed'
  if (
    status === 'awaiting_question' ||
    status === 'awaiting_interaction' ||
    status === 'input-required' ||
    status === 'input_required'
  ) return 'input-required'
  if (status === 'running' || status === 'queued' || status === 'working') return 'working'
  if (status === 'canceled' || status === 'cancelled') return 'canceled'
  return 'failed'
}

function promptResultReceipt(result: SandboxPromptResult): SandboxUsageReceipt {
  const inputTokens = result.usage?.inputTokens
  const outputTokens = result.usage?.outputTokens
  if (
    typeof inputTokens !== 'number' || !Number.isSafeInteger(inputTokens) || inputTokens < 0 ||
    typeof outputTokens !== 'number' || !Number.isSafeInteger(outputTokens) || outputTokens < 0
  ) throw new Error('sandbox detached result returned invalid usage')
  const providerCostUsd = result.costUsd ?? 0
  if (!Number.isFinite(providerCostUsd) || providerCostUsd < 0) {
    throw new Error('sandbox detached result returned invalid cost')
  }
  return {
    inputTokens,
    outputTokens,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd,
    budgetEnforced: false,
  }
}

export async function recoverExpiredExecutionIfNeeded(
  task: Task,
  deps: TaskExecutionRecoveryDependencies,
  requestedAgentSlug: string,
): Promise<Task> {
  const malformed = hasMalformedTaskExecution(task)
  if (
    task.status.state !== 'working' ||
    (isTaskFinalizing(task) && !malformed) ||
    (!malformed && !hasExpiredTaskExecution(task))
  ) return task
  if (!malformed) {
    try {
      const reconciled = await reconcileTaskExecution(task, deps, requestedAgentSlug)
      if (reconciled !== task && (
        reconciled.status.state !== 'working' ||
        !hasExpiredTaskExecution(reconciled)
      )) return reconciled
    } catch (error) {
      console.error(
        `[a2a] detached execution reconciliation failed for ${task.id}:`,
        error instanceof Error ? error.message : String(error),
      )
    }
  }
  const inspection = inspectTaskExecution(task)
  const failed: Task = {
    ...withStatus(task, 'failed'),
    metadata: {
      ...(clearTaskExecution(task).metadata ?? {}),
      gatewayExecutionRecovery: {
        error: inspection.state === 'malformed'
          ? `A2A execution marker was malformed: ${inspection.reason}`
          : 'A2A execution lease expired before a task result was stored',
      },
    },
  }
  if (await compareAndSetTask(deps.taskStore, task, failed)) {
    await deps.deliverPush(failed)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? task
}
