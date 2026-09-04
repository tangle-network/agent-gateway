import type {
  AgentMeta,
  SandboxBox,
  SandboxExecutionBudget,
  SandboxPromptResult,
  SandboxStreamEvent,
  GatewayConfig,
  GatewaySandboxContext,
} from '../types'
import { dispatchSandboxStreamRich, truncateUtf8 } from '../dispatch-sandbox'
import { redactSystemPromptFromOutput } from '../filter'
import { extractTextFromMessage } from './translate'
import {
  attachTaskExecutionReference,
  claimTaskExecution,
  hasExpiredTaskExecution,
  inspectTaskExecution,
  readTaskExecutionReference,
  type SandboxRunControlRef,
} from './execution-fence'
import { readTaskOrigin } from './task-submission-recovery'
import type { TaskStore } from './task-store'
import type { TaskExecutionSource } from './task-methods'
import type { A2ADispatchEvent } from '../dispatch-types'
import type { Task } from './types'

type DetachedSandboxBox = SandboxBox & {
  id: string
  dispatchPrompt: NonNullable<SandboxBox['dispatchPrompt']>
  session: NonNullable<SandboxBox['session']>
}

export function hasDetachedSandbox(box: SandboxBox): box is DetachedSandboxBox {
  return typeof box.id === 'string' && box.id.length > 0 &&
    typeof box.dispatchPrompt === 'function' && typeof box.session === 'function'
}

/** Stable identity for one task turn. Retries must address the same SDK turn. */
export function taskExecutionTurnId(task: Task): string {
  return `${task.id}:turn:${task.history?.length ?? 0}`
}

export interface DetachedTaskSourceDependencies {
  config: GatewayConfig
  taskStore: TaskStore
}

export async function getTaskExecutionSource(
  task: Task,
  deps: DetachedTaskSourceDependencies,
  requestedAgentSlug: string,
): Promise<TaskExecutionSource | undefined> {
  const origin = readTaskOrigin(task)
  const agent = await deps.config.resolveAgent(origin?.agentSlug ?? requestedAgentSlug)
  if (!agent || !agent.enabled) throw new Error('A2A task execution agent is unavailable')
  if (origin && origin.agentId !== agent.id) {
    throw new Error('A2A task execution agent does not match task origin')
  }
  const box = await deps.config.getSandbox(agent)
  if (!hasDetachedSandbox(box)) {
    if (readTaskExecutionReference(task) || (
      task.status.state === 'working' && inspectTaskExecution(task).state === 'valid'
    )) {
      throw new Error('A2A task execution sandbox controls are unavailable')
    }
    return undefined
  }
  let reference = readTaskExecutionReference(task)
  if (!reference) {
    const marker = inspectTaskExecution(task)
    if (marker.state !== 'valid' || task.status.state !== 'working') return undefined
    if (!hasExpiredTaskExecution(task)) return undefined
    const latestUserMessage = [...(task.history ?? [])]
      .reverse()
      .find((message) => message.role === 'user')
    const extracted = latestUserMessage ? extractTextFromMessage(latestUserMessage) : null
    if (!extracted || 'error' in extracted) {
      throw new Error(`A2A task '${task.id}' execution prompt is unavailable`)
    }
    await claimTaskExecution(deps.taskStore, task, marker.marker.requestId)
    const dispatched = await box.dispatchPrompt(extracted.text, {
      sessionId: task.id,
      turnId: taskExecutionTurnId(task),
      systemPrompt: agent.systemPrompt,
      maxOutputTokens: deps.config.defaultOutputTokens ?? 1024,
    })
    const dispatchedReference = exactReference(box, dispatched, 'retry')
    await attachTaskExecutionReference(
      deps.taskStore,
      task,
      marker.marker.requestId,
      dispatchedReference,
    )
    reference = dispatchedReference
  }
  if (box.id !== reference.environmentId) {
    throw new Error('A2A task execution sandbox reference is unavailable')
  }
  const run = reference
  const session = sessionFor(box, run.sessionId)
  const encoder = new TextEncoder()
  const maxOutputBytes = (deps.config.defaultOutputTokens ?? 1024) * 4
  return {
    reference,
    events: (options) => session.events({
      ...(options ?? {}),
      executionId: run.executionId,
    }),
    result: () => session.result({ executionId: run.executionId }),
    interrupt: () => session.interrupt({ executionId: run.executionId }),
    translateText: (value) => redactSystemPromptFromOutput(
      truncateUtf8(value, maxOutputBytes, encoder).text,
      agent.systemPrompt,
    ),
  }
}

interface DetachedDispatchOptions {
  turnId: string
  onExecutionAccepted: (reference: SandboxRunControlRef) => Promise<void>
}

/** Keep detached dispatch private to A2A while reusing the common event adapter. */
export function dispatchDetachedSandboxStreamRich(
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
  sandboxContext?: GatewaySandboxContext,
  options?: DetachedDispatchOptions,
): AsyncIterable<A2ADispatchEvent> {
  if (!options) throw new Error('A2A detached execution identity is unavailable')
  const detachedConfig: GatewayConfig = {
    ...config,
    getSandbox: async (requestedAgent, context) => {
      const box = await config.getSandbox(requestedAgent, context)
      if (!hasDetachedSandbox(box)) {
        throw new Error('A2A production execution requires sandbox dispatchPrompt and session controls')
      }
      return {
        ...box,
        streamPrompt: async function* (message, streamOptions = {}) {
          const budget = streamOptions.executionBudget
          if (!budget) throw new Error('A2A detached execution budget is unavailable')
          const executionSignal = streamOptions.signal ?? signal ?? new AbortController().signal
          const run = await dispatchRun(
            box,
            requestedAgent,
            message,
            consumerId,
            streamOptions.sessionId,
            streamOptions.maxOutputTokens ?? maxOutputTokens ?? config.defaultOutputTokens ?? 1024,
            budget,
            options,
            executionSignal,
          )
          yield* sessionFor(box, run.sessionId).events({
            executionId: run.executionId,
            signal: executionSignal,
          })
        },
      }
    },
  }
  return dispatchSandboxStreamRich(
    agent,
    userMessage,
    consumerId,
    detachedConfig,
    signal,
    sessionId,
    maxOutputTokens,
    onExecutionStart,
    requiresReceipt,
    onSandboxStart,
    maxInputTokens,
    onExecutionHeartbeat,
    sandboxContext,
  )
}

async function dispatchRun(
  box: DetachedSandboxBox,
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  sessionId: string | undefined,
  outputLimit: number,
  executionBudget: SandboxExecutionBudget,
  options: DetachedDispatchOptions,
  executionSignal: AbortSignal,
): Promise<SandboxRunControlRef> {
  const requestedSessionId = sessionId ?? `consumer:${consumerId}`
  const turnId = options.turnId
  const dispatched = await box.dispatchPrompt(userMessage, {
    sessionId: requestedSessionId,
    turnId,
    systemPrompt: agent.systemPrompt,
    maxOutputTokens: outputLimit,
    executionBudget,
    signal: executionSignal,
  })
  const runControlRef = exactReference(box, dispatched, 'dispatch')
  await options.onExecutionAccepted(runControlRef)
  return runControlRef
}

function sessionFor(box: DetachedSandboxBox, sessionId: string) {
  const session = box.session(sessionId)
  if (
    !session ||
    typeof session.events !== 'function' ||
    typeof session.result !== 'function' ||
    typeof session.interrupt !== 'function'
  ) throw new Error('A2A task execution session controls are unavailable')
  return session
}

function exactReference(
  box: DetachedSandboxBox,
  dispatched: Awaited<ReturnType<DetachedSandboxBox['dispatchPrompt']>>,
  operation: string,
): SandboxRunControlRef {
  const sessionId = nonEmptyString(dispatched?.sessionId)
  const executionId = nonEmptyString(dispatched?.executionId) ??
    normalizeRunControlRef(dispatched?.runControlRef)?.executionId
  if (dispatched?.dispatched === false && !executionId) {
    throw new Error(`sandbox detached ${operation} returned no exact execution id`)
  }
  const reference = normalizeRunControlRef(dispatched?.runControlRef) ?? (
    sessionId && executionId
      ? { environmentId: box.id, sessionId, executionId }
      : undefined
  )
  if (
    !sessionId || !executionId || !reference ||
    reference.environmentId !== box.id ||
    reference.sessionId !== sessionId ||
    reference.executionId !== executionId
  ) throw new Error(`sandbox detached ${operation} returned mismatched execution reference`)
  return reference
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function normalizeRunControlRef(value: unknown): SandboxRunControlRef | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined
  const ref = value as Partial<SandboxRunControlRef>
  const environmentId = nonEmptyString(ref.environmentId)
  const sessionId = nonEmptyString(ref.sessionId)
  const executionId = nonEmptyString(ref.executionId)
  if (!environmentId || !sessionId || !executionId) return undefined
  return { environmentId, sessionId, executionId }
}
