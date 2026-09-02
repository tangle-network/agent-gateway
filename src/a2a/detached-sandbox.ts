import type {
  AgentMeta,
  GatewayConfig,
  GatewaySandboxContext,
  SandboxBox,
  SandboxDispatchPromptOptions,
  SandboxDispatchResult,
  SandboxDurableSession,
  SandboxExecutionBudget,
  SandboxPromptResult,
  SandboxStreamEvent,
} from '../types'
import type { A2ADispatchEvent } from '../dispatch-types'
import { dispatchSandboxStreamRich } from '../dispatch-sandbox'
import type { Task } from './types'
export interface DetachedExecutionIdentity {
  sessionId: string
  executionId: string
  turnId: string
  environmentId: string
}
export interface DetachedExecutionHandle {
  events: (options?: { since?: string; signal?: AbortSignal }) => AsyncIterable<SandboxStreamEvent>
  result: () => Promise<SandboxPromptResult>
  interrupt: () => Promise<{ cancelled: boolean }>
}
export function taskExecutionTurnId(task: Task): string {
  return `${task.id}:turn:${task.history?.length ?? 0}`
}
type DetachedSandbox = SandboxBox & {
  id: string
  dispatchPrompt: NonNullable<SandboxBox['dispatchPrompt']>
  session: NonNullable<SandboxBox['session']>
}
type DetachedStreamOptions = Parameters<SandboxBox['streamPrompt']>[1]
export function hasDetachedSandbox(box: SandboxBox): box is DetachedSandbox {
  return typeof box.id === 'string' && box.id.length > 0 && typeof box.dispatchPrompt === 'function' &&
    typeof box.session === 'function'
}
export function dispatchDetachedSandboxStreamRich(
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  config: GatewayConfig,
  signal?: AbortSignal,
  sessionId?: string,
  maxOutputTokens?: number,
  onExecutionStart?: (identity: DetachedExecutionIdentity) => Promise<void>,
  requiresReceipt = config.x402.paymentOperations !== undefined,
  onSandboxStart?: () => void | Promise<void>,
  maxInputTokens?: number,
  onExecutionHeartbeat?: () => Promise<void>,
  sandboxContext?: GatewaySandboxContext,
  options: { turnId?: string } = {},
): AsyncIterable<A2ADispatchEvent> {
  let identity: DetachedExecutionIdentity | undefined
  const detachedConfig: GatewayConfig = {
    ...config,
    getSandbox: async (requestedAgent, context) => {
      const box = await config.getSandbox(requestedAgent, context)
      if (!hasDetachedSandbox(box)) throw new Error('A2A detached sandbox controls are unavailable')
      identity = buildDetachedExecutionIdentity(box.id, sessionId ?? `consumer:${consumerId}`,
        options.turnId ?? context?.requestId ?? sessionId ?? `consumer:${consumerId}`)
      return {
        ...box,
        streamPrompt: (message, streamOptions = {}) => detachedStream(
          box, requestedAgent, message, streamOptions, identity!,
          maxOutputTokens ?? config.defaultOutputTokens ?? 1024,
        ),
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
    async () => {
      if (!identity) throw new Error('A2A detached execution identity is unavailable')
      await onExecutionStart?.(identity)
    },
    requiresReceipt,
    onSandboxStart,
    maxInputTokens,
    onExecutionHeartbeat,
    sandboxContext,
  )
}
export function buildDetachedExecutionIdentity(
  sandboxId: string,
  sessionId: string,
  turnId: string,
): DetachedExecutionIdentity {
  if (!sessionId || !turnId || !sandboxId) throw new Error('detached sandbox execution identity is incomplete')
  return {
    environmentId: sandboxId,
    sessionId,
    turnId,
    executionId: `a2a-execution-${turnId}`,
  }
}
export async function dispatchDetachedExecution(
  box: DetachedSandbox,
  agent: AgentMeta,
  message: string,
  outputLimit: number,
  executionBudget: SandboxExecutionBudget,
  identity: DetachedExecutionIdentity,
): Promise<DetachedExecutionHandle> {
  const session = checkedSession(box.session(identity.sessionId))
  const options: SandboxDispatchPromptOptions = {
    sessionId: identity.sessionId,
    executionId: identity.executionId,
    turnId: identity.turnId,
    systemPrompt: agent.systemPrompt,
    maxOutputTokens: outputLimit,
    executionBudget,
  }
  const dispatched = await box.dispatchPrompt(message, options)
  assertDispatchIdentity(dispatched, identity)
  // `dispatched:false` is an idempotent lookup of the same execution.
  return detachedHandle(session, identity)
}
export function attachDetachedExecution(
  box: DetachedSandbox,
  identity: DetachedExecutionIdentity,
): DetachedExecutionHandle {
  return detachedHandle(checkedSession(box.session(identity.sessionId)), identity)
}
function detachedHandle(
  session: SandboxDurableSession,
  identity: DetachedExecutionIdentity,
): DetachedExecutionHandle {
  return {
    events: (options) => session.events({
      ...(options?.since ? { since: options.since } : {}),
      executionId: identity.executionId,
      ...(options?.signal ? { signal: options.signal } : {}),
    }),
    result: () => session.result({ executionId: identity.executionId }),
    interrupt: () => session.interrupt({ executionId: identity.executionId }),
  }
}
function checkedSession(session: SandboxDurableSession): SandboxDurableSession {
  if (!session || typeof session.events !== 'function' || typeof session.result !== 'function' ||
      typeof session.interrupt !== 'function') throw new Error('sandbox detached session controls are unavailable')
  return session
}
function assertDispatchIdentity(
  dispatched: SandboxDispatchResult,
  identity: DetachedExecutionIdentity,
): void {
  if (!dispatched || typeof dispatched.dispatched !== 'boolean') {
    throw new Error('sandbox detached dispatch did not report dispatched')
  }
  if (dispatched.sessionId !== identity.sessionId) {
    throw new Error('sandbox detached dispatch returned a different session')
  }
  if (
    dispatched.executionId !== undefined &&
    dispatched.executionId !== identity.executionId
  ) {
    throw new Error('sandbox detached dispatch returned a different execution')
  }
  const reference = dispatched.runControlRef
  if (reference !== undefined && (reference.environmentId !== identity.environmentId ||
    reference.sessionId !== identity.sessionId || reference.executionId !== identity.executionId)) {
    throw new Error('sandbox detached dispatch returned a different run reference')
  }
}
async function* detachedStream(
  box: DetachedSandbox,
  agent: AgentMeta,
  message: string,
  options: DetachedStreamOptions = {},
  identity: DetachedExecutionIdentity,
  defaultOutputTokens: number,
): AsyncIterable<SandboxStreamEvent> {
  const executionBudget = options.executionBudget
  if (!executionBudget) throw new Error('A2A detached execution budget is unavailable')
  const outputLimit = options.maxOutputTokens ?? defaultOutputTokens
  if (options.sessionId !== undefined && options.sessionId !== identity.sessionId) {
    throw new Error('A2A detached dispatch returned a different session')
  }
  const execution = await dispatchDetachedExecution(
    box, agent, message, outputLimit, executionBudget, identity,
  )
  yield* execution.events({ signal: options.signal })
}
