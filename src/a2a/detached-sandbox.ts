import type {
  AgentMeta,
  GatewaySandboxContext,
  SandboxBox,
  SandboxExecutionBudget,
  SandboxRunControlRef,
  SandboxStreamEvent,
} from '../types'

export type DetachedSandboxBox = SandboxBox & {
  id: string
  dispatchPrompt: NonNullable<SandboxBox['dispatchPrompt']>
  session: NonNullable<SandboxBox['session']>
}

export function hasDetachedSandbox(box: SandboxBox): box is DetachedSandboxBox {
  return typeof box.id === 'string' && box.id.length > 0 &&
    typeof box.dispatchPrompt === 'function' && typeof box.session === 'function'
}

export async function openDetachedSandboxStream(
  box: DetachedSandboxBox,
  agent: AgentMeta,
  userMessage: string,
  consumerId: string,
  sessionId: string | undefined,
  outputLimit: number,
  executionBudget: SandboxExecutionBudget,
  sandboxContext: GatewaySandboxContext | undefined,
  options: {
    turnId?: string
    onExecutionAccepted?: (reference: SandboxRunControlRef) => Promise<void>
  },
  executionSignal: AbortSignal,
): Promise<AsyncIterable<SandboxStreamEvent>> {
  const requestedSessionId = sessionId ?? `consumer:${consumerId}`
  const turnId = options.turnId ?? sandboxContext?.requestId ?? requestedSessionId
  const session = box.session(requestedSessionId)
  assertSessionControls(session)
  const dispatched = await box.dispatchPrompt(userMessage, {
    sessionId: requestedSessionId,
    turnId,
    systemPrompt: agent.systemPrompt,
    maxOutputTokens: outputLimit,
    executionBudget,
    signal: executionSignal,
  })
  const resolvedSessionId = nonEmptyString(dispatched.sessionId)
  if (!resolvedSessionId) throw new Error('sandbox detached dispatch returned no session id')
  const resolvedSession = resolvedSessionId === requestedSessionId
    ? session
    : box.session(resolvedSessionId)
  assertSessionControls(resolvedSession)
  const resolvedExecutionId = nonEmptyString(dispatched.executionId) ??
    normalizeRunControlRef(dispatched.runControlRef)?.executionId
  const runControlRef = dispatched.runControlRef === undefined
    ? resolvedExecutionId && {
        environmentId: box.id,
        sessionId: resolvedSessionId,
        executionId: resolvedExecutionId,
      }
    : normalizeRunControlRef(dispatched.runControlRef)
  if (
    !resolvedExecutionId ||
    !runControlRef ||
    runControlRef.environmentId !== box.id ||
    runControlRef.sessionId !== resolvedSessionId ||
    runControlRef.executionId !== resolvedExecutionId
  ) throw new Error('sandbox detached dispatch returned mismatched execution reference')
  await options.onExecutionAccepted?.(runControlRef)
  return resolvedSession.events({ executionId: resolvedExecutionId, signal: executionSignal })
}

function assertSessionControls(
  session: ReturnType<DetachedSandboxBox['session']> | undefined,
): asserts session is ReturnType<DetachedSandboxBox['session']> {
  if (
    !session ||
    typeof session.events !== 'function' ||
    typeof session.result !== 'function' ||
    typeof session.interrupt !== 'function'
  ) throw new Error('sandbox detached session controls are unavailable')
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
