/**
 * A2A JSON-RPC method dispatcher. Both `message/send` and `message/stream`
 * route through the shared `authenticateAndGuard` + `dispatchSandboxStream`
 * + `settleAndRecord` pipeline so every protocol surface gets the same
 * payment, rate-limit, injection, and authorization guarantees.
 *
 * State the dispatcher owns:
 *   - the task store (durable record of every task we accepted)
 *   - an in-process map of active AbortControllers so `tasks/cancel` can
 *     interrupt a still-running `dispatchSandboxStream`
 */

import type { Context } from 'hono'

import {
  type AuthorizedRequest,
  type GatewayState,
  authenticateAndGuard,
  claimPayment,
  estimateTokens,
} from '../dispatch'
import type {
  AgentMeta,
  ChatMessage,
  GatewayConfig,
  SandboxPromptResult,
  SandboxUsageReceipt,
} from '../types'
import { buildAgentCard } from './agent-card'
import {
  clearTaskExecution,
  hasExpiredTaskExecution,
  hasMalformedTaskExecution,
  inspectTaskExecution,
  hasActiveTaskExecution,
  readTaskExecutionReference,
} from './execution-fence'
import { executeMessageSend } from './message-send-execution'
import { executeMessageStream } from './message-stream-execution'
import { fail, ok, parseEnvelope } from './jsonrpc'
import {
  type PushNotificationStore,
} from './push-notifications'
import { type TaskStore } from './task-store'
import { extractTextFromMessage, responseTextToArtifact } from './translate'
import {
  A2A_ERROR_CODES,
  type JSONRPCRequest,
  type Message,
  type MessageSendParams,
  type Task,
} from './types'
import {
  attachPaymentRecoveryMarker,
  clearPaymentRecoveryMarker,
  hasPaymentReleaseRecovery,
  preservePaymentRecoveryMarker,
  readPaymentRecoveryMarker,
  releaseTaskPayment,
  recoverPaymentMarkerIfNeeded,
  recoverPaymentReleaseIfNeeded,
  retainPaymentRecoveryMarker,
} from './payment-recovery'
import {
  isTaskFinalizing,
  recoverFinalizationIfNeeded,
} from './task-finalization'
import {
  clearTaskSubmission,
  readTaskOrigin,
  readTaskSubmission,
  recoverSubmissionIfNeeded,
  withTaskOrigin,
  withTaskSubmission,
} from './task-submission-recovery'
import { deliverTaskPush, type PushDeliveryDependencies } from './task-push-delivery'
import {
  TaskCancellationRegistry,
} from './task-cancellation'
import {
  createTaskLifecycle as buildTaskLifecycle,
  type TaskLifecycle,
} from './task-lifecycle'
import {
  handleTasksCancel,
  handleTasksGet,
  handleTasksResubscribe,
  type TaskExecutionSource,
  type TaskMethodDependencies,
} from './task-methods'
import {
  handlePushDelete,
  handlePushGet,
  handlePushList,
  handlePushSet,
  type PushConfigMethodDependencies,
} from './push-config-methods'
import {
  compareAndSetTask,
  cryptoRandomId,
  agentMessage,
  isTerminal,
  shouldPreserveTask,
  nowIso,
  withStatus,
} from './task-state'

export interface A2AHandlerDeps {
  config: GatewayConfig
  state: GatewayState
  taskStore: TaskStore
  pushStore?: PushNotificationStore
}

const MAX_A2A_BODY_BYTES = 64 * 1024

class RequestBodyTooLargeError extends Error {}

function createTaskLifecycle(deps: A2AHandlerDeps): TaskLifecycle {
  return buildTaskLifecycle({
    taskStore: deps.taskStore,
    config: deps.config,
    state: deps.state,
    deliverPush: (task) => maybeDeliverPush(task, deps),
  })
}

function buildTaskMethodDependencies(
  deps: A2AHandlerDeps,
  cancels: TaskCancellationRegistry,
): TaskMethodDependencies {
  const lifecycle = createTaskLifecycle(deps)
  return {
    taskStore: deps.taskStore,
    payment: lifecycle.payment,
    cancels,
    authorizeTaskAccess: (c, req, task) => authorizeTaskAccess(c, req, task, deps),
    recoverTask: (task, requestedAgentSlug) =>
      recoverTaskIfNeeded(task, deps, requestedAgentSlug),
    deliverPush: (task) => maybeDeliverPush(task, deps),
    getTaskExecution: (task, requestedAgentSlug) =>
      getTaskExecutionSource(task, deps, requestedAgentSlug),
    reconcileTask: (task, requestedAgentSlug, allowActive) =>
      reconcileTaskExecution(task, deps, requestedAgentSlug, allowActive),
  }
}

function buildPushConfigMethodDependencies(
  deps: A2AHandlerDeps,
): PushConfigMethodDependencies {
  return {
    taskStore: deps.taskStore,
    pushStore: deps.pushStore,
    demoMode: deps.config.x402.demoMode === true,
    urlValidator: deps.config.a2a?.pushUrlValidator,
    authorizeTaskAccess: (c, req, task) => authorizeTaskAccess(c, req, task, deps),
  }
}

export function createA2AHandlers(deps: A2AHandlerDeps) {
  const runtimeDeps: A2AHandlerDeps = {
    ...deps,
    taskStore: normalizeTaskStore(deps.taskStore, deps.config.x402.demoMode === true),
  }
  const cancels = new TaskCancellationRegistry()

  // GET /:slug/.well-known/agent.json
  const handleAgentCard = async (c: Context): Promise<Response> => {
    const slug = c.req.param('slug')
    if (!slug) return c.json({ error: 'slug required' }, 400)
    const agent = await deps.config.resolveAgent(slug)
    if (!agent || !agent.enabled) {
      return c.json({ error: 'Agent not found or not published' }, 404)
    }
    const url = new URL(c.req.url)
    const agentUrl = `${url.origin}${url.pathname.replace(/\/\.well-known\/agent\.json$/, '')}`
    return c.json(buildAgentCard(agent, deps.config, agentUrl))
  }

  // POST /:slug — JSON-RPC dispatcher
  const handleJsonRpc = async (c: Context): Promise<Response> => {
    const slug = c.req.param('slug')
    if (!slug) {
      return c.json(fail(null, A2A_ERROR_CODES.INVALID_REQUEST, 'slug required'), 400)
    }

    // Body size limit (DoS prevention) — mirrors the OpenAI-compat handler.
    const contentLength = Number.parseInt(c.req.header('Content-Length') ?? '0', 10)
    if (contentLength > MAX_A2A_BODY_BYTES) {
      return c.json(fail(null, A2A_ERROR_CODES.INVALID_REQUEST, 'request body too large (max 64KB)'), 413)
    }

    let raw: unknown
    try {
      raw = await readJsonBody(c.req.raw)
    } catch (error) {
      if (error instanceof RequestBodyTooLargeError) {
        return c.json(
          fail(null, A2A_ERROR_CODES.INVALID_REQUEST, 'request body too large (max 64KB)'),
          413,
        )
      }
      return c.json(fail(null, A2A_ERROR_CODES.PARSE_ERROR, 'invalid JSON'), 400)
    }
    const parsed = parseEnvelope(raw)
    if ('code' in parsed) {
      return c.json(fail(parsed.id, parsed.code, parsed.message), 400)
    }

    switch (parsed.method) {
      case 'message/send':
        return handleMessageSend(c, slug, parsed, runtimeDeps, cancels)
      case 'message/stream':
        return handleMessageStream(c, slug, parsed, runtimeDeps, cancels)
      case 'tasks/get':
        return handleTasksGet(c, parsed, buildTaskMethodDependencies(runtimeDeps, cancels))
      case 'tasks/cancel':
        return handleTasksCancel(c, parsed, buildTaskMethodDependencies(runtimeDeps, cancels))
      case 'tasks/resubscribe':
        return handleTasksResubscribe(c, parsed, buildTaskMethodDependencies(runtimeDeps, cancels))
      case 'tasks/pushNotificationConfig/set':
        return handlePushSet(c, parsed, buildPushConfigMethodDependencies(runtimeDeps))
      case 'tasks/pushNotificationConfig/get':
        return handlePushGet(c, parsed, buildPushConfigMethodDependencies(runtimeDeps))
      case 'tasks/pushNotificationConfig/list':
        return handlePushList(c, parsed, buildPushConfigMethodDependencies(runtimeDeps))
      case 'tasks/pushNotificationConfig/delete':
        return handlePushDelete(c, parsed, buildPushConfigMethodDependencies(runtimeDeps))
      default:
        return c.json(
          fail(parsed.id, A2A_ERROR_CODES.METHOD_NOT_FOUND, `unknown method '${parsed.method}'`),
        )
    }
  }

  return { handleAgentCard, handleJsonRpc }
}

// ── message/send (synchronous) ────────────────────────────────────────────

async function handleMessageSend(
  c: Context,
  slug: string,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
  cancels: TaskCancellationRegistry,
): Promise<Response> {
  const guard = await guardMessageRequest(c, slug, req, deps)
  if (guard instanceof Response) return guard
  const { authz, task } = guard
  setPaymentResponseHeaders(c, authz)
  const controller = cancels.register(task.id)
  const lifecycle = createTaskLifecycle(deps)
  try {
    return await executeMessageSend(
      c,
      req,
      { taskStore: deps.taskStore, config: deps.config, ...lifecycle },
      authz,
      task,
      controller.signal,
    )
  } finally {
    cancels.clear(task.id)
  }
}

// ── message/stream (SSE) ──────────────────────────────────────────────────

async function handleMessageStream(
  c: Context,
  slug: string,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
  cancels: TaskCancellationRegistry,
): Promise<Response> {
  const guard = await guardMessageRequest(c, slug, req, deps)
  if (guard instanceof Response) return guard
  const { authz, task } = guard
  setPaymentResponseHeaders(c, authz)
  return executeMessageStream(
    c,
    req,
    {
      taskStore: deps.taskStore,
      config: deps.config,
      lifecycle: createTaskLifecycle(deps),
      cancels,
      deliverPush: (streamTask) => maybeDeliverPush(streamTask, deps),
      reportStreamError: async (streamAuthz, error) => {
        await deps.state.obs?.onStreamError?.(
          {
            requestId: streamAuthz.requestId,
            agentSlug: streamAuthz.agent.slug,
            startMs: streamAuthz.startMs,
          },
          {
            consumerId: streamAuthz.consumerId,
            errorMessage: error instanceof Error ? error.message : String(error),
          },
        )
      },
    },
    authz,
    task,
  )
}

// ── Shared message-send setup (auth + task allocation) ────────────────────

interface GuardSuccess {
  authz: AuthorizedRequest
  task: Task
}

async function guardMessageRequest(
  c: Context,
  slug: string,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<GuardSuccess | Response> {
  const params = req.params as MessageSendParams | undefined
  if (!params || !params.message) {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.message required'))
  }
  const extracted = extractTextFromMessage(params.message)
  if ('error' in extracted) {
    return c.json(fail(req.id, extracted.error.code, extracted.error.message))
  }

  let billingMessages: ChatMessage[] = [{ role: 'user', content: extracted.text }]
  let knownTaskContextId: string | undefined
  if (typeof params.message.taskId === 'string') {
    const storedForQuote = await deps.taskStore.get(params.message.taskId)
    if (!storedForQuote) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.message.taskId}' not found`),
      )
    }
    const accessError = await authorizeTaskAccess(c, req, storedForQuote, deps)
    if (accessError) return accessError
    const quotedTask = await recoverTaskIfNeeded(storedForQuote, deps, slug)
    if (quotedTask.status.state === 'input-required') {
      billingMessages = [
        ...taskHistoryAsChatMessages(quotedTask),
        { role: 'user', content: extracted.text },
      ]
    }
    knownTaskContextId = quotedTask.contextId
  }

  const messageContextId = typeof params.message.contextId === 'string'
    ? params.message.contextId
    : undefined
  const requestedThreadId = deps.config.conversationMode === 'thread'
    ? knownTaskContextId ?? messageContextId
    : undefined

  const guard = await authenticateAndGuard(
    c,
    slug,
    billingMessages,
    deps.config,
    deps.state,
    undefined,
    requestedThreadId,
  )
  if (guard instanceof Response) return guard
  const authz = guard
  // The provider session receives only the new turn. The quote above covers
  // every retained message and the configured hidden provider context.
  authz.userMessage = extracted.text

  // Multi-turn continuation: if the caller addressed an existing task that is
  // currently in `input-required`, append the new message and reserve it as
  // `submitted`. The handler transitions it to `working` only after payment
  // succeeds, so cancellation during payment cannot start sandbox work.
  // A supplied taskId must identify an existing task. Only `input-required`
  // tasks accept a follow-up message.
  if (typeof params.message.taskId === 'string') {
    const storedExisting = await deps.taskStore.get(params.message.taskId)
    if (!storedExisting) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.message.taskId}' not found`),
      )
    }
    const accessError = await authorizeTaskAccess(c, req, storedExisting, deps)
    if (accessError) return accessError
    const existing = await recoverTaskIfNeeded(storedExisting, deps, slug)
    if (existing.status.state !== 'input-required') {
      return c.json(
        fail(
          req.id,
          A2A_ERROR_CODES.UNSUPPORTED_OPERATION,
          `task '${existing.id}' is in state '${existing.status.state}'; only 'input-required' tasks accept follow-up messages`,
        ),
      )
    }
    const appendedMessage: Message = {
      ...params.message,
      taskId: existing.id,
      contextId: existing.contextId,
    }
    const continued: Task = {
      ...existing,
      status: { state: 'submitted', timestamp: nowIso() },
      history: [...(existing.history ?? []), appendedMessage],
      metadata: withTaskSubmission(existing.metadata, authz),
    }
    if (!await compareAndSetTask(deps.taskStore, existing, continued)) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${existing.id}' changed before continuation`),
      )
    }
    const claimedTask = await claimTaskPayment(c, req, continued, authz, deps, existing)
    if (claimedTask instanceof Response) return claimedTask
    return { authz, task: claimedTask }
  }

  const taskId = `task_${cryptoRandomId()}`
  const contextId = knownTaskContextId ?? (
    deps.config.conversationMode === 'thread'
      ? authz.threadId ?? `ctx_${cryptoRandomId()}`
      : messageContextId ?? `ctx_${cryptoRandomId()}`
  )
  const initialMessage = {
    ...params.message,
    taskId,
    contextId,
  }
  const task: Task = {
    kind: 'task',
    id: taskId,
    contextId,
    status: { state: 'submitted', timestamp: nowIso() },
    history: [initialMessage],
    metadata: withTaskSubmission(withTaskOrigin(undefined, authz.agent), authz),
  }
  if (!await createTask(deps.taskStore, task)) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' already exists`),
    )
  }
  const claimedTask = await claimTaskPayment(c, req, task, authz, deps)
  if (claimedTask instanceof Response) return claimedTask
  return { authz, task: claimedTask }
}

async function claimTaskPayment(
  c: Context,
  req: JSONRPCRequest,
  task: Task,
  authz: AuthorizedRequest,
  deps: A2AHandlerDeps,
  paymentFailureTask?: Task,
): Promise<Response | Task> {
  const lifecycle = createTaskLifecycle(deps)
  let paymentTask = task
  try {
    await claimPayment(authz, deps.config, deps.state, {
      onRecoveryPrepared: async (recoveryId) => {
        paymentTask = await attachPaymentRecoveryMarker(
          deps.taskStore,
          paymentTask,
          recoveryId,
        )
      },
    })
  } catch {
    let recoveryTask = paymentTask
    try {
      recoveryTask = await retainPaymentRecoveryMarker(
        deps.taskStore,
        paymentTask,
        authz.paymentRecoveryId,
      )
    } catch (error) {
      console.error(
        `[a2a] failed to attach payment recovery to ${task.id}:`,
        error instanceof Error ? error.message : String(error),
      )
    }
    const releasedTask = await releaseTaskPayment(
      authz,
      recoveryTask,
      lifecycle.payment,
      'payment authorization failed',
      false,
    )
    const cleanedReleasedTask = clearTaskSubmission(releasedTask)
    const hasReleaseRecord = hasPaymentReleaseRecovery(cleanedReleasedTask)
    const failed = hasReleaseRecord
      ? isTerminal(cleanedReleasedTask.status.state)
        ? cleanedReleasedTask
        : withStatus(cleanedReleasedTask, 'failed')
      : paymentFailureTask
        ? preservePaymentRecoveryMarker(paymentFailureTask, cleanedReleasedTask)
        : isTerminal(cleanedReleasedTask.status.state)
          ? cleanedReleasedTask
          : withStatus(cleanedReleasedTask, 'failed')
    try {
      if (await compareAndSetTask(deps.taskStore, releasedTask, failed) && isTerminal(failed.status.state)) {
        await maybeDeliverPush(failed, deps)
      }
    } catch (taskError) {
      console.error(
        `[a2a] failed to persist payment-failed task ${task.id}:`,
        taskError instanceof Error ? taskError.message : String(taskError),
      )
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'Payment authorization failed'))
  }

  const current = await deps.taskStore.get(task.id)
  if (current && JSON.stringify(current) === JSON.stringify(paymentTask)) {
    return paymentTask
  }
  {
    const changedTask = current ?? paymentTask
    let recoveryTask = changedTask
    try {
      recoveryTask = await retainPaymentRecoveryMarker(
        deps.taskStore,
        changedTask,
        authz.paymentRecoveryId,
      )
    } catch (error) {
      console.error(
        `[a2a] failed to retain payment recovery after task race for ${task.id}:`,
        error instanceof Error ? error.message : String(error),
      )
    }
    const released = await releaseTaskPayment(
      authz,
      recoveryTask,
      lifecycle.payment,
      'A2A task changed during payment confirmation',
      false,
    )
    if (released.status.state === 'canceled') return c.json(ok(req.id, released))
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' changed during payment confirmation`),
    )
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────

const EXECUTION_RECOVERY_METADATA_KEY = 'gatewayExecutionRecovery'

function setPaymentResponseHeaders(c: Context, authz: AuthorizedRequest): void {
  if (authz.mppChargeOperation) {
    c.header('Payment-Receipt', authz.mppChargeOperation.receipt)
  }
  if (authz.paymentRecoveryId) {
    c.header('X-Payment-Operation-Id', authz.paymentRecoveryId)
  }
}

async function authorizeTaskAccess(
  c: Context,
  req: JSONRPCRequest,
  task: Task,
  deps: A2AHandlerDeps,
): Promise<Response | undefined> {
  const requestedAgentSlug = c.req.param('slug') ?? ''
  const origin = readTaskOrigin(task)
  if (origin) {
    const requestedAgent = await deps.config.resolveAgent(requestedAgentSlug)
    if (!requestedAgent || requestedAgent.id !== origin.agentId) {
      return c.json(fail(req.id, A2A_ERROR_CODES.TASK_ACCESS_DENIED, 'task belongs to a different agent'), 403)
    }
  } else if (!deps.config.x402.demoMode) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_ACCESS_DENIED, 'task origin is not recorded'), 403)
  }
  const authorize = deps.config.a2a?.authorizeTaskAccess
  if (!authorize && deps.config.x402.demoMode) return undefined
  if (!authorize) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_ACCESS_DENIED, 'task access authorization is not configured'),
      403,
    )
  }
  let allowed = false
  try {
    allowed = await authorize(task, {
      method: req.method,
      agentSlug: requestedAgentSlug,
      authorization: c.req.header('Authorization') ?? '',
      paymentSignature: c.req.header('X-Payment-Signature') ?? '',
    })
  } catch (error) {
    console.error(
      `[a2a] task access authorization failed for ${task.id}:`,
      error instanceof Error ? error.message : String(error),
    )
  }
  if (allowed) return undefined
  return c.json(fail(req.id, A2A_ERROR_CODES.TASK_ACCESS_DENIED, 'task access denied'), 403)
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

async function createTask(taskStore: TaskStore, task: Task): Promise<boolean> {
  if (!taskStore.createIfAbsent) {
    throw new Error('A2A task store does not provide createIfAbsent')
  }
  return taskStore.createIfAbsent(task)
}
function normalizeTaskStore(taskStore: TaskStore, allowUnsafeFallback: boolean): TaskStore {
  const hasCreateIfAbsent = typeof taskStore.createIfAbsent === 'function'
  const hasCompareAndSet = typeof taskStore.compareAndSet === 'function'
  const hasCompareAndSetExecution = typeof taskStore.compareAndSetExecution === 'function'
  if (hasCreateIfAbsent && hasCompareAndSet && hasCompareAndSetExecution) {
    return taskStore
  }
  if (!allowUnsafeFallback) {
    throw new Error(
      'A2A production task store must implement createIfAbsent, compareAndSet, and compareAndSetExecution',
    )
  }
  return {
    get: (id) => taskStore.get(id),
    put: (task) => taskStore.put(task),
    delete: (id) => taskStore.delete(id),
    async createIfAbsent(task) {
      if (hasCreateIfAbsent) return taskStore.createIfAbsent!(task)
      if (await taskStore.get(task.id)) return false
      await taskStore.put(task)
      return true
    },
    async compareAndSet(expected, next) {
      if (hasCompareAndSet) return taskStore.compareAndSet!(expected, next)
      const current = await taskStore.get(expected.id)
      if (!current || JSON.stringify(current) !== JSON.stringify(expected)) return false
      await taskStore.put(next)
      return true
    },
    async compareAndSetExecution(expected, next, requestId, now) {
      if (hasCompareAndSetExecution) {
        return taskStore.compareAndSetExecution!(expected, next, requestId, now)
      }
      const current = await taskStore.get(expected.id)
      const expectedMarker = inspectTaskExecution(current ?? expected)
      const nextMarker = inspectTaskExecution(next)
      if (
        !current ||
        JSON.stringify(current) !== JSON.stringify(expected) ||
        expectedMarker.state !== 'valid' ||
        nextMarker.state !== 'valid' ||
        expectedMarker.marker.requestId !== requestId ||
        nextMarker.marker.requestId !== requestId ||
        expectedMarker.marker.lease.expiresAt <= now
      ) return false
      await taskStore.put(next)
      return true
    },
  }
}

async function recoverTaskIfNeeded(
  task: Task,
  deps: A2AHandlerDeps,
  requestedAgentSlug: string,
): Promise<Task> {
  const lifecycle = createTaskLifecycle(deps)
  const paymentReleased = await recoverPaymentReleaseIfNeeded(task, lifecycle.payment)
  const finalized = await recoverFinalizationIfNeeded(
    paymentReleased,
    lifecycle.finalization,
    requestedAgentSlug,
  )
  const paymentRecovered = await recoverPaymentMarkerIfNeeded(finalized, lifecycle.payment)
  const executionRecovered = await recoverExpiredExecutionIfNeeded(
    paymentRecovered,
    deps,
    requestedAgentSlug,
  )
  if (hasActiveTaskExecution(executionRecovered)) return executionRecovered
  return recoverSubmissionIfNeeded(executionRecovered, {
    taskStore: deps.taskStore,
    deliverPush: lifecycle.finalization.deliverPush,
  })
}

async function getTaskExecutionSource(
  task: Task,
  deps: A2AHandlerDeps,
  requestedAgentSlug: string,
): Promise<TaskExecutionSource | undefined> {
  const reference = readTaskExecutionReference(task)
  if (!reference) return undefined
  const origin = readTaskOrigin(task)
  const agent = await deps.config.resolveAgent(origin?.agentSlug ?? requestedAgentSlug)
  if (!agent || !agent.enabled) throw new Error('A2A task execution agent is unavailable')
  if (origin && origin.agentId !== agent.id) {
    throw new Error('A2A task execution agent does not match task origin')
  }
  const box = await deps.config.getSandbox(agent)
  const run = reference
  if (box.id !== run.environmentId || typeof box.session !== 'function') {
    throw new Error('A2A task execution sandbox reference is unavailable')
  }
  const session = box.session(run.sessionId)
  if (
    !session ||
    typeof session.events !== 'function' ||
    typeof session.result !== 'function' ||
    typeof session.interrupt !== 'function'
  ) throw new Error('A2A task execution session controls are unavailable')
  return {
    reference,
    events: (options) => session.events({
      ...(options ?? {}),
      executionId: run.executionId,
    }),
    result: () => session.result({ executionId: run.executionId }),
    interrupt: () => session.interrupt({ executionId: run.executionId }),
  }
}

async function reconcileTaskExecution(
  task: Task,
  deps: A2AHandlerDeps,
  requestedAgentSlug: string,
  allowActive = false,
): Promise<Task> {
  if (
    isTerminal(task.status.state) ||
    task.status.state === 'input-required' ||
    (hasActiveTaskExecution(task) && !allowActive)
  ) return task
  const source = await getTaskExecutionSource(task, deps, requestedAgentSlug)
  if (!source) return task
  const result = await source.result()
  if (result.executionId !== undefined && result.executionId !== source.reference.executionId) {
    throw new Error('A2A task execution result does not match its stored execution')
  }
  const current = await deps.taskStore.get(task.id) ?? task
  if (isTerminal(current.status.state) || current.status.state === 'input-required') return current
  if (readTaskExecutionReference(current)?.executionId !== source.reference.executionId) return current

  const state = taskStateFromSandboxResult(result)
  if (state === 'working') return current
  if (state === 'failed' || state === 'canceled') {
    const failed = withStatus(
      current,
      state,
      result.error ? agentMessage(current, result.error) : undefined,
    )
    if (await compareAndSetTask(deps.taskStore, current, failed)) {
      await maybeDeliverPush(failed, deps)
      return failed
    }
    return await deps.taskStore.get(task.id) ?? failed
  }

  const usage = recoveredUsage(result)
  const paymentRecoveryId = readPaymentRecoveryMarker(current)?.id
  if (paymentRecoveryId) {
    if (!deps.config.paymentRecovery) {
      throw new Error('A2A durable payment recovery is unavailable')
    }
    const lifecycle = createTaskLifecycle(deps)
    const recovered = await lifecycle.payment.recoverDurablePayment(paymentRecoveryId, {
      force: true,
      usage,
    })
    if (recovered?.state !== 'reconciled') {
      throw new Error('A2A durable payment finalization is still pending')
    }
  } else {
    const inspection = inspectTaskExecution(current)
    if (inspection.state !== 'valid') return current
    const agent = await deps.config.resolveAgent(readTaskOrigin(current)?.agentSlug ?? requestedAgentSlug)
    if (!agent || !agent.enabled) throw new Error('A2A task execution agent is unavailable')
    const authz = recoveredExecutionAuthz(current, inspection.marker.requestId, agent, deps.config)
    const lifecycle = createTaskLifecycle(deps)
    await lifecycle.finalization.settle(authz, usage)
  }
  const text = result.response ?? ''
  const artifact = text
    ? responseTextToArtifact(text, `${current.id}-artifact-0`)
    : current.artifacts?.[0]
  const next = withStatus(
    clearPaymentRecoveryMarker(current),
    state,
    state === 'input-required' && result.error ? agentMessage(current, result.error) : undefined,
    artifact ? [artifact] : current.artifacts,
  )
  if (await compareAndSetTask(deps.taskStore, current, next)) {
    await maybeDeliverPush(next, deps)
    return next
  }
  return await deps.taskStore.get(task.id) ?? next
}

function taskStateFromSandboxResult(result: SandboxPromptResult): Task['status']['state'] {
  const status = result.status.toLowerCase()
  if (result.success || status === 'success' || status === 'completed') return 'completed'
  if (status === 'awaiting_question' || status === 'input-required' || status === 'input_required') {
    return 'input-required'
  }
  if (status === 'running' || status === 'queued' || status === 'working') return 'working'
  if (status === 'canceled' || status === 'cancelled') return 'canceled'
  return 'failed'
}

function recoveredExecutionAuthz(
  task: Task,
  requestId: string,
  agent: AgentMeta,
  config: GatewayConfig,
): AuthorizedRequest {
  const submission = readTaskSubmission(task)
  if (!submission || submission.requestId !== requestId || submission.agentId !== agent.id) {
    throw new Error(`A2A task '${task.id}' execution context is unavailable`)
  }
  const latestUserMessage = [...(task.history ?? [])]
    .reverse()
    .find((message) => message.role === 'user')
  const extracted = latestUserMessage ? extractTextFromMessage(latestUserMessage) : null
  const userMessage = extracted && !('error' in extracted) ? extracted.text : '[recovered A2A task]'
  const maxOutputTokens = config.maxOutputTokens ?? config.defaultOutputTokens ?? 1024
  const maxReasoningTokens = config.executionBudget?.maxReasoningTokens ?? maxOutputTokens
  const maxToolTokens = config.executionBudget?.maxToolTokens ?? maxOutputTokens
  const maxToolCalls = config.executionBudget?.maxToolCalls ?? 8
  const executionBudget = {
    maxInputTokens: estimateTokens(userMessage),
    maxOutputTokens,
    maxReasoningTokens,
    maxToolTokens,
    maxToolCalls,
    maxProviderCostUsd: config.executionBudget?.maxProviderCostUsd ?? (
      (estimateTokens(userMessage) + maxOutputTokens + maxReasoningTokens + maxToolTokens) *
      agent.pricePerTokenUsd
    ),
  }
  const parsedStartMs = Date.parse(task.status.timestamp)
  return {
    agent,
    consumerId: submission.consumerId,
    paymentMethod: 'apikey',
    keyInfo: null,
    userMessage,
    rateLimitRemaining: undefined,
    requestId,
    startMs: Number.isFinite(parsedStartMs) ? parsedStartMs : Date.now(),
    maxOutputTokens,
    executionBudget,
    requiredPaymentAmount: 0n,
    paymentPayload: null,
  }
}

function recoveredUsage(
  result: SandboxPromptResult,
): SandboxUsageReceipt {
  const usage = result.usage
  const tokenFields = ['inputTokens', 'outputTokens', 'reasoningTokens', 'toolTokens', 'toolCallCount'] as const
  if (
    !usage ||
    tokenFields.some((field) => !Number.isSafeInteger(usage[field]) || usage[field]! < 0) ||
    typeof usage.providerCostUsd !== 'number' ||
    !Number.isFinite(usage.providerCostUsd) ||
    usage.providerCostUsd < 0 ||
    typeof usage.budgetEnforced !== 'boolean'
  ) throw new Error('sandbox detached result did not provide a complete usage receipt')
  return usage as SandboxUsageReceipt
}

async function recoverExpiredExecutionIfNeeded(
  task: Task,
  deps: A2AHandlerDeps,
  requestedAgentSlug: string,
): Promise<Task> {
  const malformed = hasMalformedTaskExecution(task)
  if (
    task.status.state !== 'working' ||
    (isTaskFinalizing(task) && !malformed) ||
    (!malformed && !hasExpiredTaskExecution(task))
  ) return task
  if (!malformed && readTaskExecutionReference(task)) {
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
      [EXECUTION_RECOVERY_METADATA_KEY]: {
        error: inspection.state === 'malformed'
          ? `A2A execution marker was malformed: ${inspection.reason}`
          : 'A2A execution lease expired before a task result was stored',
      },
    },
  }
  if (await compareAndSetTask(deps.taskStore, task, failed)) {
    await maybeDeliverPush(failed, deps)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? task
}

async function readJsonBody(request: Request): Promise<unknown> {
  if (!request.body) return await request.json()
  const reader = request.body.getReader()
  const chunks: Uint8Array[] = []
  let total = 0
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      if (value) {
        total += value.byteLength
        if (total > MAX_A2A_BODY_BYTES) {
          await reader.cancel().catch(() => undefined)
          throw new RequestBodyTooLargeError('request body too large')
        }
        chunks.push(value)
      }
    }
  } finally {
    reader.releaseLock()
  }
  const body = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    body.set(chunk, offset)
    offset += chunk.byteLength
  }
  return JSON.parse(new TextDecoder().decode(body)) as unknown
}

/**
 * Fire-and-forget push delivery. Idempotent w.r.t. push: if the task hasn't
 * reached a terminal state, this is a no-op. The dispatch logs failures via
 * the observer rather than failing the request — webhook receivers re-fetch
 * via `tasks/get` to confirm state.
 */
async function maybeDeliverPush(task: Task, deps: A2AHandlerDeps): Promise<void> {
  const pushDeps: PushDeliveryDependencies = {
    taskStore: deps.taskStore,
    pushStore: deps.pushStore,
    demoMode: deps.config.x402.demoMode === true,
    webhookSecret: deps.config.a2a?.webhookSecret,
    fetcher: deps.config.a2a?.pushFetcher,
    urlValidator: deps.config.a2a?.pushUrlValidator,
    onDeliveryFailure: (failedTask, result) => {
      void deps.state.obs?.onStreamError?.(
        { requestId: result.taskId, agentSlug: failedTask.id, startMs: Date.now() },
        {
          consumerId: result.configId,
          errorMessage: `push delivery failed (${result.status ?? 'no-status'}): ${result.error ?? 'non-2xx'}`,
        },
      )
    },
  }
  await deliverTaskPush(task, pushDeps)
}
