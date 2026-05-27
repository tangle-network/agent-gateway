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
  type A2ADispatchEvent,
  type AuthorizedRequest,
  type GatewayState,
  authenticateAndGuard,
  dispatchSandboxStreamRich,
  estimateTokens,
  settleAndRecord,
} from '../dispatch'
import type { GatewayConfig } from '../types'
import { buildAgentCard } from './agent-card'
import { fail, ok, parseEnvelope } from './jsonrpc'
import {
  deliverPushNotifications,
  type PushNotificationStore,
  type TaskPushNotificationConfig,
} from './push-notifications'
import type { TaskStore } from './task-store'
import { extractTextFromMessage, responseTextToArtifact } from './translate'
import {
  A2A_ERROR_CODES,
  type JSONRPCRequest,
  type Message,
  type MessageSendParams,
  type StreamingEvent,
  type Task,
  type TaskArtifactUpdateEvent,
  type TaskIdParams,
  type TaskPushNotificationConfigGetParams,
  type TaskStatusUpdateEvent,
} from './types'

export interface A2AHandlerDeps {
  config: GatewayConfig
  state: GatewayState
  taskStore: TaskStore
  pushStore?: PushNotificationStore
}

/** Terminal task states — fire-once push delivery occurs on these transitions. */
const TERMINAL_STATES: ReadonlySet<Task['status']['state']> = new Set([
  'completed',
  'canceled',
  'failed',
  'rejected',
])

/**
 * Per-gateway in-process registry of cancellable runs. Keyed by task id;
 * absent = task already terminal or never streamed. Cleared by the streaming
 * handler on completion. Cancel is best-effort: a cancel arriving after the
 * stream finished is reported as `TASK_NOT_CANCELABLE`.
 */
class CancelRegistry {
  private readonly controllers = new Map<string, AbortController>()

  register(taskId: string): AbortController {
    const c = new AbortController()
    this.controllers.set(taskId, c)
    return c
  }

  clear(taskId: string): void {
    this.controllers.delete(taskId)
  }

  cancel(taskId: string): boolean {
    const c = this.controllers.get(taskId)
    if (!c) return false
    c.abort()
    this.controllers.delete(taskId)
    return true
  }
}

export function createA2AHandlers(deps: A2AHandlerDeps) {
  const cancels = new CancelRegistry()

  // GET /:slug/.well-known/agent.json
  const handleAgentCard = async (c: Context): Promise<Response> => {
    const slug = c.req.param('slug')
    if (!slug) return c.json({ error: 'slug required' }, 400)
    const agent = await deps.config.resolveAgent(slug)
    if (!agent) {
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
    if (contentLength > 65536) {
      return c.json(fail(null, A2A_ERROR_CODES.INVALID_REQUEST, 'request body too large (max 64KB)'), 413)
    }

    let raw: unknown
    try {
      raw = await c.req.json()
    } catch {
      return c.json(fail(null, A2A_ERROR_CODES.PARSE_ERROR, 'invalid JSON'), 400)
    }
    const parsed = parseEnvelope(raw)
    if ('code' in parsed) {
      return c.json(fail(parsed.id, parsed.code, parsed.message), 400)
    }

    switch (parsed.method) {
      case 'message/send':
        return handleMessageSend(c, slug, parsed, deps)
      case 'message/stream':
        return handleMessageStream(c, slug, parsed, deps, cancels)
      case 'tasks/get':
        return handleTasksGet(c, parsed, deps)
      case 'tasks/cancel':
        return handleTasksCancel(c, parsed, deps, cancels)
      case 'tasks/resubscribe':
        return handleTasksResubscribe(c, parsed, deps)
      case 'tasks/pushNotificationConfig/set':
        return handlePushSet(c, parsed, deps)
      case 'tasks/pushNotificationConfig/get':
        return handlePushGet(c, parsed, deps)
      case 'tasks/pushNotificationConfig/list':
        return handlePushList(c, parsed, deps)
      case 'tasks/pushNotificationConfig/delete':
        return handlePushDelete(c, parsed, deps)
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
): Promise<Response> {
  const guard = await guardMessageRequest(c, slug, req, deps)
  if (guard instanceof Response) return guard
  const { authz, task } = guard

  let responseText = ''
  let outputTokens = 0
  let inputRequiredPrompt: string | undefined
  let inputRequiredSeen = false
  try {
    for await (const event of dispatchSandboxStreamRich(
      authz.agent,
      authz.userMessage,
      authz.consumerId,
      deps.config,
      undefined,
      task.id,
    )) {
      if (event.kind === 'text') {
        responseText += event.delta
        outputTokens += estimateTokens(event.delta)
      } else {
        inputRequiredSeen = true
        inputRequiredPrompt = event.prompt
      }
    }
  } catch (err) {
    const failed = withStatus(task, 'failed')
    await deps.taskStore.put(failed)
    await maybeDeliverPush(failed, deps)
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.INTERNAL_ERROR,
        err instanceof Error ? err.message : String(err),
      ),
    )
  }

  // Settle for the work done so far before short-circuiting on input-required.
  // The user has been charged for the partial response, which is the right
  // commercial behavior — the sandbox produced tokens.
  await settleAndRecord(
    authz.agent,
    authz,
    estimateTokens(authz.userMessage),
    outputTokens,
    deps.config,
    deps.state.obs,
  )

  if (inputRequiredSeen) {
    const paused = withStatus(
      task,
      'input-required',
      inputRequiredPrompt ? agentMessage(task, inputRequiredPrompt) : undefined,
      responseText
        ? [responseTextToArtifact(responseText, `${task.id}-artifact-0`)]
        : task.artifacts,
    )
    await deps.taskStore.put(paused)
    // input-required is non-terminal — do NOT deliver push notifications.
    return c.json(ok(req.id, paused))
  }

  const completed = withStatus(task, 'completed', undefined, [
    responseTextToArtifact(responseText, `${task.id}-artifact-0`),
  ])
  await deps.taskStore.put(completed)
  await maybeDeliverPush(completed, deps)
  return c.json(ok(req.id, completed))
}

// ── message/stream (SSE) ──────────────────────────────────────────────────

async function handleMessageStream(
  c: Context,
  slug: string,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
  cancels: CancelRegistry,
): Promise<Response> {
  const guard = await guardMessageRequest(c, slug, req, deps)
  if (guard instanceof Response) return guard
  const { authz, task } = guard

  const controller = cancels.register(task.id)
  const inputTokens = estimateTokens(authz.userMessage)
  let outputTokens = 0
  let responseText = ''

  const stream = new ReadableStream({
    async start(ctrl) {
      const encoder = new TextEncoder()
      const send = (event: StreamingEvent) => {
        ctrl.enqueue(encoder.encode(`data: ${JSON.stringify(ok(req.id, event))}\n\n`))
      }

      // Status: working
      const workingStatus: TaskStatusUpdateEvent = {
        kind: 'status-update',
        taskId: task.id,
        contextId: task.contextId,
        status: { state: 'working', timestamp: nowIso() },
        final: false,
      }
      await deps.taskStore.put({ ...task, status: workingStatus.status })
      send(workingStatus)

      let inputRequiredPrompt: string | undefined
      let inputRequiredSeen = false
      try {
        for await (const event of dispatchSandboxStreamRich(
          authz.agent,
          authz.userMessage,
          authz.consumerId,
          deps.config,
          controller.signal,
          task.id,
        )) {
          if (event.kind === 'text') {
            responseText += event.delta
            outputTokens += estimateTokens(event.delta)
            const artifactEvent: TaskArtifactUpdateEvent = {
              kind: 'artifact-update',
              taskId: task.id,
              contextId: task.contextId,
              artifact: {
                artifactId: `${task.id}-artifact-0`,
                name: 'response',
                parts: [{ kind: 'text', text: event.delta }],
              },
              append: true,
            }
            send(artifactEvent)
          } else {
            inputRequiredSeen = true
            inputRequiredPrompt = event.prompt
          }
        }

        // Caller aborted via tasks/cancel — emit canceled, do not settle.
        if (controller.signal.aborted) {
          const canceled = withStatus(task, 'canceled', undefined, [
            responseTextToArtifact(responseText, `${task.id}-artifact-0`),
          ])
          await deps.taskStore.put(canceled)
          send({
            kind: 'status-update',
            taskId: task.id,
            contextId: task.contextId,
            status: canceled.status,
            final: true,
          })
          await maybeDeliverPush(canceled, deps)
          return
        }

        // Settle once for whatever the sandbox produced (full or partial).
        await settleAndRecord(
          authz.agent,
          authz,
          inputTokens,
          outputTokens,
          deps.config,
          deps.state.obs,
        )

        if (inputRequiredSeen) {
          const paused = withStatus(
            task,
            'input-required',
            inputRequiredPrompt ? agentMessage(task, inputRequiredPrompt) : undefined,
            responseText
              ? [responseTextToArtifact(responseText, `${task.id}-artifact-0`)]
              : task.artifacts,
          )
          await deps.taskStore.put(paused)
          send({
            kind: 'status-update',
            taskId: task.id,
            contextId: task.contextId,
            status: paused.status,
            final: true,
          })
          // input-required is non-terminal — do NOT deliver push notifications.
          return
        }

        // Final: artifact lastChunk + completed status.
        send({
          kind: 'artifact-update',
          taskId: task.id,
          contextId: task.contextId,
          artifact: {
            artifactId: `${task.id}-artifact-0`,
            name: 'response',
            parts: [{ kind: 'text', text: '' }],
          },
          append: true,
          lastChunk: true,
        })
        const completed = withStatus(task, 'completed', undefined, [
          responseTextToArtifact(responseText, `${task.id}-artifact-0`),
        ])
        await deps.taskStore.put(completed)
        send({
          kind: 'status-update',
          taskId: task.id,
          contextId: task.contextId,
          status: completed.status,
          final: true,
        })
        await maybeDeliverPush(completed, deps)
      } catch (err) {
        const failed = withStatus(task, 'failed')
        await deps.taskStore.put(failed)
        send({
          kind: 'status-update',
          taskId: task.id,
          contextId: task.contextId,
          status: failed.status,
          final: true,
        })
        await maybeDeliverPush(failed, deps)
        await deps.state.obs?.onStreamError?.(
          {
            requestId: authz.requestId,
            agentSlug: authz.agent.slug,
            startMs: authz.startMs,
          },
          {
            consumerId: authz.consumerId,
            errorMessage: err instanceof Error ? err.message : String(err),
          },
        )
      } finally {
        cancels.clear(task.id)
        ctrl.close()
      }
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Request-Id': authz.requestId,
      'X-Agent-Slug': authz.agent.slug,
      'X-Task-Id': task.id,
    },
  })
}

// ── tasks/get + tasks/cancel ──────────────────────────────────────────────

async function handleTasksGet(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<Response> {
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`),
    )
  }
  return c.json(ok(req.id, task))
}

async function handleTasksCancel(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
  cancels: CancelRegistry,
): Promise<Response> {
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`),
    )
  }
  if (isTerminal(task.status.state)) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${params.id}' is in terminal state '${task.status.state}'`,
      ),
    )
  }

  const stillActive = cancels.cancel(task.id)
  const canceled: Task = {
    ...task,
    status: { state: 'canceled', timestamp: nowIso() },
  }
  await deps.taskStore.put(canceled)

  // If a stream was active, it'll observe the abort and emit its own final
  // status-update AND fire its own push delivery; the dispatcher only fires
  // push when the cancel races to terminal state with no active streamer.
  if (!stillActive) {
    await maybeDeliverPush(canceled, deps)
  }
  return c.json(ok(req.id, canceled))
}

// ── tasks/resubscribe ─────────────────────────────────────────────────────

/**
 * Re-attach to a known task via SSE. The minimum-viable shape (and the one
 * the spec actually requires): emit the task's current status as one
 * status-update event with the right `final` flag, then close. Callers that
 * lost their original stream connection can re-subscribe to find out where
 * the task ended up; in-flight tasks return their last-known state and the
 * client polls (or re-subscribes) for further updates.
 *
 * Out of scope: live-rebroadcasting deltas from an in-flight stream to a new
 * subscriber. That requires per-task pub/sub which we haven't needed yet —
 * the typical recovery path is "task already finished, fetch the result."
 */
async function handleTasksResubscribe(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<Response> {
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`),
    )
  }
  const final = isTerminal(task.status.state) || task.status.state === 'input-required'
  const event: TaskStatusUpdateEvent = {
    kind: 'status-update',
    taskId: task.id,
    contextId: task.contextId,
    status: task.status,
    final,
  }
  const encoder = new TextEncoder()
  const stream = new ReadableStream({
    start(ctrl) {
      ctrl.enqueue(encoder.encode(`data: ${JSON.stringify(ok(req.id, event))}\n\n`))
      ctrl.close()
    },
  })
  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Task-Id': task.id,
    },
  })
}

// ── tasks/pushNotificationConfig/* ────────────────────────────────────────

async function handlePushSet(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskPushNotificationConfig | undefined
  if (!params || typeof params.taskId !== 'string' || !params.pushNotificationConfig?.id) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.INVALID_PARAMS,
        'params.taskId and params.pushNotificationConfig.id required',
      ),
    )
  }
  if (typeof params.pushNotificationConfig.url !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url required'))
  }
  const task = await deps.taskStore.get(params.taskId)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.taskId}' not found`))
  }
  await deps.pushStore.set(params.taskId, params.pushNotificationConfig)
  const stored = await deps.pushStore.get(params.taskId, params.pushNotificationConfig.id)
  return c.json(ok(req.id, { taskId: params.taskId, pushNotificationConfig: stored }))
}

async function handlePushGet(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskPushNotificationConfigGetParams | undefined
  if (!params || typeof params.id !== 'string' || typeof params.pushNotificationConfigId !== 'string') {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id and params.pushNotificationConfigId required'),
    )
  }
  const cfg = await deps.pushStore.get(params.id, params.pushNotificationConfigId)
  if (!cfg) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_FOUND,
        `push config '${params.pushNotificationConfigId}' not found for task '${params.id}'`,
      ),
    )
  }
  return c.json(ok(req.id, { taskId: params.id, pushNotificationConfig: cfg }))
}

async function handlePushList(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const configs = await deps.pushStore.list(params.id)
  return c.json(ok(req.id, configs.map((cfg) => ({ taskId: params.id, pushNotificationConfig: cfg }))))
}

async function handlePushDelete(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskPushNotificationConfigGetParams | undefined
  if (!params || typeof params.id !== 'string' || typeof params.pushNotificationConfigId !== 'string') {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id and params.pushNotificationConfigId required'),
    )
  }
  await deps.pushStore.delete(params.id, params.pushNotificationConfigId)
  return c.json(ok(req.id, null))
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

  const guard = await authenticateAndGuard(
    c,
    slug,
    [{ role: 'user', content: extracted.text }],
    deps.config,
    deps.state,
  )
  if (guard instanceof Response) return guard
  const authz = guard

  // Multi-turn continuation: if the caller addressed an existing task that is
  // currently in `input-required`, append the new message and transition to
  // `working`. Any other taskId (unknown OR pointing at a terminal/working
  // task) means the caller is starting a fresh task and we mint a new id.
  if (typeof params.message.taskId === 'string') {
    const existing = await deps.taskStore.get(params.message.taskId)
    if (existing) {
      if (existing.status.state !== 'input-required') {
        return c.json(
          fail(
            req.id,
            A2A_ERROR_CODES.INVALID_PARAMS,
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
        status: { state: 'working', timestamp: nowIso() },
        history: [...(existing.history ?? []), appendedMessage],
      }
      await deps.taskStore.put(continued)
      return { authz, task: continued }
    }
    // Unknown taskId in params: fall through and mint a fresh task with that
    // exact id so callers that pre-allocate ids (idempotency) get them.
  }

  const taskId = params.message.taskId ?? `task_${cryptoRandomId()}`
  const contextId = params.message.contextId ?? `ctx_${cryptoRandomId()}`
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
  }
  await deps.taskStore.put(task)
  return { authz, task }
}

// ── Helpers ───────────────────────────────────────────────────────────────

function isTerminal(state: Task['status']['state']): boolean {
  return (
    state === 'completed' ||
    state === 'canceled' ||
    state === 'failed' ||
    state === 'rejected'
  )
}

function nowIso(): string {
  return new Date().toISOString()
}

function cryptoRandomId(): string {
  return crypto.randomUUID().replace(/-/g, '')
}

/**
 * Build a new Task with an updated status (and optional artifacts). Centralises
 * the timestamp + status structure so terminal transitions are written
 * identically across every code path.
 */
function withStatus(
  task: Task,
  state: Task['status']['state'],
  message?: Message,
  artifacts?: Task['artifacts'],
): Task {
  return {
    ...task,
    status: { state, timestamp: nowIso(), ...(message ? { message } : {}) },
    ...(artifacts !== undefined ? { artifacts } : {}),
  }
}

/**
 * Synthesize an agent-role message attached to a status (e.g. the
 * input-required prompt text). messageId is deterministic-by-task so callers
 * can dedupe on retry.
 */
function agentMessage(task: Task, text: string): Message {
  return {
    kind: 'message',
    role: 'agent',
    parts: [{ kind: 'text', text }],
    messageId: `${task.id}-status-${task.status.state}-${nowIso()}`,
    taskId: task.id,
    contextId: task.contextId,
  }
}

/**
 * Fire-and-forget push delivery. Idempotent w.r.t. push: if the task hasn't
 * reached a terminal state, this is a no-op. The dispatch logs failures via
 * the observer rather than failing the request — webhook receivers re-fetch
 * via `tasks/get` to confirm state.
 */
async function maybeDeliverPush(task: Task, deps: A2AHandlerDeps): Promise<void> {
  if (!deps.pushStore || !TERMINAL_STATES.has(task.status.state)) return
  try {
    await deliverPushNotifications({
      task,
      store: deps.pushStore,
      webhookSecret: deps.config.a2a?.webhookSecret,
      fetcher: deps.config.a2a?.pushFetcher,
      onDelivery: (result) => {
        if (!result.ok) {
          deps.state.obs?.onStreamError?.(
            { requestId: result.taskId, agentSlug: task.id, startMs: Date.now() },
            {
              consumerId: result.configId,
              errorMessage: `push delivery failed (${result.status ?? 'no-status'}): ${result.error ?? 'non-2xx'}`,
            },
          )
        }
      },
    })
  } catch (err) {
    // Catastrophic failure of the push pipeline itself (e.g. the store threw).
    // Logged but never escalated — a busted webhook MUST NOT fail the agent.
    console.error(
      `[agent-gateway] push delivery threw for task ${task.id}: ${err instanceof Error ? err.message : String(err)}`,
    )
  }
}

