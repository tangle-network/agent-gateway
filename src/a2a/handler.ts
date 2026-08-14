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
  claimPayment,
  dispatchSandboxStreamRich,
  releasePayment,
  releasePaymentAfterFailure,
  settleAndRecord,
} from '../dispatch'
import type { GatewayConfig } from '../types'
import type { SandboxUsageReceipt } from '../types'
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
  private readonly finalizing = new Set<string>()

  register(taskId: string): AbortController {
    const c = new AbortController()
    this.controllers.set(taskId, c)
    return c
  }

  clear(taskId: string): void {
    this.controllers.delete(taskId)
    this.finalizing.delete(taskId)
  }

  beginFinalization(taskId: string): boolean {
    const controller = this.controllers.get(taskId)
    if (!controller || controller.signal.aborted || this.finalizing.has(taskId)) return false
    this.finalizing.add(taskId)
    return true
  }

  isFinalizing(taskId: string): boolean {
    return this.finalizing.has(taskId)
  }

  cancel(taskId: string): boolean {
    if (this.finalizing.has(taskId)) return false
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
  const workingTask: Task = task.status.state === 'working'
    ? task
    : { ...task, status: { state: 'working', timestamp: nowIso() } }
  if (task.status.state !== 'working' && !await compareAndSetTask(deps.taskStore, task, workingTask)) {
    await releaseOwnedPayment(authz, deps, 'A2A task changed before execution started')
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' changed before execution`))
  }

  let responseText = ''
  let usage: SandboxUsageReceipt | undefined
  let workObserved = false
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
      authz.maxOutputTokens,
    )) {
      if (event.kind === 'text') {
        responseText += event.delta
        workObserved = true
      } else if (event.kind === 'activity') {
        workObserved = true
      } else if (event.kind === 'usage') {
        usage = event.usage
      } else {
        inputRequiredSeen = true
        inputRequiredPrompt = event.prompt
        workObserved = true
      }
    }
  } catch (err) {
    await releaseOrRetainPayment(
      authz,
      deps,
      err instanceof Error ? err.message : String(err),
      workObserved || usage !== undefined,
    )
    const currentTask = await deps.taskStore.get(task.id)
    const failed = currentTask && isTerminal(currentTask.status.state)
      ? currentTask
      : withStatus(workingTask, 'failed')
    try {
      await deps.taskStore.put(failed)
      await maybeDeliverPush(failed, deps)
    } catch (taskError) {
      console.error(
        `[a2a] failed to persist failed task ${task.id}:`,
        taskError instanceof Error ? taskError.message : String(taskError),
      )
    }
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
  try {
    if (!usage) throw new Error('sandbox did not provide a usage receipt')
    if (!await claimTaskFinalization(deps.taskStore, workingTask)) {
      throw new Error('A2A task changed before payment settlement')
    }
    await settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs)
  } catch (err) {
    await releaseOrRetainPayment(
      authz,
      deps,
      err instanceof Error ? err.message : String(err),
      workObserved || usage !== undefined,
    )
    const currentTask = await deps.taskStore.get(task.id)
    const failed = currentTask && isTerminal(currentTask.status.state)
      ? currentTask
      : withStatus(workingTask, 'failed')
    try {
      await deps.taskStore.put(failed)
      await maybeDeliverPush(failed, deps)
    } catch (taskError) {
      console.error(
        `[a2a] failed to persist failed task ${task.id}:`,
        taskError instanceof Error ? taskError.message : String(taskError),
      )
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'Payment settlement failed'))
  }

  if (inputRequiredSeen) {
    const paused = withStatus(
      workingTask,
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

  const completed = withStatus(workingTask, 'completed', undefined, [
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
  let responseText = ''
  let usage: SandboxUsageReceipt | undefined
  let workObserved = false

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
      const workingTask: Task = task.status.state === 'working'
        ? task
        : { ...task, status: workingStatus.status }
      let inputRequiredPrompt: string | undefined
      let inputRequiredSeen = false
      try {
        if (task.status.state !== 'working' && !await compareAndSetTask(deps.taskStore, task, workingTask)) {
          throw new Error('A2A task changed before execution started')
        }
        send(workingStatus)

        for await (const event of dispatchSandboxStreamRich(
          authz.agent,
          authz.userMessage,
          authz.consumerId,
          deps.config,
          controller.signal,
          task.id,
          authz.maxOutputTokens,
        )) {
          if (event.kind === 'text') {
            responseText += event.delta
            workObserved = true
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
          } else if (event.kind === 'activity') {
            workObserved = true
          } else if (event.kind === 'usage') {
            usage = event.usage
          } else {
            inputRequiredSeen = true
            inputRequiredPrompt = event.prompt
            workObserved = true
          }
        }

        // Caller aborted via tasks/cancel. Charge a complete receipt if one
        // exists; otherwise retain ownership when output or hidden work was
        // observed because releasing would make paid work free.
        if (controller.signal.aborted) {
          if (usage) {
            try {
              await settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs)
            } catch (settlementError) {
              console.error(
                `[a2a] canceled task settlement retained for ${authz.requestId}:`,
                settlementError instanceof Error ? settlementError.message : String(settlementError),
              )
            }
          } else {
            await releaseOrRetainPayment(authz, deps, 'a2a task canceled', workObserved || usage !== undefined)
          }
          const canceled = withStatus(task, 'canceled', undefined, [
            responseTextToArtifact(responseText, `${task.id}-artifact-0`),
          ])
          try {
            await deps.taskStore.put(canceled)
            send({
              kind: 'status-update',
              taskId: task.id,
              contextId: task.contextId,
              status: canceled.status,
              final: true,
            })
            await maybeDeliverPush(canceled, deps)
          } catch (taskError) {
            console.error(
              `[a2a] failed to persist canceled task ${task.id}:`,
              taskError instanceof Error ? taskError.message : String(taskError),
            )
          }
          return
        }

        // Settle once for whatever the sandbox produced (full or partial).
        if (!usage) throw new Error('sandbox did not provide a usage receipt')
        if (!cancels.beginFinalization(task.id) || !await claimTaskFinalization(deps.taskStore, workingTask)) {
          await releaseOrRetainPayment(
            authz,
            deps,
            'A2A task changed before payment settlement',
            workObserved || usage !== undefined,
          )
          return
        }
        await settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs)

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
        await releaseOrRetainPayment(
          authz,
          deps,
          err instanceof Error ? err.message : String(err),
          workObserved || usage !== undefined,
        )
        const currentTask = await deps.taskStore.get(task.id)
        const failed = currentTask && isTerminal(currentTask.status.state)
          ? currentTask
          : withStatus(task, 'failed')
        try {
          await deps.taskStore.put(failed)
          send({
            kind: 'status-update',
            taskId: task.id,
            contextId: task.contextId,
            status: failed.status,
            final: true,
          })
          await maybeDeliverPush(failed, deps)
        } catch (taskError) {
          console.error(
            `[a2a] failed to persist failed task ${task.id}:`,
            taskError instanceof Error ? taskError.message : String(taskError),
          )
        }
        try {
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
        } catch (observerError) {
          console.error(
            `[a2a] stream observer failed for ${authz.requestId}:`,
            observerError instanceof Error ? observerError.message : String(observerError),
          )
        }
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
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
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
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
  if (isTerminal(task.status.state)) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${params.id}' is in terminal state '${task.status.state}'`,
      ),
    )
  }

  if (isTaskFinalizing(task) || cancels.isFinalizing(task.id)) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${task.id}' is being finalized`,
      ),
    )
  }
  const canceled: Task = {
    ...task,
    status: { state: 'canceled', timestamp: nowIso() },
  }
  const transitioned = await compareAndSetTask(deps.taskStore, task, canceled)
  if (!transitioned) {
    const current = await deps.taskStore.get(task.id)
    if (current && (isTerminal(current.status.state) || isTaskFinalizing(current))) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' changed before cancellation`),
      )
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'task changed before cancellation'))
  }
  const stillActive = cancels.cancel(task.id)

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
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
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
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
  if (!isHttpsUrl(params.pushNotificationConfig.url)) {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url must use https'))
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
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
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
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
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
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await authorizeTaskAccess(c, req, task, deps)
  if (accessError) return accessError
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
  // currently in `input-required`, append the new message and reserve it as
  // `submitted`. The handler transitions it to `working` only after payment
  // succeeds, so cancellation during payment cannot start sandbox work.
  // Any other taskId (unknown OR pointing at a terminal/working
  // task) means the caller is starting a fresh task and we mint a new id.
  if (typeof params.message.taskId === 'string') {
    const existing = await deps.taskStore.get(params.message.taskId)
    if (existing) {
      const accessError = await authorizeTaskAccess(c, req, existing, deps)
      if (accessError) return accessError
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
        status: { state: 'submitted', timestamp: nowIso() },
        history: [...(existing.history ?? []), appendedMessage],
      }
      if (!await compareAndSetTask(deps.taskStore, existing, continued)) {
        return c.json(
          fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${existing.id}' changed before continuation`),
        )
      }
      const claimError = await claimTaskPayment(c, req, continued, authz, deps, existing)
      if (claimError) return claimError
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
  if (!await createTask(deps.taskStore, task)) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' already exists`),
    )
  }
  const claimError = await claimTaskPayment(c, req, task, authz, deps)
  if (claimError) return claimError
  return { authz, task }
}

async function claimTaskPayment(
  c: Context,
  req: JSONRPCRequest,
  task: Task,
  authz: AuthorizedRequest,
  deps: A2AHandlerDeps,
  paymentFailureTask?: Task,
): Promise<Response | undefined> {
  try {
    await claimPayment(authz, deps.config, deps.state)
    return undefined
  } catch {
    await releaseOwnedPayment(authz, deps, 'payment authorization failed')
    const failed = paymentFailureTask ?? withStatus(task, 'failed')
    try {
      if (await compareAndSetTask(deps.taskStore, task, failed) && isTerminal(failed.status.state)) {
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
}

async function releaseOwnedPayment(
  authz: AuthorizedRequest,
  deps: A2AHandlerDeps,
  reason: string,
): Promise<void> {
  try {
    await releasePayment(authz, deps.config, reason)
  } catch (releaseError) {
    console.error(
      `[a2a] payment release failed for ${authz.requestId}:`,
      releaseError instanceof Error ? releaseError.message : String(releaseError),
    )
  }
}

async function releaseOrRetainPayment(
  authz: AuthorizedRequest,
  deps: A2AHandlerDeps,
  reason: string,
  workObserved: boolean,
): Promise<void> {
  try {
    await releasePaymentAfterFailure(authz, deps.config, reason, workObserved)
  } catch (releaseError) {
    console.error(
      `[a2a] payment release failed for ${authz.requestId}:`,
      releaseError instanceof Error ? releaseError.message : String(releaseError),
    )
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────

const FINALIZING_METADATA_KEY = 'gatewayFinalizing'

async function authorizeTaskAccess(
  c: Context,
  req: JSONRPCRequest,
  task: Task,
  deps: A2AHandlerDeps,
): Promise<Response | undefined> {
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
      agentSlug: c.req.param('slug') ?? '',
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

function isHttpsUrl(value: string): boolean {
  try {
    const url = new URL(value)
    return url.protocol === 'https:' && url.username === '' && url.password === ''
  } catch {
    return false
  }
}

async function createTask(taskStore: TaskStore, task: Task): Promise<boolean> {
  if (taskStore.createIfAbsent) return taskStore.createIfAbsent(task)
  if (await taskStore.get(task.id)) return false
  await taskStore.put(task)
  return true
}

async function compareAndSetTask(taskStore: TaskStore, expected: Task, next: Task): Promise<boolean> {
  if (taskStore.compareAndSet) return taskStore.compareAndSet(expected, next)
  // Older adapters remain source-compatible, but their fallback is only
  // process-safe. Durable adapters must implement compareAndSet.
  const current = await taskStore.get(expected.id)
  if (!current || JSON.stringify(current) !== JSON.stringify(expected)) return false
  await taskStore.put(next)
  return true
}

async function claimTaskFinalization(taskStore: TaskStore, task: Task): Promise<boolean> {
  if (isTaskFinalizing(task)) return false
  return compareAndSetTask(taskStore, task, {
    ...task,
    metadata: { ...(task.metadata ?? {}), [FINALIZING_METADATA_KEY]: true },
  })
}

function isTaskFinalizing(task: Task): boolean {
  return task.metadata?.[FINALIZING_METADATA_KEY] === true
}

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
