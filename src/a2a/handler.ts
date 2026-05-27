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
  dispatchSandboxStream,
  estimateTokens,
  settleAndRecord,
} from '../dispatch'
import type { GatewayConfig } from '../types'
import { buildAgentCard } from './agent-card'
import { fail, ok, parseEnvelope } from './jsonrpc'
import type { TaskStore } from './task-store'
import { extractTextFromMessage, responseTextToArtifact } from './translate'
import {
  A2A_ERROR_CODES,
  type JSONRPCRequest,
  type MessageSendParams,
  type StreamingEvent,
  type Task,
  type TaskArtifactUpdateEvent,
  type TaskIdParams,
  type TaskStatusUpdateEvent,
} from './types'

export interface A2AHandlerDeps {
  config: GatewayConfig
  state: GatewayState
  taskStore: TaskStore
}

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
  try {
    for await (const delta of dispatchSandboxStream(
      authz.agent,
      authz.userMessage,
      authz.consumerId,
      deps.config,
    )) {
      responseText += delta
      outputTokens += estimateTokens(delta)
    }
  } catch (err) {
    const finalTask: Task = {
      ...task,
      status: { state: 'failed', timestamp: nowIso() },
    }
    await deps.taskStore.put(finalTask)
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.INTERNAL_ERROR,
        err instanceof Error ? err.message : String(err),
      ),
    )
  }

  const completed: Task = {
    ...task,
    status: { state: 'completed', timestamp: nowIso() },
    artifacts: [responseTextToArtifact(responseText, `${task.id}-artifact-0`)],
  }
  await deps.taskStore.put(completed)
  await settleAndRecord(
    authz.agent,
    authz,
    estimateTokens(authz.userMessage),
    outputTokens,
    deps.config,
    deps.state.obs,
  )
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

      try {
        for await (const delta of dispatchSandboxStream(
          authz.agent,
          authz.userMessage,
          authz.consumerId,
          deps.config,
          controller.signal,
        )) {
          responseText += delta
          outputTokens += estimateTokens(delta)
          const artifactEvent: TaskArtifactUpdateEvent = {
            kind: 'artifact-update',
            taskId: task.id,
            contextId: task.contextId,
            artifact: {
              artifactId: `${task.id}-artifact-0`,
              name: 'response',
              parts: [{ kind: 'text', text: delta }],
            },
            append: true,
          }
          send(artifactEvent)
        }

        // Caller aborted via tasks/cancel — emit canceled, do not settle.
        if (controller.signal.aborted) {
          const canceled: Task = {
            ...task,
            status: { state: 'canceled', timestamp: nowIso() },
            artifacts: [responseTextToArtifact(responseText, `${task.id}-artifact-0`)],
          }
          await deps.taskStore.put(canceled)
          send({
            kind: 'status-update',
            taskId: task.id,
            contextId: task.contextId,
            status: canceled.status,
            final: true,
          })
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
        const completed: Task = {
          ...task,
          status: { state: 'completed', timestamp: nowIso() },
          artifacts: [responseTextToArtifact(responseText, `${task.id}-artifact-0`)],
        }
        await deps.taskStore.put(completed)
        send({
          kind: 'status-update',
          taskId: task.id,
          contextId: task.contextId,
          status: completed.status,
          final: true,
        })
        await settleAndRecord(
          authz.agent,
          authz,
          inputTokens,
          outputTokens,
          deps.config,
          deps.state.obs,
        )
      } catch (err) {
        const failed: Task = {
          ...task,
          status: { state: 'failed', timestamp: nowIso() },
        }
        await deps.taskStore.put(failed)
        send({
          kind: 'status-update',
          taskId: task.id,
          contextId: task.contextId,
          status: failed.status,
          final: true,
        })
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
  // status-update; the dispatcher just acknowledges the cancel here. If no
  // stream was active (synchronous send already complete OR task never
  // started), the cancel is still recorded so callers see consistent state.
  void stillActive
  return c.json(ok(req.id, canceled))
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
