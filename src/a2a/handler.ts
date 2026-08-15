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
  beginPaymentExecution,
  markPaymentExecutionStarted,
  renewPaymentExecution,
  claimPayment,
  dispatchSandboxStreamRich,
  releasePayment,
  releasePaymentAfterFailure,
  settleAndRecord,
} from '../dispatch'
import type { PaymentOperation } from '../payment-operations'
import {
  deserializePaymentOperation,
  serializePaymentOperation,
  type SerializedPaymentOperation,
} from '../payment-recovery'
import { recoverPayment as recoverDurablePayment } from '../payment-recovery-worker'
import type {
  ChatMessage,
  GatewayConfig,
  PaymentMethod,
  SandboxExecutionBudget,
  SandboxUsageReceipt,
} from '../types'
import { buildAgentCard } from './agent-card'
import {
  claimTaskExecution,
  clearTaskExecution,
  hasActiveTaskExecution,
  hasExpiredTaskExecution,
  inspectTaskExecution,
  hasMalformedTaskExecution,
  renewTaskExecution,
} from './execution-fence'
import { fail, ok, parseEnvelope } from './jsonrpc'
import {
  deliverDemoPushNotifications,
  deliverPushNotifications,
  validatePushNotificationUrl,
  type PushNotificationDeliveryOptions,
  type PushDeliveryResult,
  type PushNotificationStore,
  type TaskPushNotificationConfig,
} from './push-notifications'
import { hasPendingPaymentRecovery, type TaskStore } from './task-store'
import { extractTextFromMessage, responseTextToArtifact } from './translate'
import {
  A2A_ERROR_CODES,
  type Artifact,
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

type FinalizationState = 'completed' | 'input-required' | 'canceled'

interface FinalizationRecord {
  version: 1
  lease: { id: string; expiresAt: number }
  agentSlug: string
  requestId: string
  consumerId: string
  paymentMethod: PaymentMethod
  startMs: number
  operationId: string | null
  paymentOperation: SerializedPaymentOperation | null
  receipt: SandboxUsageReceipt
  artifact: Artifact | null
  inputRequired: boolean
  inputRequiredPrompt?: string
  finalState?: FinalizationState
  maxOutputTokens: number
  executionBudget: SandboxExecutionBudget
  usageRecorded: boolean
  recoveryAttempts?: number
  recoveryError?: string
}

interface PaymentReleaseRecord {
  version: 1
  lease: { id: string; expiresAt: number }
  agentSlug: string
  requestId: string
  operationId: string
  paymentOperation: SerializedPaymentOperation
  reason: string
  recoveryAttempts?: number
  recoveryError?: string
}

interface TaskPaymentRecoveryMarker {
  version: 1
  id: string
}

interface TaskPushDeliveryClaims {
  version: 1
  claims: Record<string, Task['status']['state']>
}

interface TaskOriginBinding {
  version: 1
  agentId: string
  agentSlug: string
}

interface TaskSubmissionRecord {
  version: 1
  lease: { id: string; expiresAt: number }
  agentId: string
  agentSlug: string
  requestId: string
  consumerId: string
}

/** Terminal task states — fire-once push delivery occurs on these transitions. */
const TERMINAL_STATES: ReadonlySet<Task['status']['state']> = new Set([
  'completed',
  'canceled',
  'failed',
  'rejected',
])

const TASK_ORIGIN_METADATA_KEY = 'gatewayOrigin'
const TASK_SUBMISSION_METADATA_KEY = 'gatewaySubmission'
const TASK_SUBMISSION_RECOVERY_METADATA_KEY = 'gatewaySubmissionRecovery'
const TASK_SUBMISSION_LEASE_MS = 5 * 60 * 1000
const MAX_A2A_BODY_BYTES = 64 * 1024

class RequestBodyTooLargeError extends Error {}

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

  has(taskId: string): boolean {
    const controller = this.controllers.get(taskId)
    return controller !== undefined && !controller.signal.aborted
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
  const runtimeDeps: A2AHandlerDeps = {
    ...deps,
    taskStore: normalizeTaskStore(deps.taskStore, deps.config.x402.demoMode === true),
  }
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
        return handleTasksGet(c, parsed, runtimeDeps)
      case 'tasks/cancel':
        return handleTasksCancel(c, parsed, runtimeDeps, cancels)
      case 'tasks/resubscribe':
        return handleTasksResubscribe(c, parsed, runtimeDeps)
      case 'tasks/pushNotificationConfig/set':
        return handlePushSet(c, parsed, runtimeDeps)
      case 'tasks/pushNotificationConfig/get':
        return handlePushGet(c, parsed, runtimeDeps)
      case 'tasks/pushNotificationConfig/list':
        return handlePushList(c, parsed, runtimeDeps)
      case 'tasks/pushNotificationConfig/delete':
        return handlePushDelete(c, parsed, runtimeDeps)
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
  cancels: CancelRegistry,
): Promise<Response> {
  const guard = await guardMessageRequest(c, slug, req, deps)
  if (guard instanceof Response) return guard
  const { authz, task } = guard
  setPaymentResponseHeaders(c, authz)
  const controller = cancels.register(task.id)
  const detachRequestAbort = bindRequestAbort(c.req.raw.signal, controller)
  try {
    return await executeMessageSend(c, req, deps, authz, task, controller.signal)
  } finally {
    detachRequestAbort()
    cancels.clear(task.id)
  }
}

async function executeMessageSend(
  c: Context,
  req: JSONRPCRequest,
  deps: A2AHandlerDeps,
  authz: AuthorizedRequest,
  task: Task,
  signal: AbortSignal,
): Promise<Response> {
  if (isTerminal(task.status.state)) return c.json(ok(req.id, task))
  const taskWithoutSubmission = clearTaskSubmission(task)
  let workingTask: Task = task.status.state === 'working'
    ? taskWithoutSubmission
    : { ...taskWithoutSubmission, status: { state: 'working', timestamp: nowIso() } }
  if (
    JSON.stringify(task) !== JSON.stringify(workingTask) &&
    !await compareAndSetTask(deps.taskStore, task, workingTask)
  ) {
    await releaseTaskPayment(authz, task, deps, 'A2A task changed before execution started', false)
    if (signal.aborted) {
      const canceled = await deps.taskStore.get(task.id)
      if (canceled?.status.state === 'canceled') {
        await maybeDeliverPush(canceled, deps)
        return c.json(ok(req.id, canceled))
      }
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' changed before execution`))
  }

  let responseText = ''
  let usage: SandboxUsageReceipt | undefined
  let workObserved = false
  let inputRequiredPrompt: string | undefined
  let inputRequiredSeen = false
  let finalizationLeaseId: string | undefined
  try {
    for await (const event of dispatchSandboxStreamRich(
      authz.agent,
      authz.userMessage,
      authz.consumerId,
      deps.config,
      signal,
      task.id,
      authz.maxOutputTokens,
      async () => {
        workingTask = await claimTaskExecution(deps.taskStore, workingTask, authz.requestId)
        await beginPaymentExecution(authz, deps.config)
      },
      authz.paymentOperation !== undefined || authz.mppChargeOperation !== undefined,
      async () => {
        workObserved = true
        await markPaymentExecutionStarted(authz, deps.config)
      },
      authz.executionBudget.maxInputTokens,
      async () => {
        workingTask = await renewTaskExecution(deps.taskStore, task.id, authz.requestId)
        await renewPaymentExecution(authz, deps.config)
      },
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
    const releasedTask = await releaseTaskPayment(
      authz,
      workingTask,
      deps,
      err instanceof Error ? err.message : String(err),
      workObserved || usage !== undefined,
    )
    const currentTask = await deps.taskStore.get(task.id) ?? releasedTask
    const failed = shouldPreserveTask(currentTask)
      ? currentTask
      : withStatus(clearTaskSubmission(currentTask), 'failed')
    try {
      const persisted = await persistTaskIfCurrent(deps.taskStore, currentTask, failed)
      await maybeDeliverPush(persisted, deps)
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

  if (signal.aborted) {
    const canceled = await completeCanceledTask(
      authz,
      workingTask,
      responseText,
      usage,
      workObserved,
      deps,
    )
    return c.json(ok(req.id, canceled))
  }

  // Settle for the work done so far before short-circuiting on input-required.
  // The user has been charged for the partial response, which is the right
  // commercial behavior — the sandbox produced tokens.
  try {
    if (!usage) throw new Error('sandbox did not provide a usage receipt')
    const finalizationArtifact = responseText
      ? responseTextToArtifact(responseText, `${task.id}-artifact-0`)
      : task.artifacts?.[0] ?? null
    const finalization = buildFinalizationRecord(
      authz,
      usage,
      finalizationArtifact,
      inputRequiredSeen,
      inputRequiredPrompt,
    )
    const finalizingTask = withFinalizationRecord(workingTask, finalization)
    if (!await compareAndSetTask(deps.taskStore, workingTask, finalizingTask)) {
      const currentTask = await deps.taskStore.get(task.id)
      if (currentTask?.status.state === 'canceled') {
        const canceled = await completeCanceledTask(
          authz,
          currentTask,
          responseText,
          usage,
          workObserved,
          deps,
        )
        return c.json(ok(req.id, canceled))
      }
      throw new Error('A2A task changed before payment settlement')
    }
    finalizationLeaseId = finalization.lease.id
    let usageRecordedTask = finalizingTask
    await settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs, {
      onUsageRecorded: async () => {
        usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
      },
    })
    usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
    const settledBase = clearPaymentRecoveryMarker(clearFinalizationMarker(usageRecordedTask))
    const result = inputRequiredSeen
      ? withStatus(
          settledBase,
          'input-required',
          inputRequiredPrompt ? agentMessage(task, inputRequiredPrompt) : undefined,
          responseText
            ? [responseTextToArtifact(responseText, `${task.id}-artifact-0`)]
            : task.artifacts,
        )
      : withStatus(settledBase, 'completed', undefined, [
          responseTextToArtifact(responseText, `${task.id}-artifact-0`),
        ])
    if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, result)) {
      const currentTask = await deps.taskStore.get(task.id)
      if (currentTask && (isTerminal(currentTask.status.state) || currentTask.status.state === 'input-required')) {
        return c.json(ok(req.id, currentTask))
      }
      throw new Error('A2A task changed after payment settlement')
    }
    if (inputRequiredSeen) return c.json(ok(req.id, result))
    await maybeDeliverPush(result, deps)
    return c.json(ok(req.id, result))
  } catch (err) {
    const releasedTask = await releaseTaskPayment(
      authz,
      workingTask,
      deps,
      err instanceof Error ? err.message : String(err),
      workObserved || usage !== undefined,
    )
    if (finalizationLeaseId) {
      await retainFinalizationForRecovery(
        deps.taskStore,
        task.id,
        finalizationLeaseId,
        asError(err),
      )
      return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'Payment settlement failed'))
    }
    const currentTask = await deps.taskStore.get(task.id) ?? releasedTask
    const failed = shouldPreserveTask(currentTask)
      ? currentTask
      : withStatus(clearTaskSubmission(currentTask), 'failed')
    try {
      const persisted = await persistTaskIfCurrent(deps.taskStore, currentTask, failed)
      await maybeDeliverPush(persisted, deps)
    } catch (taskError) {
      console.error(
        `[a2a] failed to persist failed task ${task.id}:`,
        taskError instanceof Error ? taskError.message : String(taskError),
      )
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'Payment settlement failed'))
  }
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
  setPaymentResponseHeaders(c, authz)

  const controller = cancels.register(task.id)
  const detachRequestAbort = bindRequestAbort(c.req.raw.signal, controller)
  const workingStatus: TaskStatusUpdateEvent = {
    kind: 'status-update',
    taskId: task.id,
    contextId: task.contextId,
    status: { state: 'working', timestamp: nowIso() },
    final: false,
  }
  if (isTerminal(task.status.state)) {
    detachRequestAbort()
    cancels.clear(task.id)
    return c.json(ok(req.id, task))
  }
  let workingTask: Task = task.status.state === 'working'
    ? task
    : { ...task, status: workingStatus.status }
  if (task.status.state !== 'working' && !await compareAndSetTask(deps.taskStore, task, workingTask)) {
    detachRequestAbort()
    cancels.clear(task.id)
    const current = await releaseTaskPayment(
      authz,
      await deps.taskStore.get(task.id) ?? task,
      deps,
      'A2A task changed before execution started',
      false,
    )
    if (current.status.state === 'canceled') return c.json(ok(req.id, current))
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' changed before execution`))
  }
  let responseText = ''
  let usage: SandboxUsageReceipt | undefined
  let workObserved = false

  const stream = new ReadableStream({
    start(ctrl) {
      void (async () => {
        const encoder = new TextEncoder()
        const send = (event: StreamingEvent) => {
          if (ctrl.desiredSize === null) return
          try {
            ctrl.enqueue(encoder.encode(`data: ${JSON.stringify(ok(req.id, event))}\n\n`))
          } catch {
            // The client can cancel between the desiredSize check and enqueue.
          }
        }

        let inputRequiredPrompt: string | undefined
        let inputRequiredSeen = false
        let finalizationLeaseId: string | undefined
        try {
          send(workingStatus)

          for await (const event of dispatchSandboxStreamRich(
            authz.agent,
            authz.userMessage,
            authz.consumerId,
            deps.config,
            controller.signal,
            task.id,
            authz.maxOutputTokens,
            async () => {
              workingTask = await claimTaskExecution(deps.taskStore, workingTask, authz.requestId)
              await beginPaymentExecution(authz, deps.config)
            },
            authz.paymentOperation !== undefined || authz.mppChargeOperation !== undefined,
            async () => {
              workObserved = true
              await markPaymentExecutionStarted(authz, deps.config)
            },
            authz.executionBudget.maxInputTokens,
            async () => {
              workingTask = await renewTaskExecution(deps.taskStore, task.id, authz.requestId)
              await renewPaymentExecution(authz, deps.config)
            },
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
            const canceled = await completeCanceledTask(
              authz,
              workingTask,
              responseText,
              usage,
              workObserved,
              deps,
            )
            send({
              kind: 'status-update',
              taskId: task.id,
              contextId: task.contextId,
              status: canceled.status,
              final: true,
            })
            return
          }

          // Settle once for whatever the sandbox produced (full or partial).
          if (!usage) throw new Error('sandbox did not provide a usage receipt')
          const finalizationArtifact = responseTextToArtifact(responseText, `${task.id}-artifact-0`)
          const finalization = buildFinalizationRecord(
            authz,
            usage,
            finalizationArtifact,
            inputRequiredSeen,
            inputRequiredPrompt,
          )
          // Let the durable task-store CAS decide the cancellation race. Mark
          // the local registry only after that CAS wins, so cancel can replace a
          // still-pending finalization instead of being rejected prematurely.
          const finalizingTask = withFinalizationRecord(workingTask, finalization)
          if (!await compareAndSetTask(deps.taskStore, workingTask, finalizingTask)) {
            const currentTask = await deps.taskStore.get(task.id)
            if (currentTask?.status.state === 'canceled') {
              const canceled = await completeCanceledTask(
                authz,
                currentTask,
                responseText,
                usage,
                workObserved,
                deps,
              )
              send({
                kind: 'status-update',
                taskId: task.id,
                contextId: task.contextId,
                status: canceled.status,
                final: true,
              })
              return
            }
            await releaseTaskPayment(
              authz,
              task,
              deps,
              'A2A task changed before payment settlement',
              workObserved || usage !== undefined,
            )
            return
          }
          finalizationLeaseId = finalization.lease.id
          cancels.beginFinalization(task.id)
          let usageRecordedTask = finalizingTask
          await settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs, {
            onUsageRecorded: async () => {
              usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
            },
          })
          usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)

          if (inputRequiredSeen) {
            const paused = withStatus(
              clearPaymentRecoveryMarker(clearFinalizationMarker(usageRecordedTask)),
              'input-required',
              inputRequiredPrompt ? agentMessage(task, inputRequiredPrompt) : undefined,
              responseText
                ? [responseTextToArtifact(responseText, `${task.id}-artifact-0`)]
                : task.artifacts,
            )
            if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, paused)) {
              const currentTask = await deps.taskStore.get(task.id)
              if (currentTask) {
                send({
                  kind: 'status-update',
                  taskId: task.id,
                  contextId: task.contextId,
                  status: currentTask.status,
                  final: isTerminal(currentTask.status.state) || currentTask.status.state === 'input-required',
                })
              }
              return
            }
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

          // Final: persist the terminal task before emitting terminal events.
          const completed = withStatus(clearPaymentRecoveryMarker(clearFinalizationMarker(usageRecordedTask)), 'completed', undefined, [
            responseTextToArtifact(responseText, `${task.id}-artifact-0`),
          ])
          if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, completed)) {
            const currentTask = await deps.taskStore.get(task.id)
            if (currentTask) {
              send({
                kind: 'status-update',
                taskId: task.id,
                contextId: task.contextId,
                status: currentTask.status,
                final: isTerminal(currentTask.status.state) || currentTask.status.state === 'input-required',
              })
            }
            return
          }
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
          send({
            kind: 'status-update',
            taskId: task.id,
            contextId: task.contextId,
            status: completed.status,
            final: true,
          })
          await maybeDeliverPush(completed, deps)
        } catch (err) {
          const releasedTask = await releaseTaskPayment(
            authz,
            task,
            deps,
            err instanceof Error ? err.message : String(err),
            workObserved || usage !== undefined,
          )
          if (finalizationLeaseId) {
            const retained = await retainFinalizationForRecovery(
              deps.taskStore,
              task.id,
              finalizationLeaseId,
              asError(err),
            )
            if (retained) {
              send({
                kind: 'status-update',
                taskId: task.id,
                contextId: task.contextId,
                status: retained.status,
                final: false,
              })
            }
            return
          }
          const currentTask = await deps.taskStore.get(task.id) ?? releasedTask
          const failed = shouldPreserveTask(currentTask)
            ? currentTask
            : withStatus(clearTaskSubmission(currentTask), 'failed')
          try {
            const persisted = await persistTaskIfCurrent(deps.taskStore, currentTask, failed)
            send({
              kind: 'status-update',
              taskId: task.id,
              contextId: task.contextId,
              status: persisted.status,
              final: true,
            })
            await maybeDeliverPush(persisted, deps)
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
          detachRequestAbort()
          cancels.clear(task.id)
          try {
            if (ctrl.desiredSize !== null) ctrl.close()
          } catch {
            // The response may already be closed by client cancellation.
          }
        }
      })()
    },
    cancel() {
      controller.abort()
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Request-Id': authz.requestId,
      'X-Agent-Slug': authz.agent.slug,
      'X-Task-Id': task.id,
      ...(authz.mppChargeOperation
        ? { 'Payment-Receipt': authz.mppChargeOperation.receipt }
        : {}),
      ...(authz.paymentRecoveryId
        ? { 'X-Payment-Operation-Id': authz.paymentRecoveryId }
        : {}),
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
  const storedTask = await deps.taskStore.get(params.id)
  if (!storedTask) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`),
    )
  }
  const accessError = await authorizeTaskAccess(c, req, storedTask, deps)
  if (accessError) return accessError
  const task = await recoverTaskIfNeeded(
    storedTask,
    deps,
    c.req.param('slug') ?? '',
  )
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
  const storedTask = await deps.taskStore.get(params.id)
  if (!storedTask) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`),
    )
  }
  const accessError = await authorizeTaskAccess(c, req, storedTask, deps)
  if (accessError) return accessError
  const task = await recoverTaskIfNeeded(
    storedTask,
    deps,
    c.req.param('slug') ?? '',
  )
  if (isTerminal(task.status.state)) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${params.id}' is in terminal state '${task.status.state}'`,
      ),
    )
  }

  if (
    isTaskFinalizing(task) ||
    cancels.isFinalizing(task.id) ||
    (hasActiveTaskExecution(task) && !cancels.has(task.id))
  ) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        hasActiveTaskExecution(task)
          ? `task '${task.id}' has an active execution fence`
          : `task '${task.id}' is being finalized`,
      ),
    )
  }
  let stillActive = false
  let candidate = task
  for (let attempt = 0; attempt < 8; attempt += 1) {
    if (isTerminal(candidate.status.state)) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' changed before cancellation`),
      )
    }
    if (isTaskFinalizing(candidate)) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' is being finalized`),
      )
    }
    if (hasActiveTaskExecution(candidate) && !cancels.has(candidate.id)) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' has an active execution fence`),
      )
    }
    const canceled = withStatus(candidate, 'canceled')
    if (await compareAndSetTask(deps.taskStore, candidate, canceled)) {
      stillActive = cancels.cancel(task.id)
      // If a stream was active, it observes the abort and emits its own final
      // status update and push delivery. Otherwise this handler owns delivery.
      if (!stillActive) await maybeDeliverPush(canceled, deps)
      return c.json(ok(req.id, canceled))
    }
    const current = await deps.taskStore.get(task.id)
    if (!current) {
      return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${task.id}' not found`))
    }
    candidate = current
  }
  return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'task changed before cancellation'))
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
  const storedTask = await deps.taskStore.get(params.id)
  if (!storedTask) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`),
    )
  }
  const accessError = await authorizeTaskAccess(c, req, storedTask, deps)
  if (accessError) return accessError
  const task = await recoverTaskIfNeeded(
    storedTask,
    deps,
    c.req.param('slug') ?? '',
  )
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
  const pushUrl = validatePushNotificationUrl(params.pushNotificationConfig.url)
  if (!pushUrl) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url is not a safe HTTPS destination'),
    )
  }
  const urlValidator = deps.config.a2a?.pushUrlValidator
  if (!deps.config.x402.demoMode && !urlValidator) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'production push URL validation is not configured'),
    )
  }
  let allowedByHostPolicy = true
  try {
    if (urlValidator) allowedByHostPolicy = await urlValidator(pushUrl)
  } catch (error) {
    allowedByHostPolicy = false
    console.error(
      `[a2a] push URL policy failed for task ${task.id}:`,
      error instanceof Error ? error.message : String(error),
    )
  }
  if (!allowedByHostPolicy) {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url was rejected'))
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

  let billingMessages: ChatMessage[] = [{ role: 'user', content: extracted.text }]
  if (typeof params.message.taskId === 'string') {
    const storedForQuote = await deps.taskStore.get(params.message.taskId)
    if (storedForQuote) {
      const accessError = await authorizeTaskAccess(c, req, storedForQuote, deps)
      if (accessError) return accessError
      const quotedTask = await recoverTaskIfNeeded(storedForQuote, deps, slug)
      if (quotedTask.status.state === 'input-required') {
        billingMessages = [
          ...taskHistoryAsChatMessages(quotedTask),
          { role: 'user', content: extracted.text },
        ]
      }
    }
  }

  const guard = await authenticateAndGuard(
    c,
    slug,
    billingMessages,
    deps.config,
    deps.state,
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
  // Any other taskId (unknown OR pointing at a terminal/working
  // task) means the caller is starting a fresh task and we mint a new id.
  if (typeof params.message.taskId === 'string') {
    const storedExisting = await deps.taskStore.get(params.message.taskId)
    if (storedExisting) {
      const accessError = await authorizeTaskAccess(c, req, storedExisting, deps)
      if (accessError) return accessError
      const existing = await recoverTaskIfNeeded(storedExisting, deps, slug)
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
      deps,
      'payment authorization failed',
      false,
    )
    const cleanedReleasedTask = clearTaskSubmission(releasedTask)
    const releaseRecord = cleanedReleasedTask.metadata?.[PAYMENT_RELEASE_METADATA_KEY]
    const failed = releaseRecord !== undefined
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
      deps,
      'A2A task changed during payment confirmation',
      false,
    )
    if (released.status.state === 'canceled') return c.json(ok(req.id, released))
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' changed during payment confirmation`),
    )
  }
}

async function releaseTaskPayment(
  authz: AuthorizedRequest,
  task: Task,
  deps: A2AHandlerDeps,
  reason: string,
  workObserved: boolean,
): Promise<Task> {
  // Store the operation before release because the adapter acknowledgement can be ambiguous.
  if (
    !workObserved &&
    !authz.paymentRecoveryId &&
    authz.paymentOperation &&
    deps.config.x402.paymentOperations
  ) {
    let marked: Task
    try {
      marked = await beginPaymentReleaseRecovery(deps.taskStore, task, authz, reason) ?? task
    } catch (error) {
      console.error(
        '[a2a] failed to persist payment release recovery for ' + authz.requestId + ':',
        error instanceof Error ? error.message : String(error),
      )
      return await deps.taskStore.get(task.id) ?? task
    }
    const record = readPaymentReleaseRecord(marked)
    if (!record) return marked
    try {
      await releasePayment(authz, deps.config, reason)
    } catch (releaseError) {
      const retained = await retainPaymentReleaseForRecovery(
        deps.taskStore,
        task.id,
        record.lease.id,
        releaseError instanceof Error ? releaseError : new Error(String(releaseError)),
      )
      console.error(
        '[a2a] payment release retained for ' + authz.requestId + ':',
        releaseError instanceof Error ? releaseError.message : String(releaseError),
      )
      return retained ?? marked
    }
    return clearPaymentReleaseRecovery(deps.taskStore, marked, record.lease.id)
  }

  try {
    await releasePaymentAfterFailure(authz, deps.config, reason, workObserved)
  } catch (releaseError) {
    console.error(
      `[a2a] payment release failed for ${authz.requestId}:`,
      releaseError instanceof Error ? releaseError.message : String(releaseError),
    )
  }
  const current = await deps.taskStore.get(task.id) ?? task
  return workObserved
    ? current
    : clearReconciledPaymentRecoveryMarker(current, deps)
}

/** Keep an owned finalization record durable when settlement acknowledgement is lost. */
async function retainFinalizationForRecovery(
  taskStore: TaskStore,
  taskId: string,
  leaseId: string,
  error: Error,
): Promise<Task | undefined> {
  const current = await taskStore.get(taskId)
  if (!current) return undefined
  const record = readFinalizationRecord(current)
  if (!record || record.lease.id !== leaseId) return undefined
  const retry: FinalizationRecord = {
    ...record,
    lease: { id: cryptoRandomId(), expiresAt: Date.now() + FINALIZATION_LEASE_MS },
    recoveryAttempts: (record.recoveryAttempts ?? 0) + 1,
    recoveryError: error.message,
  }
  const next = withFinalizationRecord(current, retry)
  if (await compareAndSetTask(taskStore, current, next)) return next
  return await taskStore.get(taskId)
}

async function completeCanceledTask(
  authz: AuthorizedRequest,
  task: Task,
  responseText: string,
  usage: SandboxUsageReceipt | undefined,
  workObserved: boolean,
  deps: A2AHandlerDeps,
): Promise<Task> {
  if (usage) {
    const current = await deps.taskStore.get(task.id) ?? task
    const finalization = buildFinalizationRecord(
      authz,
      usage,
      responseText
        ? responseTextToArtifact(responseText, `${task.id}-artifact-0`)
        : current.artifacts?.[0] ?? null,
      false,
      undefined,
      'canceled',
    )
    let finalizingTask: Task | undefined
    let candidate = current
    for (let attempt = 0; attempt < 8; attempt += 1) {
      if (isTaskFinalizing(candidate)) return candidate
      if (isTerminal(candidate.status.state) && candidate.status.state !== 'canceled') return candidate
      // Store the lease before settlement can move the payment to settling.
      const next = withFinalizationRecord(candidate, finalization)
      if (await compareAndSetTask(deps.taskStore, candidate, next)) {
        finalizingTask = next
        break
      }
      const latest = await deps.taskStore.get(task.id)
      if (!latest) break
      if (isTerminal(latest.status.state) && latest.status.state !== 'canceled') return latest
      candidate = latest
    }
    if (!finalizingTask) {
      throw new Error(`A2A task '${task.id}' changed before cancellation settlement`)
    }

    let usageRecordedTask = finalizingTask
    try {
      await settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs, {
        onUsageRecorded: async () => {
          usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
        },
      })
      usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
    } catch (settlementError) {
      await releasePaymentAfterFailure(
        authz,
        deps.config,
        settlementError instanceof Error ? settlementError.message : String(settlementError),
        true,
      )
      const retained = await retainFinalizationForRecovery(
        deps.taskStore,
        task.id,
        finalization.lease.id,
        asError(settlementError),
      )
      const recoveryTask = retained ?? finalizingTask
      console.error(
        `[a2a] canceled task settlement retained for ${authz.requestId}:`,
        settlementError instanceof Error ? settlementError.message : String(settlementError),
      )
      await maybeDeliverPush(recoveryTask, deps)
      return recoveryTask
    }

    const canceled = withStatus(
      clearPaymentRecoveryMarker(clearFinalizationMarker(usageRecordedTask)),
      'canceled',
      undefined,
      responseText
        ? [responseTextToArtifact(responseText, `${task.id}-artifact-0`)]
        : finalizingTask.artifacts,
    )
    if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, canceled)) {
      return await deps.taskStore.get(task.id) ?? canceled
    }
    await maybeDeliverPush(canceled, deps)
    return canceled
  }
  await releaseTaskPayment(authz, task, deps, 'a2a task canceled', workObserved)
  const currentTask = await deps.taskStore.get(task.id)
  const canceledBase = currentTask?.status.state === 'canceled'
    ? currentTask
    : withStatus(currentTask ?? task, 'canceled')
  const canceled: Task = responseText
    ? {
        ...canceledBase,
        artifacts: [responseTextToArtifact(responseText, `${task.id}-artifact-0`)],
      }
    : canceledBase
  const persisted = await persistTaskIfCurrent(deps.taskStore, currentTask ?? task, canceled)
  await maybeDeliverPush(persisted, deps)
  return persisted
}

// ── Helpers ───────────────────────────────────────────────────────────────

const FINALIZING_METADATA_KEY = 'gatewayFinalizing'
const PAYMENT_RELEASE_METADATA_KEY = 'gatewayPaymentRelease'
const PAYMENT_RECOVERY_METADATA_KEY = 'gatewayPaymentRecovery'
const EXECUTION_RECOVERY_METADATA_KEY = 'gatewayExecutionRecovery'
const PUSH_DELIVERY_METADATA_KEY = 'gatewayPushDelivery'
const FINALIZATION_LEASE_MS = 5 * 60 * 1000
const PAYMENT_RELEASE_LEASE_MS = 5 * 60 * 1000

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
    if (origin.agentSlug !== requestedAgentSlug) {
      return c.json(fail(req.id, A2A_ERROR_CODES.TASK_ACCESS_DENIED, 'task belongs to a different agent'), 403)
    }
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

function bindRequestAbort(requestSignal: AbortSignal, controller: AbortController): () => void {
  const abort = () => controller.abort()
  if (requestSignal.aborted) abort()
  else requestSignal.addEventListener('abort', abort, { once: true })
  return () => requestSignal.removeEventListener('abort', abort)
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

function withTaskOrigin(
  metadata: Record<string, unknown> | undefined,
  agent: { id: string; slug: string },
): Record<string, unknown> {
  return {
    ...(metadata ?? {}),
    [TASK_ORIGIN_METADATA_KEY]: {
      version: 1,
      agentId: agent.id,
      agentSlug: agent.slug,
    } satisfies TaskOriginBinding,
  }
}

function withTaskSubmission(
  metadata: Record<string, unknown> | undefined,
  authz: AuthorizedRequest,
): Record<string, unknown> {
  const origin = metadata?.[TASK_ORIGIN_METADATA_KEY]
  return {
    ...(metadata ?? {}),
    ...(origin === undefined
      ? {
          [TASK_ORIGIN_METADATA_KEY]: {
            version: 1,
            agentId: authz.agent.id,
            agentSlug: authz.agent.slug,
          } satisfies TaskOriginBinding,
        }
      : {}),
    [TASK_SUBMISSION_METADATA_KEY]: {
      version: 1,
      lease: { id: cryptoRandomId(), expiresAt: Date.now() + TASK_SUBMISSION_LEASE_MS },
      agentId: authz.agent.id,
      agentSlug: authz.agent.slug,
      requestId: authz.requestId,
      consumerId: authz.consumerId,
    } satisfies TaskSubmissionRecord,
  }
}

function readTaskOrigin(task: Task): TaskOriginBinding | undefined {
  const raw = task.metadata?.[TASK_ORIGIN_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const origin = raw as Partial<TaskOriginBinding>
  if (
    origin.version !== 1 ||
    typeof origin.agentId !== 'string' ||
    origin.agentId.length === 0 ||
    typeof origin.agentSlug !== 'string' ||
    origin.agentSlug.length === 0
  ) {
    return undefined
  }
  return origin as TaskOriginBinding
}

function readTaskSubmission(task: Task): TaskSubmissionRecord | undefined {
  const raw = task.metadata?.[TASK_SUBMISSION_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const submission = raw as Partial<TaskSubmissionRecord>
  if (
    submission.version !== 1 ||
    !submission.lease ||
    typeof submission.lease.id !== 'string' ||
    submission.lease.id.length === 0 ||
    typeof submission.lease.expiresAt !== 'number' ||
    !Number.isFinite(submission.lease.expiresAt) ||
    typeof submission.agentId !== 'string' ||
    submission.agentId.length === 0 ||
    typeof submission.agentSlug !== 'string' ||
    submission.agentSlug.length === 0 ||
    typeof submission.requestId !== 'string' ||
    submission.requestId.length === 0 ||
    typeof submission.consumerId !== 'string'
  ) {
    return undefined
  }
  return submission as TaskSubmissionRecord
}

function clearTaskSubmission(task: Task): Task {
  if (!task.metadata || !(TASK_SUBMISSION_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[TASK_SUBMISSION_METADATA_KEY]
  return Object.keys(metadata).length > 0
    ? { ...task, metadata }
    : (() => {
        const { metadata: _metadata, ...withoutMetadata } = task
        return withoutMetadata
      })()
}

async function clearTaskSubmissionMarker(
  taskStore: TaskStore,
  expected: Task,
): Promise<{ task: Task; applied: boolean }> {
  const current = await taskStore.get(expected.id)
  if (!current || JSON.stringify(current) !== JSON.stringify(expected)) {
    return { task: current ?? expected, applied: false }
  }
  const cleared = clearTaskSubmission(current)
  if (cleared === current) return { task: current, applied: true }
  if (await compareAndSetTask(taskStore, current, cleared)) return { task: cleared, applied: true }
  return { task: await taskStore.get(expected.id) ?? expected, applied: false }
}

function shouldPreserveTask(task: Task): boolean {
  return isTerminal(task.status.state) || hasPendingPaymentRecovery(task)
}

async function persistTaskIfCurrent(
  taskStore: TaskStore,
  expected: Task,
  next: Task,
): Promise<Task> {
  if (expected === next || JSON.stringify(expected) === JSON.stringify(next)) return expected
  if (await compareAndSetTask(taskStore, expected, next)) return next
  return await taskStore.get(expected.id) ?? expected
}

async function createTask(taskStore: TaskStore, task: Task): Promise<boolean> {
  if (!taskStore.createIfAbsent) {
    throw new Error('A2A task store does not provide createIfAbsent')
  }
  return taskStore.createIfAbsent(task)
}

async function compareAndSetTask(taskStore: TaskStore, expected: Task, next: Task): Promise<boolean> {
  if (!taskStore.compareAndSet) {
    throw new Error('A2A task store does not provide compareAndSet')
  }
  return taskStore.compareAndSet(expected, next)
}

async function attachPaymentRecoveryMarker(
  taskStore: TaskStore,
  task: Task,
  recoveryId: string | undefined,
): Promise<Task> {
  if (!recoveryId) return task
  const existing = readPaymentRecoveryMarker(task)
  if (existing) {
    if (existing.id !== recoveryId) {
      throw new Error('A2A task already has a different payment recovery identity')
    }
    return task
  }
  const next = withPaymentRecoveryMarker(task, recoveryId)
  if (await compareAndSetTask(taskStore, task, next)) return next
  throw new Error('A2A task changed while payment recovery was attached')
}

/** Attach only as a retention marker. The returned task must never execute. */
async function retainPaymentRecoveryMarker(
  taskStore: TaskStore,
  task: Task,
  recoveryId: string | undefined,
): Promise<Task> {
  if (!recoveryId) return task
  let current = await taskStore.get(task.id) ?? task
  for (let attempt = 0; attempt < 16; attempt += 1) {
    const existing = readPaymentRecoveryMarker(current)
    if (existing) {
      if (existing.id !== recoveryId) {
        throw new Error('A2A task already has a different payment recovery identity')
      }
      return current
    }
    const next = withPaymentRecoveryMarker(current, recoveryId)
    if (await compareAndSetTask(taskStore, current, next)) return next
    const latest = await taskStore.get(task.id)
    if (!latest) throw new Error('A2A task disappeared while payment recovery was retained')
    current = latest
  }
  throw new Error('A2A task changed too many times while payment recovery was retained')
}

function withPaymentRecoveryMarker(task: Task, recoveryId: string): Task {
  return {
    ...task,
    metadata: {
      ...(task.metadata ?? {}),
      [PAYMENT_RECOVERY_METADATA_KEY]: { version: 1, id: recoveryId },
    },
  }
}

function preservePaymentRecoveryMarker(base: Task, source: Task): Task {
  const marker = readPaymentRecoveryMarker(source)
  return marker ? withPaymentRecoveryMarker(base, marker.id) : base
}

function readPaymentRecoveryMarker(task: Task): TaskPaymentRecoveryMarker | undefined {
  const raw = task.metadata?.[PAYMENT_RECOVERY_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const marker = raw as Partial<TaskPaymentRecoveryMarker>
  if (marker.version !== 1 || typeof marker.id !== 'string' || marker.id.length === 0) {
    return undefined
  }
  return marker as TaskPaymentRecoveryMarker
}

function clearPaymentRecoveryMarker(task: Task): Task {
  if (!task.metadata || !(PAYMENT_RECOVERY_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[PAYMENT_RECOVERY_METADATA_KEY]
  return Object.keys(metadata).length > 0
    ? { ...task, metadata }
    : (() => {
        const { metadata: _metadata, ...withoutMetadata } = task
        return withoutMetadata
      })()
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

function buildFinalizationRecord(
  authz: AuthorizedRequest,
  receipt: SandboxUsageReceipt,
  artifact: Artifact | null,
  inputRequired: boolean,
  inputRequiredPrompt: string | undefined,
  finalState: FinalizationState = inputRequired ? 'input-required' : 'completed',
): FinalizationRecord {
  const operation = authz.paymentOperation
  return {
    version: 1,
    lease: { id: cryptoRandomId(), expiresAt: Date.now() + FINALIZATION_LEASE_MS },
    agentSlug: authz.agent.slug,
    requestId: authz.requestId,
    consumerId: authz.consumerId,
    paymentMethod: authz.paymentMethod,
    startMs: authz.startMs,
    operationId: operation?.operationId ?? null,
    paymentOperation: operation ? serializePaymentOperation(operation) : null,
    receipt,
    artifact,
    inputRequired,
    ...(inputRequiredPrompt ? { inputRequiredPrompt } : {}),
    finalState,
    maxOutputTokens: authz.maxOutputTokens,
    executionBudget: authz.executionBudget,
    usageRecorded: false,
  }
}

function withFinalizationRecord(task: Task, record: FinalizationRecord): Task {
  return {
    ...task,
    metadata: { ...(task.metadata ?? {}), [FINALIZING_METADATA_KEY]: record },
  }
}

function markUsageRecordedRecord(task: Task): Task {
  const record = readFinalizationRecord(task)
  if (!record || record.usageRecorded) return task
  return withFinalizationRecord(task, { ...record, usageRecorded: true })
}

async function markUsageRecorded(taskStore: TaskStore, task: Task): Promise<Task> {
  const marked = markUsageRecordedRecord(task)
  if (marked === task) return task
  if (await compareAndSetTask(taskStore, task, marked)) return marked
  return await taskStore.get(task.id) ?? marked
}

function withPaymentReleaseRecord(task: Task, record: PaymentReleaseRecord): Task {
  return {
    ...task,
    metadata: { ...(task.metadata ?? {}), [PAYMENT_RELEASE_METADATA_KEY]: record },
  }
}

function clearPaymentReleaseRecord(task: Task): Task {
  if (!task.metadata || !(PAYMENT_RELEASE_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[PAYMENT_RELEASE_METADATA_KEY]
  return Object.keys(metadata).length > 0
    ? { ...task, metadata }
    : (() => {
        const { metadata: _metadata, ...withoutMetadata } = task
        return withoutMetadata
      })()
}

function readPaymentReleaseRecord(task: Task): PaymentReleaseRecord | undefined {
  const raw = task.metadata?.[PAYMENT_RELEASE_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const record = raw as Partial<PaymentReleaseRecord>
  if (
    record.version !== 1 ||
    !record.lease ||
    typeof record.lease.id !== 'string' ||
    record.lease.id.length === 0 ||
    typeof record.lease.expiresAt !== 'number' ||
    !Number.isFinite(record.lease.expiresAt) ||
    typeof record.agentSlug !== 'string' ||
    record.agentSlug.length === 0 ||
    typeof record.requestId !== 'string' ||
    record.requestId.length === 0 ||
    typeof record.operationId !== 'string' ||
    record.operationId.length === 0 ||
    !record.paymentOperation ||
    typeof record.paymentOperation !== 'object' ||
    typeof record.reason !== 'string'
  ) {
    return undefined
  }
  if (record.paymentOperation.operationId !== record.operationId) return undefined
  try {
    deserializePaymentOperation(record.paymentOperation)
  } catch {
    return undefined
  }
  return record as PaymentReleaseRecord
}

function buildPaymentReleaseRecord(
  authz: AuthorizedRequest,
  reason: string,
): PaymentReleaseRecord | undefined {
  const operation = authz.paymentOperation
  if (!operation) return undefined
  return {
    version: 1,
    lease: { id: cryptoRandomId(), expiresAt: Date.now() + PAYMENT_RELEASE_LEASE_MS },
    agentSlug: authz.agent.slug,
    requestId: authz.requestId,
    operationId: operation.operationId,
    paymentOperation: serializePaymentOperation({ ...operation, state: 'releasing' }),
    reason,
  }
}

async function beginPaymentReleaseRecovery(
  taskStore: TaskStore,
  task: Task,
  authz: AuthorizedRequest,
  reason: string,
): Promise<Task | undefined> {
  const record = buildPaymentReleaseRecord(authz, reason)
  if (!record) return undefined
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = await taskStore.get(task.id) ?? task
    const existing = readPaymentReleaseRecord(current)
    if (existing) {
      if (existing.operationId !== record.operationId) {
        throw new Error('A2A task already has a different payment release recovery')
      }
      return current
    }
    const next = withPaymentReleaseRecord(current, record)
    if (await compareAndSetTask(taskStore, current, next)) return next
  }
  throw new Error('A2A task changed before payment release recovery was stored')
}

async function retainPaymentReleaseForRecovery(
  taskStore: TaskStore,
  taskId: string,
  leaseId: string,
  error: Error,
): Promise<Task | undefined> {
  const current = await taskStore.get(taskId)
  if (!current) return undefined
  const record = readPaymentReleaseRecord(current)
  if (!record || record.lease.id !== leaseId) return undefined
  const retry: PaymentReleaseRecord = {
    ...record,
    lease: { id: cryptoRandomId(), expiresAt: Date.now() + PAYMENT_RELEASE_LEASE_MS },
    recoveryAttempts: (record.recoveryAttempts ?? 0) + 1,
    recoveryError: error.message,
  }
  const next = withPaymentReleaseRecord(current, retry)
  if (await compareAndSetTask(taskStore, current, next)) return next
  return await taskStore.get(taskId)
}

async function clearPaymentReleaseRecovery(
  taskStore: TaskStore,
  task: Task,
  leaseId: string,
): Promise<Task> {
  const current = await taskStore.get(task.id) ?? task
  const record = readPaymentReleaseRecord(current)
  if (!record || record.lease.id !== leaseId) return current
  const cleared = clearPaymentRecoveryMarker(clearPaymentReleaseRecord(current))
  if (await compareAndSetTask(taskStore, current, cleared)) return cleared
  return await taskStore.get(task.id) ?? cleared
}

function readFinalizationRecord(task: Task): FinalizationRecord | undefined {
  const raw = task.metadata?.[FINALIZING_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const record = raw as Partial<FinalizationRecord>
  if (
    record.version !== 1 ||
    !record.lease ||
    typeof record.lease.id !== 'string' ||
    typeof record.lease.expiresAt !== 'number'
  ) {
    return undefined
  }
  return record as FinalizationRecord
}

function isTaskFinalizing(task: Task): boolean {
  const marker = task.metadata?.[FINALIZING_METADATA_KEY]
  return marker === true || (typeof marker === 'object' && marker !== null)
}

async function recoverPaymentReleaseIfNeeded(
  task: Task,
  deps: A2AHandlerDeps,
): Promise<Task> {
  const raw = task.metadata?.[PAYMENT_RELEASE_METADATA_KEY]
  if (raw === undefined) return task
  const record = readPaymentReleaseRecord(task)
  if (!record) {
    return expirePaymentRelease(
      task,
      deps,
      new Error('A2A payment release recovery record is missing'),
    )
  }
  if (record.lease.expiresAt > Date.now()) return task

  const renewed: PaymentReleaseRecord = {
    ...record,
    lease: { id: cryptoRandomId(), expiresAt: Date.now() + PAYMENT_RELEASE_LEASE_MS },
  }
  const leasedTask = withPaymentReleaseRecord(task, renewed)
  if (!await compareAndSetTask(deps.taskStore, task, leasedTask)) {
    return await deps.taskStore.get(task.id) ?? task
  }

  try {
    if (!deps.config.x402.paymentOperations) {
      throw new Error('A2A payment release recovery is not configured')
    }
    const operation = deserializePaymentOperation(renewed.paymentOperation)
    await deps.config.x402.paymentOperations.releasePayment(operation, renewed.reason)
    if (deps.config.paymentRecovery) {
      const recovered = await recoverDurablePayment(renewed.operationId, deps.config, { force: true })
      if (recovered && recovered.state !== 'reconciled') {
        throw new Error('durable payment release is still pending')
      }
    }
    const recovered = clearPaymentReleaseRecord(leasedTask)
    if (!await compareAndSetTask(deps.taskStore, leasedTask, recovered)) {
      return await deps.taskStore.get(task.id) ?? recovered
    }
    return recovered
  } catch (error) {
    const recoveryError = error instanceof Error ? error : new Error(String(error))
    const retained = await retainPaymentReleaseForRecovery(
      deps.taskStore,
      task.id,
      renewed.lease.id,
      recoveryError,
    )
    console.error(
      '[a2a] payment release recovery failed for ' + task.id + ':',
      recoveryError.message,
    )
    return retained ?? leasedTask
  }
}

async function recoverFinalizationIfNeeded(
  task: Task,
  deps: A2AHandlerDeps,
  requestedAgentSlug: string,
): Promise<Task> {
  if (!isTaskFinalizing(task)) return task
  const record = readFinalizationRecord(task)
  if (!record) {
    return expireFinalization(
      task,
      deps,
      null,
      new Error('A2A finalization record is missing'),
    )
  }
  if (record.lease.expiresAt > Date.now()) return task

  const renewed: FinalizationRecord = {
    ...record,
    lease: { id: cryptoRandomId(), expiresAt: Date.now() + FINALIZATION_LEASE_MS },
  }
  const leasedTask = withFinalizationRecord(task, renewed)
  if (!await compareAndSetTask(deps.taskStore, task, leasedTask)) {
    return await deps.taskStore.get(task.id) ?? task
  }

  try {
    const agentSlug = renewed.agentSlug || requestedAgentSlug
    const agent = await deps.config.resolveAgent(agentSlug)
    if (!agent || !agent.enabled) throw new Error('A2A recovery agent is unavailable')

    const paymentRecovery = readPaymentRecoveryMarker(leasedTask)
    if (paymentRecovery && deps.config.paymentRecovery) {
      const recovery = await recoverDurablePayment(paymentRecovery.id, deps.config, {
        force: true,
        usage: renewed.receipt,
      })
      if (recovery?.state !== 'reconciled') {
        throw new Error('durable payment finalization is still pending')
      }
      const usageRecordedTask = await markUsageRecorded(deps.taskStore, leasedTask)
      const recoveredTask = finalizationResultTask(usageRecordedTask, renewed)
      if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, recoveredTask)) {
        return await deps.taskStore.get(task.id) ?? recoveredTask
      }
      await maybeDeliverPush(recoveredTask, deps)
      return recoveredTask
    }

    let paymentOperation: PaymentOperation | undefined
    if (renewed.operationId || renewed.paymentOperation) {
      if (!renewed.operationId || !renewed.paymentOperation) {
        throw new Error('A2A payment operation recovery record is incomplete')
      }
      if (renewed.operationId !== renewed.paymentOperation.operationId) {
        throw new Error('A2A payment operation recovery id does not match')
      }
      if (!deps.config.x402.paymentOperations) {
        throw new Error('A2A payment operation recovery is not configured')
      }
      paymentOperation = deserializePaymentOperation(renewed.paymentOperation)
    }

    let paymentAlreadySettled = false
    if (paymentOperation && deps.config.x402.paymentOperations) {
      const currentOperation = await deps.config.x402.paymentOperations.getPaymentOperation(
        paymentOperation.operationId,
      )
      if (currentOperation.state === 'not-found') {
        throw new Error('A2A payment operation disappeared during finalization recovery')
      }
      if (currentOperation.operationId !== paymentOperation.operationId) {
        throw new Error('A2A payment operation recovery returned a different operation')
      }
      paymentOperation = currentOperation
      paymentAlreadySettled = currentOperation.state === 'settled'
    }

    const authz: AuthorizedRequest = {
      agent,
      consumerId: renewed.consumerId,
      paymentMethod: renewed.paymentMethod,
      keyInfo: null,
      userMessage: '[recovered A2A task]',
      rateLimitRemaining: undefined,
      requestId: renewed.requestId,
      startMs: renewed.startMs,
      maxOutputTokens: renewed.maxOutputTokens,
      executionBudget: renewed.executionBudget,
      requiredPaymentAmount: 0n,
      paymentPayload: null,
      ...(readPaymentRecoveryMarker(leasedTask)
        ? { paymentRecoveryId: readPaymentRecoveryMarker(leasedTask)!.id }
        : {}),
      ...(paymentOperation
        ? { paymentOperation, paymentOperationAcquired: true }
        : {}),
    }
    let usageRecordedTask = leasedTask
    await settleAndRecord(
      agent,
      authz,
      renewed.receipt,
      deps.config,
      deps.state.obs,
      {
        usageAlreadyRecorded: renewed.usageRecorded === true,
        paymentAlreadySettled,
        onUsageRecorded: async () => {
          usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
        },
      },
    )

    usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
    const recovered = finalizationResultTask(usageRecordedTask, renewed)
    if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, recovered)) {
      return await deps.taskStore.get(task.id) ?? recovered
    }
    await maybeDeliverPush(recovered, deps)
    return recovered
  } catch (error) {
    const recoveryError = error instanceof Error ? error : new Error(String(error))
    console.error(
      `[a2a] finalization recovery failed for ${task.id}:`,
      recoveryError.message,
    )
    if (
      (readPaymentRecoveryMarker(leasedTask) && deps.config.paymentRecovery) ||
      (renewed.operationId && renewed.paymentOperation)
    ) {
      const retained = await retainFinalizationForRecovery(
        deps.taskStore,
        task.id,
        renewed.lease.id,
        recoveryError,
      )
      if (retained) return retained
    }
    return expireFinalization(leasedTask, deps, renewed, recoveryError)
  }
}

async function recoverTaskIfNeeded(
  task: Task,
  deps: A2AHandlerDeps,
  requestedAgentSlug: string,
): Promise<Task> {
  const released = await recoverPaymentReleaseIfNeeded(task, deps)
  const finalized = await recoverFinalizationIfNeeded(released, deps, requestedAgentSlug)
  const paymentRecovered = await recoverPaymentMarkerIfNeeded(finalized, deps)
  const submissionRecovered = await recoverSubmissionIfNeeded(paymentRecovered, deps)
  return recoverExpiredExecutionIfNeeded(submissionRecovered, deps)
}

async function recoverExpiredExecutionIfNeeded(task: Task, deps: A2AHandlerDeps): Promise<Task> {
  const malformed = hasMalformedTaskExecution(task)
  if (
    task.status.state !== 'working' ||
    (isTaskFinalizing(task) && !malformed) ||
    (!malformed && !hasExpiredTaskExecution(task))
  ) return task
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

async function recoverSubmissionIfNeeded(task: Task, deps: A2AHandlerDeps): Promise<Task> {
  const raw = task.metadata?.[TASK_SUBMISSION_METADATA_KEY]
  if (raw === undefined) return task
  const submission = readTaskSubmission(task)
  if (submission && submission.lease.expiresAt > Date.now()) return task
  if (task.status.state !== 'submitted') {
    return (await clearTaskSubmissionMarker(deps.taskStore, task)).task
  }

  const cleanTask = clearTaskSubmission(task)
  const failed: Task = {
    ...withStatus(cleanTask, 'failed'),
    metadata: {
      ...(cleanTask.metadata ?? {}),
      [TASK_SUBMISSION_RECOVERY_METADATA_KEY]: {
        error: submission
          ? 'A2A task submission lease expired before payment authorization completed'
          : 'A2A task submission lease is invalid',
      },
    },
  }
  if (await compareAndSetTask(deps.taskStore, task, failed)) {
    await maybeDeliverPush(failed, deps)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? task
}

async function recoverPaymentMarkerIfNeeded(
  task: Task,
  deps: A2AHandlerDeps,
): Promise<Task> {
  const marker = readPaymentRecoveryMarker(task)
  if (!marker || !deps.config.paymentRecovery) return task
  try {
    const record = await recoverDurablePayment(marker.id, deps.config)
    if (record?.state !== 'reconciled') return task
    return clearReconciledPaymentRecoveryMarker(task, deps)
  } catch (error) {
    console.error(
      `[a2a] durable payment recovery failed for ${task.id}:`,
      error instanceof Error ? error.message : String(error),
    )
    return task
  }
}

async function clearReconciledPaymentRecoveryMarker(
  task: Task,
  deps: A2AHandlerDeps,
): Promise<Task> {
  const marker = readPaymentRecoveryMarker(task)
  if (!marker || !deps.config.paymentRecovery) return task
  const record = await deps.config.paymentRecovery.store.get(marker.id)
  if (record?.state !== 'reconciled') return task
  const cleared = clearPaymentRecoveryMarker(task)
  if (cleared.status.state === 'working' || cleared.status.state === 'submitted') {
    const failed: Task = {
      ...withStatus(cleared, 'failed'),
      metadata: {
        ...(cleared.metadata ?? {}),
        [EXECUTION_RECOVERY_METADATA_KEY]: {
          error: 'payment recovery completed without a task result',
        },
      },
    }
    if (await compareAndSetTask(deps.taskStore, task, failed)) return failed
    return await deps.taskStore.get(task.id) ?? failed
  }
  if (await compareAndSetTask(deps.taskStore, task, cleared)) return cleared
  return await deps.taskStore.get(task.id) ?? cleared
}

function finalizationResultTask(task: Task, record: FinalizationRecord): Task {
  const cleanTask = clearPaymentRecoveryMarker(clearFinalizationMarker(task))
  const finalState = record.finalState ?? (
    task.status.state === 'canceled'
      ? 'canceled'
      : record.inputRequired
        ? 'input-required'
        : 'completed'
  )
  if (finalState === 'canceled') {
    return withStatus(
      cleanTask,
      'canceled',
      undefined,
      record.artifact ? [record.artifact] : cleanTask.artifacts,
    )
  }
  if (finalState === 'input-required') {
    return withStatus(
      cleanTask,
      'input-required',
      record.inputRequiredPrompt ? agentMessage(cleanTask, record.inputRequiredPrompt) : undefined,
      record.artifact ? [record.artifact] : cleanTask.artifacts,
    )
  }
  return withStatus(
    cleanTask,
    'completed',
    undefined,
    record.artifact ? [record.artifact] : cleanTask.artifacts,
  )
}

async function expireFinalization(
  task: Task,
  deps: A2AHandlerDeps,
  record: FinalizationRecord | null,
  error: Error,
): Promise<Task> {
  const cleanTask = clearFinalizationMarker(task)
  const failed: Task = {
    ...withStatus(cleanTask, 'failed'),
    metadata: {
      ...(cleanTask.metadata ?? {}),
      gatewayFinalizationRecovery: {
        operationId: record?.operationId ?? null,
        error: error.message,
      },
    },
  }
  if (await compareAndSetTask(deps.taskStore, task, failed)) {
    await maybeDeliverPush(failed, deps)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? failed
}

async function expirePaymentRelease(
  task: Task,
  deps: A2AHandlerDeps,
  error: Error,
): Promise<Task> {
  const cleanTask = clearPaymentReleaseRecord(task)
  const failed: Task = {
    ...withStatus(cleanTask, 'failed'),
    metadata: {
      ...(cleanTask.metadata ?? {}),
      gatewayPaymentReleaseRecovery: { error: error.message },
    },
  }
  if (await compareAndSetTask(deps.taskStore, task, failed)) {
    await maybeDeliverPush(failed, deps)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? failed
}

function clearFinalizationMarker(task: Task): Task {
  if (!task.metadata || !(FINALIZING_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[FINALIZING_METADATA_KEY]
  return Object.keys(metadata).length > 0
    ? { ...task, metadata }
    : (() => {
        const { metadata: _metadata, ...withoutMetadata } = task
        return withoutMetadata
      })()
}

function isTerminal(state: Task['status']['state']): boolean {
  return TERMINAL_STATES.has(state)
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
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
  const next: Task = {
    ...task,
    status: { state, timestamp: nowIso(), ...(message ? { message } : {}) },
    ...(artifacts !== undefined ? { artifacts } : {}),
  }
  return isTerminal(state) || state === 'input-required' ? clearTaskExecution(next) : next
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
    messageId: `${task.id}-input-required-${stableMessageDigest(text)}`,
    taskId: task.id,
    contextId: task.contextId,
  }
}

function stableMessageDigest(value: string): string {
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0).toString(16).padStart(8, '0')
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
  if (!deps.pushStore || !TERMINAL_STATES.has(task.status.state)) return
  const webhookSecret = deps.config.a2a?.webhookSecret
  const hasWebhookSecret = typeof webhookSecret === 'string' && webhookSecret.trim().length > 0
  if (!deps.config.x402.demoMode && !hasWebhookSecret) {
    console.error(`[agent-gateway] production A2A push requires a webhookSecret for task ${task.id}`)
    return
  }
  try {
    const deliveryTask = clearPushDeliveryClaims(task)
    const deliveryArgs: Omit<PushNotificationDeliveryOptions, 'webhookSecret'> = {
      task: deliveryTask,
      store: deps.pushStore,
      fetcher: deps.config.a2a?.pushFetcher,
      urlValidator: deps.config.a2a?.pushUrlValidator,
      requireUrlValidator: !deps.config.x402.demoMode,
      claimDelivery: (taskId, configId, terminalState) => claimTaskPushDelivery(
        deps.taskStore,
        taskId,
        configId,
        terminalState,
      ),
      onDelivery: (result: PushDeliveryResult) => {
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
    }
    if (hasWebhookSecret) {
      await deliverPushNotifications({ ...deliveryArgs, webhookSecret })
    } else {
      await deliverDemoPushNotifications(deliveryArgs)
    }
  } catch (err) {
    // Catastrophic failure of the push pipeline itself (e.g. the store threw).
    // Logged but never escalated — a busted webhook MUST NOT fail the agent.
    console.error(
      `[agent-gateway] push delivery threw for task ${task.id}: ${err instanceof Error ? err.message : String(err)}`,
    )
  }
}

async function claimTaskPushDelivery(
  taskStore: TaskStore,
  taskId: string,
  configId: string,
  terminalState: Task['status']['state'],
): Promise<boolean> {
  if (!TERMINAL_STATES.has(terminalState)) return false
  for (let attempt = 0; attempt < 16; attempt += 1) {
    const current = await taskStore.get(taskId)
    if (!current || current.status.state !== terminalState) return false
    const existing = readPushDeliveryClaims(current)
    if (existing?.claims[configId] === terminalState) return false
    const next: Task = {
      ...current,
      metadata: {
        ...(current.metadata ?? {}),
        [PUSH_DELIVERY_METADATA_KEY]: {
          version: 1,
          claims: {
            ...(existing?.claims ?? {}),
            [configId]: terminalState,
          },
        } satisfies TaskPushDeliveryClaims,
      },
    }
    if (await compareAndSetTask(taskStore, current, next)) return true
  }
  throw new Error(`A2A push delivery claim changed too many times for task '${taskId}'`)
}

function readPushDeliveryClaims(task: Task): TaskPushDeliveryClaims | undefined {
  const raw = task.metadata?.[PUSH_DELIVERY_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const record = raw as Partial<TaskPushDeliveryClaims>
  if (!record.claims || typeof record.claims !== 'object') return undefined
  const claims = Object.fromEntries(
    Object.entries(record.claims).filter(([, state]) =>
      typeof state === 'string' && TERMINAL_STATES.has(state as Task['status']['state']),
    ),
  ) as Record<string, Task['status']['state']>
  return record.version === 1 ? { version: 1, claims } : undefined
}

function clearPushDeliveryClaims(task: Task): Task {
  if (!task.metadata || !(PUSH_DELIVERY_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[PUSH_DELIVERY_METADATA_KEY]
  return Object.keys(metadata).length > 0
    ? { ...task, metadata }
    : (() => {
        const { metadata: _metadata, ...withoutMetadata } = task
        return withoutMetadata
      })()
}
