import type { Context } from 'hono'
import {
  type AuthorizedRequest,
  beginPaymentExecution,
  buildGatewaySandboxContext,
  dispatchSandboxStreamRich,
  markPaymentExecutionStarted,
  renewPaymentExecution,
} from '../dispatch'
import type { GatewayConfig, SandboxUsageReceipt } from '../types'
import { claimTaskExecution, renewTaskExecution } from './execution-fence'
import { fail, ok } from './jsonrpc'
import {
  clearPaymentRecoveryMarker,
  releaseTaskPayment,
} from './payment-recovery'
import {
  buildFinalizationRecord,
  clearFinalizationMarker,
  completeCanceledTask,
  markUsageRecorded,
  retainFinalizationForRecovery,
  withFinalizationRecord,
} from './task-finalization'
import { clearTaskSubmission } from './task-submission-recovery'
import { bindRequestAbort, type TaskCancellationRegistry } from './task-cancellation'
import type { TaskLifecycle } from './task-lifecycle'
import {
  agentMessage,
  asError,
  compareAndSetTask,
  isTerminal,
  nowIso,
  persistTaskIfCurrent,
  shouldPreserveTask,
  withStatus,
} from './task-state'
import type { TaskStateStore } from './task-state'
import {
  A2A_ERROR_CODES,
  type JSONRPCRequest,
  type StreamingEvent,
  type Task,
  type TaskArtifactUpdateEvent,
  type TaskStatusUpdateEvent,
} from './types'
import { responseTextToArtifact } from './translate'

export interface MessageStreamExecutionDependencies {
  taskStore: TaskStateStore
  config: GatewayConfig
  lifecycle: TaskLifecycle
  cancels: TaskCancellationRegistry
  deliverPush: (task: Task) => Promise<void>
  reportStreamError: (authz: AuthorizedRequest, error: unknown) => Promise<void>
}

export async function executeMessageStream(
  c: Context,
  req: JSONRPCRequest,
  deps: MessageStreamExecutionDependencies,
  authz: AuthorizedRequest,
  task: Task,
): Promise<Response> {
  const controller = deps.cancels.register(task.id)
  const lifecycle = deps.lifecycle
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
    deps.cancels.clear(task.id)
    return c.json(ok(req.id, task))
  }
  let workingTask: Task = task.status.state === 'working'
    ? task
    : { ...task, status: workingStatus.status }
  if (task.status.state !== 'working' && !await compareAndSetTask(deps.taskStore, task, workingTask)) {
    detachRequestAbort()
    deps.cancels.clear(task.id)
    const current = await releaseTaskPayment(
      authz,
      await deps.taskStore.get(task.id) ?? task,
      lifecycle.payment,
      'A2A task changed before execution started',
      false,
    )
    if (current.status.state === 'canceled') return c.json(ok(req.id, current))
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, `task '${task.id}' changed before execution`))
  }
  let responseText = ''
  let usage: SandboxUsageReceipt | undefined
  let workObserved = false
  const sandboxContext = deps.config.conversationMode === 'thread'
    ? buildGatewaySandboxContext(authz, authz.threadId)
    : undefined

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
            sandboxContext,
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

          if (controller.signal.aborted) {
            const canceled = await completeCanceledTask(
              authz,
              workingTask,
              responseText,
              usage,
              workObserved,
              lifecycle.finalization,
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

          if (!usage) throw new Error('sandbox did not provide a usage receipt')
          const finalizationArtifact = responseTextToArtifact(responseText, `${task.id}-artifact-0`)
          const finalization = buildFinalizationRecord(
            authz,
            usage,
            finalizationArtifact,
            inputRequiredSeen,
            inputRequiredPrompt,
          )
          // Let the durable task-store CAS decide the cancellation race.
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
                lifecycle.finalization,
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
              lifecycle.payment,
              'A2A task changed before payment settlement',
              workObserved || usage !== undefined,
            )
            return
          }
          finalizationLeaseId = finalization.lease.id
          deps.cancels.beginFinalization(task.id)
          let usageRecordedTask = finalizingTask
          await lifecycle.finalization.settle(authz, usage, {
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
            return
          }

          const completed = withStatus(
            clearPaymentRecoveryMarker(clearFinalizationMarker(usageRecordedTask)),
            'completed',
            undefined,
            [responseTextToArtifact(responseText, `${task.id}-artifact-0`)],
          )
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
          await lifecycle.finalization.deliverPush(completed)
        } catch (err) {
          const releasedTask = await releaseTaskPayment(
            authz,
            task,
            lifecycle.payment,
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
            await deps.deliverPush(persisted)
          } catch (taskError) {
            console.error(
              `[a2a] failed to persist failed task ${task.id}:`,
              taskError instanceof Error ? taskError.message : String(taskError),
            )
          }
          try {
            await deps.reportStreamError(authz, err)
          } catch (observerError) {
            console.error(
              `[a2a] stream observer failed for ${authz.requestId}:`,
              observerError instanceof Error ? observerError.message : String(observerError),
            )
          }
        } finally {
          detachRequestAbort()
          deps.cancels.clear(task.id)
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
