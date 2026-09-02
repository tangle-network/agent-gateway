import type { Context } from 'hono'
import {
  type AuthorizedRequest,
  dispatchSandboxStreamRich,
  buildGatewaySandboxContext,
  beginPaymentExecution,
  markPaymentExecutionStarted,
  renewPaymentExecution,
} from '../dispatch'
import {
  attachTaskExecutionReference,
  claimTaskExecution,
  renewTaskExecution,
} from './execution-fence'
import type { GatewayConfig, SandboxUsageReceipt } from '../types'
import type { TaskStore } from './task-store'
import { A2A_ERROR_CODES, type JSONRPCRequest, type Task } from './types'
import { clearTaskSubmission } from './task-submission-recovery'
import {
  buildFinalizationRecord,
  clearFinalizationMarker,
  completeCanceledTask,
  markUsageRecorded,
  retainFinalizationForRecovery,
  withFinalizationRecord,
} from './task-finalization'
import { clearPaymentRecoveryMarker, releaseTaskPayment } from './payment-recovery'
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
import { fail, ok } from './jsonrpc'
import { responseTextToArtifact } from './translate'
import type { TaskFinalizationDependencies } from './task-finalization'
import type { PaymentRecoveryDependencies } from './payment-recovery'

export interface MessageExecutionDependencies {
  taskStore: TaskStore
  config: GatewayConfig
  payment: PaymentRecoveryDependencies
  finalization: TaskFinalizationDependencies
}

export async function executeMessageSend(
  c: Context,
  req: JSONRPCRequest,
  deps: MessageExecutionDependencies,
  authz: AuthorizedRequest,
  task: Task,
  signal: AbortSignal,
): Promise<Response> {
  if (isTerminal(task.status.state)) return c.json(ok(req.id, task))
  let workingTask: Task = task.status.state === 'working'
    ? task
    : { ...task, status: { state: 'working', timestamp: nowIso() } }
  if (
    JSON.stringify(task) !== JSON.stringify(workingTask) &&
    !await compareAndSetTask(deps.taskStore, task, workingTask)
  ) {
    await releaseTaskPayment(authz, task, deps.payment, 'A2A task changed before execution started', false)
    if (signal.aborted) {
      const canceled = await deps.taskStore.get(task.id)
      if (canceled?.status.state === 'canceled') {
        await deps.finalization.deliverPush(canceled)
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
      buildGatewaySandboxContext(authz),
      {
        detached: true,
        turnId: authz.requestId,
        onExecutionAccepted: async (reference) => {
          workingTask = await attachTaskExecutionReference(
            deps.taskStore,
            workingTask,
            authz.requestId,
            reference,
          )
        },
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
      deps.payment,
      err instanceof Error ? err.message : String(err),
      workObserved || usage !== undefined,
    )
    const currentTask = await deps.taskStore.get(task.id) ?? releasedTask
    const failed = shouldPreserveTask(currentTask)
      ? currentTask
      : withStatus(clearTaskSubmission(currentTask), 'failed')
    try {
      const persisted = await persistTaskIfCurrent(deps.taskStore, currentTask, failed)
      await deps.finalization.deliverPush(persisted)
    } catch (taskError) {
      console.error(
        `[a2a] failed to persist failed task ${task.id}:`,
        taskError instanceof Error ? taskError.message : String(taskError),
      )
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, err instanceof Error ? err.message : String(err)))
  }

  if (signal.aborted) {
    const canceled = await completeCanceledTask(
      authz,
      workingTask,
      responseText,
      usage,
      workObserved,
      deps.finalization,
    )
    return c.json(ok(req.id, canceled))
  }

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
          deps.finalization,
        )
        return c.json(ok(req.id, canceled))
      }
      throw new Error('A2A task changed before payment settlement')
    }
    finalizationLeaseId = finalization.lease.id
    let usageRecordedTask = finalizingTask
    await deps.finalization.settle(authz, usage, {
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
          responseText ? [responseTextToArtifact(responseText, `${task.id}-artifact-0`)] : task.artifacts,
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
    await deps.finalization.deliverPush(result)
    return c.json(ok(req.id, result))
  } catch (err) {
    const releasedTask = await releaseTaskPayment(
      authz,
      workingTask,
      deps.payment,
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
      await deps.finalization.deliverPush(persisted)
    } catch (taskError) {
      console.error(
        `[a2a] failed to persist failed task ${task.id}:`,
        taskError instanceof Error ? taskError.message : String(taskError),
      )
    }
    return c.json(fail(req.id, A2A_ERROR_CODES.INTERNAL_ERROR, 'Payment settlement failed'))
  }
}
