import type {
  AuthorizedRequest,
  SettleAndRecordOptions,
} from '../dispatch'
import type { PaymentOperation, PaymentOperations } from '../payment-operations'
import {
  deserializePaymentOperation,
  serializePaymentOperation,
  type PaymentRecoveryConfig,
  type PaymentRecoveryRecord,
  type SerializedPaymentOperation,
} from '../payment-recovery'
import type { AgentMeta, PaymentMethod, SandboxExecutionBudget, SandboxUsageReceipt } from '../types'
import {
  clearPaymentRecoveryMarker,
  readPaymentRecoveryMarker,
} from './payment-recovery'
import { clearTaskSubmission } from './task-submission-recovery'
import type { Task } from './types'
import {
  agentMessage,
  asError,
  compareAndSetTask,
  clearTaskMetadata,
  cryptoRandomId,
  isTerminal,
  persistTaskIfCurrent,
  type TaskStateStore,
  withStatus,
} from './task-state'
import { responseTextToArtifact } from './translate'
import type { Artifact } from './types'

const FINALIZING_METADATA_KEY = 'gatewayFinalizing'
const FINALIZATION_LEASE_MS = 5 * 60 * 1000

export type FinalizationState = 'completed' | 'input-required' | 'canceled'

export interface FinalizationRecord {
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

export interface TaskFinalizationDependencies {
  taskStore: TaskStateStore
  settle: (
    authz: AuthorizedRequest,
    usage: SandboxUsageReceipt,
    options?: SettleAndRecordOptions,
  ) => Promise<void>
  resolveAgent: (slug: string) => Promise<AgentMeta | null | undefined>
  paymentOperations?: PaymentOperations
  paymentRecovery?: PaymentRecoveryConfig
  recoverDurablePayment: (
    recoveryId: string,
    options?: { force?: boolean; usage?: SandboxUsageReceipt },
  ) => Promise<PaymentRecoveryRecord | undefined>
  releasePaymentAfterFailure: (
    authz: AuthorizedRequest,
    reason: string,
    workObserved: boolean,
  ) => Promise<void>
  releaseTaskPayment: (
    authz: AuthorizedRequest,
    task: Task,
    reason: string,
    workObserved: boolean,
  ) => Promise<Task>
  deliverPush: (task: Task) => Promise<void>
}

export function buildFinalizationRecord(
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

export function withFinalizationRecord(task: Task, record: FinalizationRecord): Task {
  return {
    ...task,
    metadata: { ...(task.metadata ?? {}), [FINALIZING_METADATA_KEY]: record },
  }
}

export function readFinalizationRecord(task: Task): FinalizationRecord | undefined {
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

export function isTaskFinalizing(task: Task): boolean {
  const marker = task.metadata?.[FINALIZING_METADATA_KEY]
  return marker === true || (typeof marker === 'object' && marker !== null)
}

export function clearFinalizationMarker(task: Task): Task {
  return clearTaskMetadata(task, FINALIZING_METADATA_KEY)
}

function markUsageRecordedRecord(task: Task): Task {
  const record = readFinalizationRecord(task)
  if (!record || record.usageRecorded) return task
  return withFinalizationRecord(task, { ...record, usageRecorded: true })
}

export async function markUsageRecorded(
  taskStore: TaskStateStore,
  task: Task,
): Promise<Task> {
  const marked = markUsageRecordedRecord(task)
  if (marked === task) return task
  if (await compareAndSetTask(taskStore, task, marked)) return marked
  return await taskStore.get(task.id) ?? marked
}

export async function retainFinalizationForRecovery(
  taskStore: TaskStateStore,
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

export async function completeCanceledTask(
  authz: AuthorizedRequest,
  task: Task,
  responseText: string,
  usage: SandboxUsageReceipt | undefined,
  workObserved: boolean,
  deps: TaskFinalizationDependencies,
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
      await deps.settle(authz, usage, {
        onUsageRecorded: async () => {
          usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
        },
      })
      usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
    } catch (settlementError) {
      await deps.releasePaymentAfterFailure(
        authz,
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
      await deps.deliverPush(recoveryTask)
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
    await deps.deliverPush(canceled)
    return canceled
  }
  await deps.releaseTaskPayment(authz, task, 'a2a task canceled', workObserved)
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
  await deps.deliverPush(persisted)
  return persisted
}

export async function recoverFinalizationIfNeeded(
  task: Task,
  deps: TaskFinalizationDependencies,
  requestedAgentSlug: string,
  options: { force?: boolean } = {},
): Promise<Task> {
  if (!isTaskFinalizing(task)) return task
  const record = readFinalizationRecord(task)
  if (!record) {
    return expireFinalization(task, deps, null, new Error('A2A finalization record is missing'))
  }
  if (!options.force && record.lease.expiresAt > Date.now()) return task

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
    const agent = await deps.resolveAgent(agentSlug)
    if (!agent || !agent.enabled) throw new Error('A2A recovery agent is unavailable')

    const paymentRecovery = readPaymentRecoveryMarker(leasedTask)
    if (paymentRecovery && deps.paymentRecovery) {
      const recovery = await deps.recoverDurablePayment(paymentRecovery.id, {
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
      await deps.deliverPush(recoveredTask)
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
      if (!deps.paymentOperations) {
        throw new Error('A2A payment operation recovery is not configured')
      }
      paymentOperation = deserializePaymentOperation(renewed.paymentOperation)
    }

    let paymentAlreadySettled = false
    if (paymentOperation && deps.paymentOperations) {
      const currentOperation = await deps.paymentOperations.getPaymentOperation(paymentOperation.operationId)
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
      ...(paymentRecovery ? { paymentRecoveryId: paymentRecovery.id } : {}),
      ...(paymentOperation ? { paymentOperation, paymentOperationAcquired: true } : {}),
    }
    let usageRecordedTask = leasedTask
    await deps.settle(authz, renewed.receipt, {
      usageAlreadyRecorded: renewed.usageRecorded === true,
      paymentAlreadySettled,
      onUsageRecorded: async () => {
        usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
      },
    })
    usageRecordedTask = await markUsageRecorded(deps.taskStore, usageRecordedTask)
    const recovered = finalizationResultTask(usageRecordedTask, renewed)
    if (!await compareAndSetTask(deps.taskStore, usageRecordedTask, recovered)) {
      return await deps.taskStore.get(task.id) ?? recovered
    }
    await deps.deliverPush(recovered)
    return recovered
  } catch (error) {
    const recoveryError = asError(error)
    console.error(`[a2a] finalization recovery failed for ${task.id}:`, recoveryError.message)
    if (
      (readPaymentRecoveryMarker(leasedTask) && deps.paymentRecovery) ||
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
    return withStatus(cleanTask, 'canceled', undefined, record.artifact ? [record.artifact] : cleanTask.artifacts)
  }
  if (finalState === 'input-required') {
    return withStatus(
      cleanTask,
      'input-required',
      record.inputRequiredPrompt ? agentMessage(cleanTask, record.inputRequiredPrompt) : undefined,
      record.artifact ? [record.artifact] : cleanTask.artifacts,
    )
  }
  return withStatus(cleanTask, 'completed', undefined, record.artifact ? [record.artifact] : cleanTask.artifacts)
}

async function expireFinalization(
  task: Task,
  deps: TaskFinalizationDependencies,
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
    await deps.deliverPush(failed)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? failed
}
