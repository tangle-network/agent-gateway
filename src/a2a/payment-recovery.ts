import type { AuthorizedRequest } from '../dispatch'
import type { PaymentOperations, PaymentOperation } from '../payment-operations'
import {
  deserializePaymentOperation,
  serializePaymentOperation,
  type PaymentRecoveryConfig,
  type PaymentRecoveryRecord,
  type SerializedPaymentOperation,
} from '../payment-recovery'
import type { SandboxUsageReceipt } from '../types'
import type { Task } from './types'
import {
  asError,
  compareAndSetTask,
  clearTaskMetadata,
  cryptoRandomId,
  withStatus,
  type TaskStateStore,
} from './task-state'

const PAYMENT_RELEASE_METADATA_KEY = 'gatewayPaymentRelease'
const PAYMENT_RECOVERY_METADATA_KEY = 'gatewayPaymentRecovery'
const PAYMENT_RELEASE_LEASE_MS = 5 * 60 * 1000

interface TaskPaymentRecoveryMarker {
  version: 1
  id: string
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

export interface PaymentRecoveryDependencies {
  taskStore: TaskStateStore
  paymentOperations?: PaymentOperations
  paymentRecovery?: PaymentRecoveryConfig
  releasePayment: (authz: AuthorizedRequest, reason: string) => Promise<void>
  releasePaymentAfterFailure: (
    authz: AuthorizedRequest,
    reason: string,
    workObserved: boolean,
  ) => Promise<void>
  recoverDurablePayment: (
    recoveryId: string,
    options?: { force?: boolean; usage?: SandboxUsageReceipt },
  ) => Promise<PaymentRecoveryRecord | undefined>
  deliverPush: (task: Task) => Promise<void>
}

export async function attachPaymentRecoveryMarker(
  taskStore: TaskStateStore,
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
export async function retainPaymentRecoveryMarker(
  taskStore: TaskStateStore,
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

export function preservePaymentRecoveryMarker(base: Task, source: Task): Task {
  const marker = readPaymentRecoveryMarker(source)
  return marker ? withPaymentRecoveryMarker(base, marker.id) : base
}

export function readPaymentRecoveryMarker(task: Task): TaskPaymentRecoveryMarker | undefined {
  const raw = task.metadata?.[PAYMENT_RECOVERY_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const marker = raw as Partial<TaskPaymentRecoveryMarker>
  if (marker.version !== 1 || typeof marker.id !== 'string' || marker.id.length === 0) {
    return undefined
  }
  return marker as TaskPaymentRecoveryMarker
}

export function clearPaymentRecoveryMarker(task: Task): Task {
  return clearTaskMetadata(task, PAYMENT_RECOVERY_METADATA_KEY)
}

export function hasPaymentReleaseRecovery(task: Task): boolean {
  return task.metadata?.[PAYMENT_RELEASE_METADATA_KEY] !== undefined
}

export async function releaseTaskPayment(
  authz: AuthorizedRequest,
  task: Task,
  deps: PaymentRecoveryDependencies,
  reason: string,
  workObserved: boolean,
): Promise<Task> {
  if (
    !workObserved &&
    !authz.paymentRecoveryId &&
    authz.paymentOperation &&
    deps.paymentOperations
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
      await deps.releasePayment(authz, reason)
    } catch (releaseError) {
      const retained = await retainPaymentReleaseForRecovery(
        deps.taskStore,
        task.id,
        record.lease.id,
        asError(releaseError),
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
    await deps.releasePaymentAfterFailure(authz, reason, workObserved)
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

async function beginPaymentReleaseRecovery(
  taskStore: TaskStateStore,
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
  taskStore: TaskStateStore,
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
  taskStore: TaskStateStore,
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

export async function recoverPaymentReleaseIfNeeded(
  task: Task,
  deps: PaymentRecoveryDependencies,
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
    if (!deps.paymentOperations) {
      throw new Error('A2A payment release recovery is not configured')
    }
    const operation = deserializePaymentOperation(renewed.paymentOperation)
    await deps.paymentOperations.releasePayment(operation, renewed.reason)
    if (deps.paymentRecovery) {
      const recovered = await deps.recoverDurablePayment(renewed.operationId, { force: true })
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
    const recoveryError = asError(error)
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

export async function recoverPaymentMarkerIfNeeded(
  task: Task,
  deps: PaymentRecoveryDependencies,
): Promise<Task> {
  const marker = readPaymentRecoveryMarker(task)
  if (!marker || !deps.paymentRecovery) return task
  try {
    const record = await deps.recoverDurablePayment(marker.id)
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
  deps: PaymentRecoveryDependencies,
): Promise<Task> {
  const marker = readPaymentRecoveryMarker(task)
  if (!marker || !deps.paymentRecovery) return task
  const record = await deps.paymentRecovery.store.get(marker.id)
  if (record?.state !== 'reconciled') return task
  const cleared = clearPaymentRecoveryMarker(task)
  if (cleared.status.state === 'working' || cleared.status.state === 'submitted') {
    const failed: Task = {
      ...withStatus(cleared, 'failed'),
      metadata: {
        ...(cleared.metadata ?? {}),
        gatewayExecutionRecovery: {
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

function withPaymentRecoveryMarker(task: Task, recoveryId: string): Task {
  return {
    ...task,
    metadata: {
      ...(task.metadata ?? {}),
      [PAYMENT_RECOVERY_METADATA_KEY]: { version: 1, id: recoveryId },
    },
  }
}

function withPaymentReleaseRecord(task: Task, record: PaymentReleaseRecord): Task {
  return {
    ...task,
    metadata: { ...(task.metadata ?? {}), [PAYMENT_RELEASE_METADATA_KEY]: record },
  }
}

function clearPaymentReleaseRecord(task: Task): Task {
  return clearTaskMetadata(task, PAYMENT_RELEASE_METADATA_KEY)
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

async function expirePaymentRelease(
  task: Task,
  deps: PaymentRecoveryDependencies,
  error: Error,
): Promise<Task> {
  const cleanTask = clearPaymentReleaseRecord(task)
  const terminalTask = withStatus(cleanTask, 'failed')
  const failed: Task = {
    ...terminalTask,
    metadata: {
      ...(terminalTask.metadata ?? {}),
      gatewayPaymentReleaseRecovery: { error: error.message },
    },
  }
  if (await compareAndSetTask(deps.taskStore, task, failed)) {
    await deps.deliverPush(failed)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? failed
}
