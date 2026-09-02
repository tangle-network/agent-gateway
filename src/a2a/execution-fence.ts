import type { Task } from './types'
import type { DetachedExecutionIdentity } from './detached-sandbox'

interface ExecutionTaskStore {
  get(id: string): Promise<Task | undefined>
  compareAndSet?(expected: Task, next: Task): Promise<boolean>
  compareAndSetExecution?(
    expected: Task,
    next: Task,
    requestId: string,
    now: number,
  ): Promise<boolean>
}

/** Durable marker that prevents cancellation from racing sandbox start. */
export const TASK_EXECUTION_METADATA_KEY = 'gatewayExecution'

const TASK_EXECUTION_VERSION = 1 as const
const TASK_EXECUTION_LEASE_MS = 5 * 60 * 1000

export interface TaskExecutionMarker {
  version: typeof TASK_EXECUTION_VERSION
  requestId: string
  lease: { id: string; expiresAt: number }
  /** Provider identity persisted before dispatch to close the admission gap. */
  detached?: DetachedExecutionIdentity
}

export type TaskExecutionInspection =
  | { state: 'absent' }
  | { state: 'valid'; marker: TaskExecutionMarker }
  | { state: 'malformed'; reason: string }

export class TaskExecutionCanceledError extends Error {
  constructor(taskId: string) {
    super(`A2A task '${taskId}' was canceled before sandbox execution`)
    this.name = 'TaskExecutionCanceledError'
  }
}

/** Claim the right to start one task after its sandbox has been acquired. */
export async function claimTaskExecution(
  store: ExecutionTaskStore,
  task: Task,
  requestId: string,
  now = Date.now(),
  detached?: DetachedExecutionIdentity,
): Promise<Task> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = await store.get(task.id)
    if (!current || current.status.state !== 'working') {
      throw new TaskExecutionCanceledError(task.id)
    }
    const inspection = inspectTaskExecution(current)
    if (inspection.state === 'malformed') {
      throw new Error(`A2A task '${task.id}' has a malformed execution marker`)
    }
    const existing = inspection.state === 'valid' ? inspection.marker : undefined
    if (existing && existing.lease.expiresAt > now) {
      if (existing.requestId === requestId) {
        if (detached && !sameDetachedIdentity(existing.detached, detached)) {
          throw new Error(`A2A task '${task.id}' has a different detached execution identity`)
        }
        return current
      }
      throw new Error(`A2A task '${task.id}' is already executing`)
    }
    const next = withTaskExecution(current, requestId, now, detached)
    if (store.compareAndSet && await store.compareAndSet(current, next)) return next
  }
  throw new Error(`A2A task '${task.id}' changed too many times before sandbox execution`)
}

/** Renew both the task execution fence and its cancellation protection. */
export async function renewTaskExecution(
  store: ExecutionTaskStore,
  taskId: string,
  requestId: string,
  now?: number,
): Promise<Task> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = await store.get(taskId)
    const inspection = current ? inspectTaskExecution(current) : { state: 'absent' as const }
    if (inspection.state === 'malformed') {
      throw new Error(`A2A task '${taskId}' has a malformed execution marker`)
    }
    const marker = inspection.state === 'valid' ? inspection.marker : undefined
    if (!current || current.status.state !== 'working' || marker?.requestId !== requestId) {
      throw new TaskExecutionCanceledError(taskId)
    }
    const renewalNow = now ?? Date.now()
    if (marker.lease.expiresAt <= renewalNow) {
      throw new TaskExecutionCanceledError(taskId)
    }
    const next = withTaskExecution(current, requestId, renewalNow)
    if (!store.compareAndSetExecution) {
      throw new Error('A2A task store does not provide atomic execution renewal')
    }
    if (await store.compareAndSetExecution(current, next, requestId, renewalNow)) return next
  }
  throw new Error(`A2A task '${taskId}' changed too many times while execution was active`)
}

/** Remote cancellation is rejected while a live execution fence is held. */
export function hasActiveTaskExecution(task: Task, now = Date.now()): boolean {
  const marker = readTaskExecutionMarker(task)
  return marker !== undefined && marker.lease.expiresAt > now
}

/** A working task with this marker has lost its execution owner. */
export function hasExpiredTaskExecution(task: Task, now = Date.now()): boolean {
  const marker = readTaskExecutionMarker(task)
  return marker !== undefined && !hasActiveTaskExecution(task, now)
}

/** A working task with an execution key that cannot be trusted. */
export function hasMalformedTaskExecution(task: Task): boolean {
  return inspectTaskExecution(task).state === 'malformed'
}

/** Remove the marker when the task reaches a terminal or paused state. */
export function clearTaskExecution(task: Task): Task {
  if (!task.metadata || !(TASK_EXECUTION_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[TASK_EXECUTION_METADATA_KEY]
  return Object.keys(metadata).length > 0
    ? { ...task, metadata }
    : (() => {
        const { metadata: _metadata, ...withoutMetadata } = task
        return withoutMetadata
      })()
}

export function readTaskExecutionMarker(task: Task): TaskExecutionMarker | undefined {
  const inspection = inspectTaskExecution(task)
  return inspection.state === 'valid' ? inspection.marker : undefined
}

export function readDetachedExecutionIdentity(task: Task): DetachedExecutionIdentity | undefined {
  return readTaskExecutionMarker(task)?.detached
}

function withTaskExecution(
  task: Task,
  requestId: string,
  now: number,
  detached?: DetachedExecutionIdentity,
): Task {
  const existing = readTaskExecutionMarker(task)
  return {
    ...task,
    metadata: {
      ...(task.metadata ?? {}),
      [TASK_EXECUTION_METADATA_KEY]: {
        version: TASK_EXECUTION_VERSION,
        requestId,
        lease: { id: requestId, expiresAt: now + TASK_EXECUTION_LEASE_MS },
        ...(detached || existing?.detached
          ? { detached: detached ?? existing?.detached }
          : {}),
      } satisfies TaskExecutionMarker,
    },
  }
}

export function inspectTaskExecution(task: Task): TaskExecutionInspection {
  const raw = task.metadata?.[TASK_EXECUTION_METADATA_KEY]
  if (raw === undefined) return { state: 'absent' }
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { state: 'malformed', reason: 'marker must be an object' }
  }
  const marker = raw as Partial<TaskExecutionMarker>
  if (
    marker.version !== TASK_EXECUTION_VERSION ||
    typeof marker.requestId !== 'string' ||
    marker.requestId.length === 0 ||
    !marker.lease ||
    typeof marker.lease.id !== 'string' ||
    marker.lease.id.length === 0 ||
    typeof marker.lease.expiresAt !== 'number' ||
    !Number.isFinite(marker.lease.expiresAt)
  ) return { state: 'malformed', reason: 'marker fields are invalid' }
  if (marker.detached !== undefined && !isDetachedIdentity(marker.detached)) {
    return { state: 'malformed', reason: 'detached execution identity is invalid' }
  }
  return { state: 'valid', marker: marker as TaskExecutionMarker }
}

function isDetachedIdentity(value: unknown): value is DetachedExecutionIdentity {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const identity = value as Partial<DetachedExecutionIdentity>
  return typeof identity.environmentId === 'string' && identity.environmentId.length > 0 &&
    typeof identity.sessionId === 'string' && identity.sessionId.length > 0 &&
    typeof identity.executionId === 'string' && identity.executionId.length > 0 &&
    typeof identity.turnId === 'string' && identity.turnId.length > 0
}

function sameDetachedIdentity(
  left: DetachedExecutionIdentity | undefined,
  right: DetachedExecutionIdentity,
): boolean {
  return left?.environmentId === right.environmentId &&
    left.sessionId === right.sessionId &&
    left.executionId === right.executionId &&
    left.turnId === right.turnId
}
