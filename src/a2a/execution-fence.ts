import type { Task } from './types'
import type { TaskStore } from './task-store'

/** Durable marker that prevents cancellation from racing sandbox start. */
export const TASK_EXECUTION_METADATA_KEY = 'gatewayExecution'

const TASK_EXECUTION_VERSION = 1 as const
const TASK_EXECUTION_LEASE_MS = 5 * 60 * 1000

interface TaskExecutionMarker {
  version: typeof TASK_EXECUTION_VERSION
  requestId: string
  lease: { id: string; expiresAt: number }
}

export class TaskExecutionCanceledError extends Error {
  constructor(taskId: string) {
    super(`A2A task '${taskId}' was canceled before sandbox execution`)
    this.name = 'TaskExecutionCanceledError'
  }
}

/** Claim the right to start one task after its sandbox has been acquired. */
export async function claimTaskExecution(
  store: TaskStore,
  task: Task,
  requestId: string,
  now = Date.now(),
): Promise<Task> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = await store.get(task.id)
    if (!current || current.status.state !== 'working') {
      throw new TaskExecutionCanceledError(task.id)
    }
    const existing = readTaskExecution(current)
    if (existing && existing.lease.expiresAt > now) {
      if (existing.requestId === requestId) return current
      throw new Error(`A2A task '${task.id}' is already executing`)
    }
    const next = withTaskExecution(current, requestId, now)
    if (store.compareAndSet && await store.compareAndSet(current, next)) return next
  }
  throw new Error(`A2A task '${task.id}' changed too many times before sandbox execution`)
}

/** Renew both the task execution fence and its cancellation protection. */
export async function renewTaskExecution(
  store: TaskStore,
  taskId: string,
  requestId: string,
  now = Date.now(),
): Promise<Task> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = await store.get(taskId)
    const marker = current ? readTaskExecution(current) : undefined
    if (!current || current.status.state !== 'working' || marker?.requestId !== requestId) {
      throw new TaskExecutionCanceledError(taskId)
    }
    const next = withTaskExecution(current, requestId, now)
    if (store.compareAndSet && await store.compareAndSet(current, next)) return next
  }
  throw new Error(`A2A task '${taskId}' changed too many times while execution was active`)
}

/** Cancellation is rejected only while a live execution fence is held. */
export function hasActiveTaskExecution(task: Task, now = Date.now()): boolean {
  const marker = readTaskExecution(task)
  return marker !== undefined && marker.lease.expiresAt > now
}

/** A working task with this marker has lost its execution owner. */
export function hasExpiredTaskExecution(task: Task, now = Date.now()): boolean {
  const marker = readTaskExecution(task)
  return marker !== undefined && !hasActiveTaskExecution(task, now)
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

function withTaskExecution(task: Task, requestId: string, now: number): Task {
  return {
    ...task,
    metadata: {
      ...(task.metadata ?? {}),
      [TASK_EXECUTION_METADATA_KEY]: {
        version: TASK_EXECUTION_VERSION,
        requestId,
        lease: { id: requestId, expiresAt: now + TASK_EXECUTION_LEASE_MS },
      } satisfies TaskExecutionMarker,
    },
  }
}

function readTaskExecution(task: Task): TaskExecutionMarker | undefined {
  const raw = task.metadata?.[TASK_EXECUTION_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
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
  ) return undefined
  return marker as TaskExecutionMarker
}
