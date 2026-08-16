import type { Task } from './types'
import {
  compareAndSetTask,
  cryptoRandomId,
  type TaskStateStore,
  withStatus,
} from './task-state'

const TASK_ORIGIN_METADATA_KEY = 'gatewayOrigin'
const TASK_SUBMISSION_METADATA_KEY = 'gatewaySubmission'
const TASK_SUBMISSION_RECOVERY_METADATA_KEY = 'gatewaySubmissionRecovery'
const TASK_SUBMISSION_LEASE_MS = 5 * 60 * 1000

export interface TaskOriginAgent {
  id: string
  slug: string
}

export interface TaskSubmissionIdentity {
  agent: TaskOriginAgent
  requestId: string
  consumerId: string
}

interface TaskOriginBinding {
  version: 1
  agentId: string
  agentSlug: string
}

export interface TaskSubmissionRecord {
  version: 1
  lease: { id: string; expiresAt: number }
  agentId: string
  agentSlug: string
  requestId: string
  consumerId: string
}

export interface SubmissionRecoveryDependencies {
  taskStore: TaskStateStore
  deliverPush: (task: Task) => Promise<void>
}

export function withTaskOrigin(
  metadata: Record<string, unknown> | undefined,
  agent: TaskOriginAgent,
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

export function withTaskSubmission(
  metadata: Record<string, unknown> | undefined,
  identity: TaskSubmissionIdentity,
): Record<string, unknown> {
  const origin = metadata?.[TASK_ORIGIN_METADATA_KEY]
  return {
    ...(metadata ?? {}),
    ...(origin === undefined
      ? {
          [TASK_ORIGIN_METADATA_KEY]: {
            version: 1,
            agentId: identity.agent.id,
            agentSlug: identity.agent.slug,
          } satisfies TaskOriginBinding,
        }
      : {}),
    [TASK_SUBMISSION_METADATA_KEY]: {
      version: 1,
      lease: { id: cryptoRandomId(), expiresAt: Date.now() + TASK_SUBMISSION_LEASE_MS },
      agentId: identity.agent.id,
      agentSlug: identity.agent.slug,
      requestId: identity.requestId,
      consumerId: identity.consumerId,
    } satisfies TaskSubmissionRecord,
  }
}

export function readTaskOrigin(task: Task): TaskOriginBinding | undefined {
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

export function readTaskSubmission(task: Task): TaskSubmissionRecord | undefined {
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

export function clearTaskSubmission(task: Task): Task {
  if (!task.metadata || !(TASK_SUBMISSION_METADATA_KEY in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[TASK_SUBMISSION_METADATA_KEY]
  if (Object.keys(metadata).length > 0) return { ...task, metadata }
  const { metadata: _metadata, ...withoutMetadata } = task
  return withoutMetadata
}

export async function recoverSubmissionIfNeeded(
  task: Task,
  deps: SubmissionRecoveryDependencies,
): Promise<Task> {
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
    await deps.deliverPush(failed)
    return failed
  }
  return await deps.taskStore.get(task.id) ?? task
}

async function clearTaskSubmissionMarker(
  taskStore: TaskStateStore,
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
