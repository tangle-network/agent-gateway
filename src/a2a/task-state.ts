import type { Task, TaskStatus, Message } from './types'
import { clearTaskExecution } from './execution-fence'
import { hasPendingPaymentRecovery } from './task-recovery'

export interface TaskStateStore {
  get(id: string): Promise<Task | undefined>
  compareAndSet?(expected: Task, next: Task): Promise<boolean>
}

export const TERMINAL_STATES: ReadonlySet<Task['status']['state']> = new Set([
  'completed',
  'canceled',
  'failed',
  'rejected',
])

export function isTerminal(state: Task['status']['state']): boolean {
  return TERMINAL_STATES.has(state)
}

export function shouldPreserveTask(task: Task): boolean {
  return isTerminal(task.status.state) || hasPendingPaymentRecovery(task)
}

export async function compareAndSetTask(
  taskStore: TaskStateStore,
  expected: Task,
  next: Task,
): Promise<boolean> {
  if (!taskStore.compareAndSet) {
    throw new Error('A2A task store does not provide compareAndSet')
  }
  return taskStore.compareAndSet(expected, next)
}

export async function persistTaskIfCurrent(
  taskStore: TaskStateStore,
  expected: Task,
  next: Task,
): Promise<Task> {
  if (expected === next || JSON.stringify(expected) === JSON.stringify(next)) return expected
  if (await compareAndSetTask(taskStore, expected, next)) return next
  return await taskStore.get(expected.id) ?? expected
}

export function clearTaskMetadata(task: Task, key: string): Task {
  if (!task.metadata || !(key in task.metadata)) return task
  const metadata = { ...task.metadata }
  delete metadata[key]
  if (Object.keys(metadata).length > 0) return { ...task, metadata }
  const { metadata: _metadata, ...withoutMetadata } = task
  return withoutMetadata
}

export function withStatus(
  task: Task,
  state: TaskStatus['state'],
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

export function agentMessage(task: Task, text: string): Message {
  return {
    kind: 'message',
    role: 'agent',
    parts: [{ kind: 'text', text }],
    messageId: `${task.id}-input-required-${stableMessageDigest(text)}`,
    taskId: task.id,
    contextId: task.contextId,
  }
}

export function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

export function nowIso(): string {
  return new Date().toISOString()
}

export function cryptoRandomId(): string {
  return crypto.randomUUID().replace(/-/g, '')
}

function stableMessageDigest(value: string): string {
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0).toString(16).padStart(8, '0')
}
