import type { Context } from 'hono'
import { hasActiveTaskExecution } from './execution-fence'
import { fail, ok } from './jsonrpc'
import type { PaymentRecoveryDependencies } from './payment-recovery'
import { isTaskFinalizing } from './task-finalization'
import type { TaskCancellationRegistry } from './task-cancellation'
import {
  compareAndSetTask,
  isTerminal,
  withStatus,
} from './task-state'
import type { TaskStateStore } from './task-state'
import {
  A2A_ERROR_CODES,
  type JSONRPCRequest,
  type Task,
  type TaskIdParams,
  type TaskStatusUpdateEvent,
  type StreamingEvent,
} from './types'
import type { SandboxPromptResult, SandboxRunControlRef, SandboxStreamEvent } from '../types'

export interface TaskExecutionSource {
  reference: SandboxRunControlRef
  events: (opts?: { since?: string; signal?: AbortSignal }) => AsyncIterable<SandboxStreamEvent>
  result: () => Promise<SandboxPromptResult>
  interrupt: () => Promise<{ cancelled: boolean }>
  translateText: (value: string) => string
}

export interface TaskMethodDependencies {
  taskStore: TaskStateStore
  payment: PaymentRecoveryDependencies
  cancels: TaskCancellationRegistry
  authorizeTaskAccess: (
    c: Context,
    req: JSONRPCRequest,
    task: Task,
  ) => Promise<Response | undefined>
  recoverTask: (task: Task, requestedAgentSlug: string) => Promise<Task>
  deliverPush: (task: Task) => Promise<void>
  getTaskExecution: (task: Task, requestedAgentSlug: string) => Promise<TaskExecutionSource | undefined>
  reconcileTask: (
    task: Task,
    requestedAgentSlug: string,
    allowActive?: boolean,
  ) => Promise<Task>
}

export async function handleTasksGet(
  c: Context,
  req: JSONRPCRequest,
  deps: TaskMethodDependencies,
): Promise<Response> {
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const storedTask = await deps.taskStore.get(params.id)
  if (!storedTask) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, storedTask)
  if (accessError) return accessError
  const slug = c.req.param('slug') ?? ''
  const task = await deps.recoverTask(storedTask, slug)
  const reconciled = await deps.reconcileTask(task, slug, true)
  return c.json(ok(req.id, reconciled))
}

export async function handleTasksCancel(
  c: Context,
  req: JSONRPCRequest,
  deps: TaskMethodDependencies,
): Promise<Response> {
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const storedTask = await deps.taskStore.get(params.id)
  if (!storedTask) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, storedTask)
  if (accessError) return accessError
  const slug = c.req.param('slug') ?? ''
  const task = await deps.recoverTask(storedTask, slug)
  if (isTerminal(task.status.state)) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${params.id}' is in terminal state '${task.status.state}'`,
      ),
    )
  }
  if (isTaskFinalizing(task) || deps.cancels.isFinalizing(task.id)) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' is being finalized`),
    )
  }
  if (hasActiveTaskExecution(task) && !deps.cancels.has(task.id)) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' has an active execution fence`),
    )
  }
  let execution: TaskExecutionSource | undefined
  try {
    execution = await deps.getTaskExecution(task, slug)
  } catch (error) {
    return executionError(c, req, error)
  }
  let candidate = task
  if (execution) {
    let interrupted: { cancelled: boolean }
    try {
      interrupted = await execution.interrupt()
    } catch (error) {
      return executionError(c, req, error)
    }
    if (!interrupted.cancelled) {
      const reconciled = await deps.reconcileTask(task, slug, true)
      if (isTerminal(reconciled.status.state) || reconciled.status.state === 'input-required') {
        return c.json(fail(
          req.id,
          A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
          `task '${task.id}' is in state '${reconciled.status.state}'`,
        ))
      }
      candidate = reconciled
    }
  }
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
    if (hasActiveTaskExecution(candidate) && !deps.cancels.has(candidate.id) && !execution) {
      return c.json(
        fail(req.id, A2A_ERROR_CODES.TASK_NOT_CANCELABLE, `task '${task.id}' has an active execution fence`),
      )
    }
    const canceled = withStatus(candidate, 'canceled')
    if (await compareAndSetTask(deps.taskStore, candidate, canceled)) {
      const stillActive = deps.cancels.cancel(task.id)
      if (!stillActive) await deps.deliverPush(canceled)
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

export async function handleTasksResubscribe(
  c: Context,
  req: JSONRPCRequest,
  deps: TaskMethodDependencies,
): Promise<Response> {
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const storedTask = await deps.taskStore.get(params.id)
  if (!storedTask) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, storedTask)
  if (accessError) return accessError
  const slug = c.req.param('slug') ?? ''
  const task = await deps.recoverTask(storedTask, slug)
  if (isTerminal(task.status.state) || task.status.state === 'input-required') {
    return statusEventStream(req, task)
  }
  let execution: TaskExecutionSource | undefined
  try {
    execution = await deps.getTaskExecution(task, slug)
  } catch (error) {
    return executionError(c, req, error)
  }
  if (!execution) return statusEventStream(req, task)
  const observation = new AbortController()
  const abortObservation = () => observation.abort()
  if (c.req.raw.signal.aborted) abortObservation()
  else c.req.raw.signal.addEventListener('abort', abortObservation, { once: true })
  const encoder = new TextEncoder()
  const stream = new ReadableStream({
    start(ctrl) {
      void (async () => {
        const send = (value: StreamingEvent) => {
          if (ctrl.desiredSize === null) return
          try {
            ctrl.enqueue(encoder.encode(`data: ${JSON.stringify(ok(req.id, value))}\n\n`))
          } catch {
            observation.abort()
          }
        }
        send(taskStatusEvent(task))
        let latest = task
        let observationError: unknown
        try {
          for await (const sandboxEvent of execution!.events({
            ...(params.lastEventId ? { since: params.lastEventId } : {}),
            signal: observation.signal,
          })) {
            if (sandboxEvent.type === 'message.part.updated' && sandboxEvent.data?.delta) {
              send({
                kind: 'artifact-update',
                taskId: task.id,
                contextId: task.contextId,
                artifact: {
                  artifactId: `${task.id}-artifact-0`,
                  name: 'response',
                  parts: [{ kind: 'text', text: execution!.translateText(sandboxEvent.data.delta) }],
                },
                append: true,
              })
            }
          }
        } catch (error) {
          observationError = error
        }
        if (!observation.signal.aborted) {
          try {
            latest = await deps.reconcileTask(task, slug, true)
            send({
              kind: 'status-update',
              taskId: task.id,
              contextId: task.contextId,
              status: latest.status,
              final: isTerminal(latest.status.state) || latest.status.state === 'input-required',
              ...(observationError ? { metadata: { error: observationError instanceof Error ? observationError.message : String(observationError) } } : {}),
            })
          } catch (error) {
            send({
              kind: 'status-update',
              taskId: task.id,
              contextId: task.contextId,
              status: (await deps.taskStore.get(task.id) ?? task).status,
              final: false,
              metadata: { error: error instanceof Error ? error.message : String(error) },
            })
          }
        }
        c.req.raw.signal.removeEventListener('abort', abortObservation)
        try { ctrl.close() } catch { /* client disconnected */ }
      })()
    },
    cancel() {
      observation.abort()
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

function statusEventStream(req: JSONRPCRequest, task: Task): Response {
  const event = taskStatusEvent(task)
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

function taskStatusEvent(task: Task): TaskStatusUpdateEvent {
  return {
    kind: 'status-update',
    taskId: task.id,
    contextId: task.contextId,
    status: task.status,
    final: isTerminal(task.status.state) || task.status.state === 'input-required',
  }
}

function executionError(c: Context, req: JSONRPCRequest, error: unknown): Response {
  return c.json(fail(
    req.id,
    A2A_ERROR_CODES.INTERNAL_ERROR,
    error instanceof Error ? error.message : String(error),
  ))
}
