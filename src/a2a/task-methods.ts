import type { Context } from 'hono'
import { hasActiveTaskExecution } from './execution-fence'
import { fail, ok } from './jsonrpc'
import { releaseTaskPayment } from './payment-recovery'
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
} from './types'

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
  const task = await deps.recoverTask(storedTask, c.req.param('slug') ?? '')
  return c.json(ok(req.id, task))
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
  const task = await deps.recoverTask(storedTask, c.req.param('slug') ?? '')
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
    deps.cancels.isFinalizing(task.id) ||
    (hasActiveTaskExecution(task) && !deps.cancels.has(task.id))
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
    if (hasActiveTaskExecution(candidate) && !deps.cancels.has(candidate.id)) {
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
  const task = await deps.recoverTask(storedTask, c.req.param('slug') ?? '')
  const event: TaskStatusUpdateEvent = {
    kind: 'status-update',
    taskId: task.id,
    contextId: task.contextId,
    status: task.status,
    final: isTerminal(task.status.state) || task.status.state === 'input-required',
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
