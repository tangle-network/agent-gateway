import type { Context } from 'hono'
import { hasActiveTaskExecution } from './execution-fence'
import type { TaskExecutionSource } from './task-execution-recovery'
import { fail, ok } from './jsonrpc'
import { clearTaskSubmission } from './task-submission-recovery'
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
  type StreamingEvent,
  type Task,
  type TaskIdParams,
  type TaskStatusUpdateEvent,
} from './types'
import { redactSystemPromptFromOutput } from '../filter'

export interface TaskMethodDependencies {
  taskStore: TaskStateStore
  cancels: TaskCancellationRegistry
  authorizeTaskAccess: (
    c: Context,
    req: JSONRPCRequest,
    task: Task,
  ) => Promise<Response | undefined>
  recoverTask: (
    task: Task,
    requestedAgentSlug: string,
    options?: { reconcileDetached?: boolean },
  ) => Promise<Task>
  deliverPush: (task: Task) => Promise<void>
  getTaskExecution: (task: Task, requestedAgentSlug: string) => Promise<TaskExecutionSource | undefined>
  reconcileTask: (task: Task, requestedAgentSlug: string) => Promise<Task>
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
  const slug = c.req.param('slug') ?? ''
  const task = await deps.recoverTask(storedTask, slug, { reconcileDetached: false })
  if (isTerminal(task.status.state)) {
    return c.json(fail(
      req.id,
      A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
      `task '${params.id}' is in terminal state '${task.status.state}'`,
    ))
  }
  if (isTaskFinalizing(task) || deps.cancels.isFinalizing(task.id)) {
    return c.json(fail(
      req.id,
      A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
      `task '${params.id}' is being finalized`,
    ))
  }

  let execution: TaskExecutionSource | undefined
  try {
    execution = await deps.getTaskExecution(task, slug)
  } catch (error) {
    return c.json(fail(
      req.id,
      A2A_ERROR_CODES.INTERNAL_ERROR,
      error instanceof Error ? error.message : String(error),
    ))
  }
  if (hasActiveTaskExecution(task) && !execution) {
    return c.json(fail(
      req.id,
      A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
      `task '${params.id}' has an active execution fence`,
    ))
  }

  // This is the only A2A path that interrupts the provider execution.
  if (execution) {
    let interrupted: { cancelled: boolean }
    try {
      interrupted = await execution.interrupt()
    } catch (error) {
      return c.json(fail(
        req.id,
        A2A_ERROR_CODES.INTERNAL_ERROR,
        error instanceof Error ? error.message : String(error),
      ))
    }
    if (!interrupted.cancelled) {
      try {
        const reconciled = await deps.reconcileTask(task, slug)
        if (isTerminal(reconciled.status.state) || reconciled.status.state === 'input-required') {
          return c.json(fail(
            req.id,
            A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
            `task '${params.id}' is in state '${reconciled.status.state}'`,
          ))
        }
      } catch (error) {
        return c.json(fail(
          req.id,
          A2A_ERROR_CODES.INTERNAL_ERROR,
          error instanceof Error ? error.message : String(error),
        ))
      }
      return c.json(fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${params.id}' execution was not canceled`,
      ))
    }
  }

  let candidate = await deps.taskStore.get(task.id) ?? task
  for (let attempt = 0; attempt < 8; attempt += 1) {
    if (isTerminal(candidate.status.state)) {
      return c.json(fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${task.id}' changed before cancellation`,
      ))
    }
    if (isTaskFinalizing(candidate)) {
      return c.json(fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${task.id}' is being finalized`,
      ))
    }
    if (hasActiveTaskExecution(candidate) && !execution) {
      return c.json(fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_CANCELABLE,
        `task '${task.id}' has an active execution fence`,
      ))
    }
    const canceled = withStatus(clearTaskSubmission(candidate), 'canceled')
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
  const task = await deps.recoverTask(storedTask, slug, { reconcileDetached: false })
  if (isTerminal(task.status.state) || task.status.state === 'input-required') {
    return statusEventStream(req, task)
  }

  let execution: TaskExecutionSource | undefined
  try {
    execution = await deps.getTaskExecution(task, slug)
  } catch (error) {
    return c.json(fail(
      req.id,
      A2A_ERROR_CODES.INTERNAL_ERROR,
      error instanceof Error ? error.message : String(error),
    ))
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
        const send = (event: StreamingEvent) => {
          if (ctrl.desiredSize === null) return
          try {
            ctrl.enqueue(encoder.encode(`data: ${JSON.stringify(ok(req.id, event))}\n\n`))
          } catch {
            observation.abort()
          }
        }
        const sendStatus = (value: Task, metadata?: Record<string, unknown>) => send({
          kind: 'status-update',
          taskId: value.id,
          contextId: value.contextId,
          status: value.status,
          final: isTerminal(value.status.state) || value.status.state === 'input-required',
          ...(metadata ? { metadata } : {}),
        })
        let latest = task
        try {
          sendStatus(task)
          for await (const sandboxEvent of execution.events({
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
                  parts: [{
                    kind: 'text',
                    text: redactSystemPromptFromOutput(
                      sandboxEvent.data.delta,
                      execution.agent.systemPrompt,
                    ),
                  }],
                },
                append: true,
              })
            }
          }
          if (!observation.signal.aborted) {
            latest = await deps.reconcileTask(latest, slug)
            sendStatus(latest)
          }
        } catch (error) {
          if (!observation.signal.aborted) {
            try {
              latest = await deps.reconcileTask(latest, slug)
            } catch {
              // Keep the task working when the exact result is temporarily unavailable.
            }
            sendStatus(latest, { error: error instanceof Error ? error.message : String(error) })
          }
        } finally {
          c.req.raw.signal.removeEventListener('abort', abortObservation)
          try { ctrl.close() } catch { /* client disconnected */ }
        }
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
