import type { Context } from 'hono'
import {
  validatePushNotificationUrl,
  type PushNotificationStore,
  type TaskPushNotificationConfig,
} from './push-notifications'
import type { TaskStateStore } from './task-state'
import {
  A2A_ERROR_CODES,
  type JSONRPCRequest,
  type Task,
  type TaskIdParams,
  type TaskPushNotificationConfigGetParams,
} from './types'
import { fail, ok } from './jsonrpc'

export interface PushConfigMethodDependencies {
  taskStore: TaskStateStore
  pushStore?: PushNotificationStore
  demoMode: boolean
  urlValidator?: (url: URL) => boolean | Promise<boolean>
  authorizeTaskAccess: (
    c: Context,
    req: JSONRPCRequest,
    task: Task,
  ) => Promise<Response | undefined>
}

export async function handlePushSet(
  c: Context,
  req: JSONRPCRequest,
  deps: PushConfigMethodDependencies,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskPushNotificationConfig | undefined
  if (!params || typeof params.taskId !== 'string' || !params.pushNotificationConfig?.id) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.taskId and params.pushNotificationConfig.id required'),
    )
  }
  if (typeof params.pushNotificationConfig.url !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url required'))
  }
  const task = await deps.taskStore.get(params.taskId)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.taskId}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, task)
  if (accessError) return accessError
  const pushUrl = validatePushNotificationUrl(params.pushNotificationConfig.url)
  if (!pushUrl) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url is not a safe HTTPS destination'),
    )
  }
  if (!deps.demoMode && !deps.urlValidator) {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'production push URL validation is not configured'),
    )
  }
  let allowedByHostPolicy = true
  try {
    if (deps.urlValidator) allowedByHostPolicy = await deps.urlValidator(pushUrl)
  } catch (error) {
    allowedByHostPolicy = false
    console.error(
      `[a2a] push URL policy failed for task ${task.id}:`,
      error instanceof Error ? error.message : String(error),
    )
  }
  if (!allowedByHostPolicy) {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'pushNotificationConfig.url was rejected'))
  }
  await deps.pushStore.set(params.taskId, params.pushNotificationConfig)
  const stored = await deps.pushStore.get(params.taskId, params.pushNotificationConfig.id)
  return c.json(ok(req.id, { taskId: params.taskId, pushNotificationConfig: stored }))
}

export async function handlePushGet(
  c: Context,
  req: JSONRPCRequest,
  deps: PushConfigMethodDependencies,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskPushNotificationConfigGetParams | undefined
  if (!params || typeof params.id !== 'string' || typeof params.pushNotificationConfigId !== 'string') {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id and params.pushNotificationConfigId required'),
    )
  }
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, task)
  if (accessError) return accessError
  const cfg = await deps.pushStore.get(params.id, params.pushNotificationConfigId)
  if (!cfg) {
    return c.json(
      fail(
        req.id,
        A2A_ERROR_CODES.TASK_NOT_FOUND,
        `push config '${params.pushNotificationConfigId}' not found for task '${params.id}'`,
      ),
    )
  }
  return c.json(ok(req.id, { taskId: params.id, pushNotificationConfig: cfg }))
}

export async function handlePushList(
  c: Context,
  req: JSONRPCRequest,
  deps: PushConfigMethodDependencies,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskIdParams | undefined
  if (!params || typeof params.id !== 'string') {
    return c.json(fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id required'))
  }
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, task)
  if (accessError) return accessError
  const configs = await deps.pushStore.list(params.id)
  return c.json(ok(req.id, configs.map((cfg) => ({ taskId: params.id, pushNotificationConfig: cfg }))))
}

export async function handlePushDelete(
  c: Context,
  req: JSONRPCRequest,
  deps: PushConfigMethodDependencies,
): Promise<Response> {
  if (!deps.pushStore) {
    return c.json(fail(req.id, A2A_ERROR_CODES.PUSH_NOT_SUPPORTED, 'push notifications not configured'))
  }
  const params = req.params as TaskPushNotificationConfigGetParams | undefined
  if (!params || typeof params.id !== 'string' || typeof params.pushNotificationConfigId !== 'string') {
    return c.json(
      fail(req.id, A2A_ERROR_CODES.INVALID_PARAMS, 'params.id and params.pushNotificationConfigId required'),
    )
  }
  const task = await deps.taskStore.get(params.id)
  if (!task) {
    return c.json(fail(req.id, A2A_ERROR_CODES.TASK_NOT_FOUND, `task '${params.id}' not found`))
  }
  const accessError = await deps.authorizeTaskAccess(c, req, task)
  if (accessError) return accessError
  await deps.pushStore.delete(params.id, params.pushNotificationConfigId)
  return c.json(ok(req.id, null))
}
