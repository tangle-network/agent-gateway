import {
  deliverDemoPushNotifications,
  deliverPushNotifications,
  type PushDeliveryResult,
  type PushNotificationDeliveryOptions,
  type PushNotificationStore,
} from './push-notifications'
import type { Task } from './types'
import {
  compareAndSetTask,
  clearTaskMetadata,
  TERMINAL_STATES,
  type TaskStateStore,
} from './task-state'

const PUSH_DELIVERY_METADATA_KEY = 'gatewayPushDelivery'

interface TaskPushDeliveryClaims {
  version: 1
  claims: Record<string, Task['status']['state']>
}

export interface PushDeliveryDependencies {
  taskStore: TaskStateStore
  pushStore?: PushNotificationStore
  demoMode: boolean
  webhookSecret?: string
  fetcher?: PushNotificationDeliveryOptions['fetcher']
  urlValidator?: PushNotificationDeliveryOptions['urlValidator']
  onDeliveryFailure?: (task: Task, result: PushDeliveryResult) => void
}

export async function deliverTaskPush(
  task: Task,
  deps: PushDeliveryDependencies,
): Promise<void> {
  if (!deps.pushStore || !TERMINAL_STATES.has(task.status.state)) return
  const webhookSecret = deps.webhookSecret
  const hasWebhookSecret = typeof webhookSecret === 'string' && webhookSecret.trim().length > 0
  if (!deps.demoMode && !hasWebhookSecret) {
    console.error(`[agent-gateway] production A2A push requires a webhookSecret for task ${task.id}`)
    return
  }
  try {
    const deliveryTask = clearPushDeliveryClaims(task)
    const deliveryArgs: Omit<PushNotificationDeliveryOptions, 'webhookSecret'> = {
      task: deliveryTask,
      store: deps.pushStore,
      fetcher: deps.fetcher,
      urlValidator: deps.urlValidator,
      requireUrlValidator: !deps.demoMode,
      claimDelivery: (taskId, configId, terminalState) => claimTaskPushDelivery(
        deps.taskStore,
        taskId,
        configId,
        terminalState,
      ),
      onDelivery: (result) => {
        if (!result.ok) deps.onDeliveryFailure?.(task, result)
      },
    }
    if (hasWebhookSecret) {
      await deliverPushNotifications({ ...deliveryArgs, webhookSecret })
    } else {
      await deliverDemoPushNotifications(deliveryArgs)
    }
  } catch (err) {
    console.error(
      `[agent-gateway] push delivery threw for task ${task.id}: ${err instanceof Error ? err.message : String(err)}`,
    )
  }
}

async function claimTaskPushDelivery(
  taskStore: TaskStateStore,
  taskId: string,
  configId: string,
  terminalState: Task['status']['state'],
): Promise<boolean> {
  if (!TERMINAL_STATES.has(terminalState)) return false
  for (let attempt = 0; attempt < 16; attempt += 1) {
    const current = await taskStore.get(taskId)
    if (!current || current.status.state !== terminalState) return false
    const existing = readPushDeliveryClaims(current)
    if (existing?.claims[configId] === terminalState) return false
    const next: Task = {
      ...current,
      metadata: {
        ...(current.metadata ?? {}),
        [PUSH_DELIVERY_METADATA_KEY]: {
          version: 1,
          claims: {
            ...(existing?.claims ?? {}),
            [configId]: terminalState,
          },
        } satisfies TaskPushDeliveryClaims,
      },
    }
    if (await compareAndSetTask(taskStore, current, next)) return true
  }
  throw new Error(`A2A push delivery claim changed too many times for task '${taskId}'`)
}

function clearPushDeliveryClaims(task: Task): Task {
  return clearTaskMetadata(task, PUSH_DELIVERY_METADATA_KEY)
}

function readPushDeliveryClaims(task: Task): TaskPushDeliveryClaims | undefined {
  const raw = task.metadata?.[PUSH_DELIVERY_METADATA_KEY]
  if (!raw || typeof raw !== 'object') return undefined
  const record = raw as Partial<TaskPushDeliveryClaims>
  if (!record.claims || typeof record.claims !== 'object') return undefined
  const claims = Object.fromEntries(
    Object.entries(record.claims).filter(([, state]) =>
      typeof state === 'string' && TERMINAL_STATES.has(state as Task['status']['state']),
    ),
  ) as Record<string, Task['status']['state']>
  return record.version === 1 ? { version: 1, claims } : undefined
}
