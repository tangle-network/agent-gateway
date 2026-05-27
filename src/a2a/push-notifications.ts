/**
 * @stable
 *
 * A2A push notifications — a webhook delivery channel that fires when a task
 * reaches a terminal state (`completed`, `canceled`, `failed`, `rejected`).
 * The protocol specifies four JSON-RPC methods (`tasks/pushNotificationConfig/`
 * {set, get, list, delete}) for registering / inspecting / removing configs,
 * plus the delivery contract: an HTTP POST to the registered URL with the
 * task envelope as the body and an HMAC-SHA256 signature for verification.
 *
 * This is the minimum shape long-horizon agents need. A consumer that finishes
 * a task in 30 minutes can't keep an SSE stream open against a Worker (CPU
 * limits) or an unauthenticated browser tab (network drops) — they need a
 * fire-and-forget endpoint the gateway calls when the task is done.
 *
 * Out of scope for the first pass: retries, queue durability, partial-state
 * notifications. If the webhook returns non-2xx or the request fails, the
 * gateway logs and moves on — the consumer's endpoint should idempotently
 * pull state via `tasks/get` rather than rely on at-least-once delivery.
 *
 * @example registering a webhook
 *   {
 *     "jsonrpc": "2.0", "id": 1, "method": "tasks/pushNotificationConfig/set",
 *     "params": {
 *       "taskId": "task_abc",
 *       "pushNotificationConfig": {
 *         "id": "cfg_1",
 *         "url": "https://my-consumer.example.com/agent/done",
 *         "token": "my-shared-secret-not-the-hmac-secret"
 *       }
 *     }
 *   }
 *
 * @example webhook delivery
 *   POST https://my-consumer.example.com/agent/done
 *   X-A2A-Notification-Token: my-shared-secret-not-the-hmac-secret
 *   X-A2A-Signature: sha256=<hex(HMAC-SHA256(webhookSecret, body))>
 *   Content-Type: application/json
 *   { "taskId": "task_abc", "state": "completed", "task": { ...full Task... } }
 */

import type { SqlAdapter } from './task-store-sql'
import type { Task } from './types'

/**
 * Authentication metadata for the webhook itself. The A2A spec leaves this
 * to consumers — the most common shape is a bearer token the gateway sends as
 * `Authorization: <scheme> <credential>`. We pass it through verbatim.
 */
export interface PushNotificationAuthentication {
  schemes: string[]
  credentials?: string
}

/**
 * Per-task push notification configuration. A task can have multiple configs
 * (e.g. one for the consumer's own webhook + one for an audit log endpoint).
 */
export interface PushNotificationConfig {
  /** Stable id within the task's config set. Required for get/delete addressing. */
  id: string
  /** HTTPS URL the gateway will POST to. */
  url: string
  /**
   * Opaque token the gateway sends back as `X-A2A-Notification-Token` so the
   * webhook can verify the call originated from a registration the consumer
   * authorized. Distinct from the HMAC signature (which proves the body
   * wasn't tampered with) — this proves the registration is recognised.
   */
  token?: string
  /** Optional webhook-side auth metadata. */
  authentication?: PushNotificationAuthentication
}

export interface TaskPushNotificationConfig {
  taskId: string
  pushNotificationConfig: PushNotificationConfig
}

/**
 * Storage for push configs. The default in-memory store is fine for single
 * Worker instances + tests; production multi-instance deployments need
 * `SqlPushNotificationStore` (or any other shared-state adapter) so a config
 * registered on instance A is visible to a delivery firing from instance B.
 */
export interface PushNotificationStore {
  set(taskId: string, config: PushNotificationConfig): Promise<void>
  get(taskId: string, configId: string): Promise<PushNotificationConfig | undefined>
  list(taskId: string): Promise<PushNotificationConfig[]>
  delete(taskId: string, configId: string): Promise<void>
}

export class InMemoryPushNotificationStore implements PushNotificationStore {
  private readonly byTask = new Map<string, Map<string, PushNotificationConfig>>()

  async set(taskId: string, config: PushNotificationConfig): Promise<void> {
    let configs = this.byTask.get(taskId)
    if (!configs) {
      configs = new Map()
      this.byTask.set(taskId, configs)
    }
    configs.set(config.id, { ...config })
  }

  async get(taskId: string, configId: string): Promise<PushNotificationConfig | undefined> {
    const cfg = this.byTask.get(taskId)?.get(configId)
    return cfg ? { ...cfg } : undefined
  }

  async list(taskId: string): Promise<PushNotificationConfig[]> {
    const configs = this.byTask.get(taskId)
    if (!configs) return []
    return [...configs.values()].map((c) => ({ ...c }))
  }

  async delete(taskId: string, configId: string): Promise<void> {
    const configs = this.byTask.get(taskId)
    if (!configs) return
    configs.delete(configId)
    if (configs.size === 0) this.byTask.delete(taskId)
  }
}

const PUSH_TABLE_DDL = (table: string) => `
  CREATE TABLE IF NOT EXISTS ${table} (
    task_id TEXT NOT NULL,
    config_id TEXT NOT NULL,
    url TEXT NOT NULL,
    token TEXT,
    authentication TEXT,
    PRIMARY KEY (task_id, config_id)
  )
`

/** SQL-backed push config store. Schema: one row per (taskId, configId). */
export class SqlPushNotificationStore implements PushNotificationStore {
  constructor(
    private readonly db: SqlAdapter,
    private readonly table: string = 'a2a_push_configs',
  ) {}

  async migrate(): Promise<void> {
    await this.db.exec(PUSH_TABLE_DDL(this.table))
  }

  async set(taskId: string, config: PushNotificationConfig): Promise<void> {
    const auth = config.authentication ? JSON.stringify(config.authentication) : null
    const updated = await this.db.exec(
      `UPDATE ${this.table} SET url = ?, token = ?, authentication = ? WHERE task_id = ? AND config_id = ?`,
      [config.url, config.token ?? null, auth, taskId, config.id],
    )
    if (updated.rowsAffected === 0) {
      await this.db.exec(
        `INSERT INTO ${this.table} (task_id, config_id, url, token, authentication) VALUES (?, ?, ?, ?, ?)`,
        [taskId, config.id, config.url, config.token ?? null, auth],
      )
    }
  }

  async get(taskId: string, configId: string): Promise<PushNotificationConfig | undefined> {
    const rows = await this.db.query<{
      config_id: string
      url: string
      token: string | null
      authentication: string | null
    }>(
      `SELECT config_id, url, token, authentication FROM ${this.table} WHERE task_id = ? AND config_id = ?`,
      [taskId, configId],
    )
    const row = rows[0]
    if (!row) return undefined
    return {
      id: row.config_id,
      url: row.url,
      token: row.token ?? undefined,
      authentication: row.authentication
        ? (JSON.parse(row.authentication) as PushNotificationAuthentication)
        : undefined,
    }
  }

  async list(taskId: string): Promise<PushNotificationConfig[]> {
    const rows = await this.db.query<{
      config_id: string
      url: string
      token: string | null
      authentication: string | null
    }>(
      `SELECT config_id, url, token, authentication FROM ${this.table} WHERE task_id = ?`,
      [taskId],
    )
    return rows.map((row) => ({
      id: row.config_id,
      url: row.url,
      token: row.token ?? undefined,
      authentication: row.authentication
        ? (JSON.parse(row.authentication) as PushNotificationAuthentication)
        : undefined,
    }))
  }

  async delete(taskId: string, configId: string): Promise<void> {
    await this.db.exec(
      `DELETE FROM ${this.table} WHERE task_id = ? AND config_id = ?`,
      [taskId, configId],
    )
  }
}

/**
 * Send the webhook for each registered config on a task. Signs the body with
 * HMAC-SHA256 against `webhookSecret` so the consumer can verify authenticity.
 * Fire-and-forget per the design note above — the function awaits delivery
 * (so observability hooks see the result) but does not retry on failure.
 *
 * The caller decides *when* to deliver — typically on terminal-state
 * transitions emitted from `message/send` and `message/stream`.
 */
export async function deliverPushNotifications(args: {
  task: Task
  store: PushNotificationStore
  webhookSecret: string | undefined
  /** Inject for tests. Defaults to global `fetch`. */
  fetcher?: typeof fetch
  /** Optional callback so the gateway's observer can log delivery outcomes. */
  onDelivery?: (result: PushDeliveryResult) => void
}): Promise<PushDeliveryResult[]> {
  const fetcher = args.fetcher ?? fetch
  const configs = await args.store.list(args.task.id)
  const body = JSON.stringify({
    taskId: args.task.id,
    state: args.task.status.state,
    task: args.task,
  })
  const signature = args.webhookSecret
    ? `sha256=${await hmacSha256Hex(args.webhookSecret, body)}`
    : undefined

  const results: PushDeliveryResult[] = []
  for (const config of configs) {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    if (config.token) headers['X-A2A-Notification-Token'] = config.token
    if (signature) headers['X-A2A-Signature'] = signature
    if (config.authentication?.credentials) {
      const scheme = config.authentication.schemes[0] ?? 'Bearer'
      headers.Authorization = `${scheme} ${config.authentication.credentials}`
    }

    let result: PushDeliveryResult
    try {
      const res = await fetcher(config.url, { method: 'POST', headers, body })
      result = {
        taskId: args.task.id,
        configId: config.id,
        url: config.url,
        ok: res.ok,
        status: res.status,
      }
    } catch (err) {
      result = {
        taskId: args.task.id,
        configId: config.id,
        url: config.url,
        ok: false,
        error: err instanceof Error ? err.message : String(err),
      }
    }
    args.onDelivery?.(result)
    results.push(result)
  }
  return results
}

export interface PushDeliveryResult {
  taskId: string
  configId: string
  url: string
  ok: boolean
  status?: number
  error?: string
}

/**
 * HMAC-SHA256 via WebCrypto. Works on Workers, Node 19+, Bun, Deno. The
 * gateway requires WebCrypto for x402 verification already, so this adds no
 * new platform constraint.
 */
async function hmacSha256Hex(secret: string, body: string): Promise<string> {
  const enc = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw',
    enc.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )
  const sig = await crypto.subtle.sign('HMAC', key, enc.encode(body))
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('')
}
