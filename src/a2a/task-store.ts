/**
 * Task persistence behind the JSON-RPC dispatcher. Default adapter is in
 * memory with a 1-hour TTL — adequate for tests, scratch, and Workers with
 * a short-lived process. Production deployments wire their own
 * `TaskStore` (D1, postgres, Durable Object) via `GatewayConfig.a2a`.
 */

import type { Task } from './types'

export interface TaskStore {
  get(id: string): Promise<Task | undefined>
  put(task: Task): Promise<void>
  /** Insert only when the task id is absent. Required outside explicit demo mode. */
  createIfAbsent?(task: Task): Promise<boolean>
  /** Replace only when the stored task still equals `expected`. Required for production races. */
  compareAndSet?(expected: Task, next: Task): Promise<boolean>
  delete(id: string): Promise<void>
}

const DEFAULT_TTL_MS = 60 * 60 * 1000

const PAYMENT_RECOVERY_KEYS = [
  'gatewayFinalizing',
  'gatewayPaymentRelease',
  'gatewayPaymentRecovery',
] as const

/** Payment recovery tasks must remain readable until reconciliation clears the marker. */
export function hasPendingPaymentRecovery(task: Task): boolean {
  return PAYMENT_RECOVERY_KEYS.some((key) => task.metadata?.[key] !== undefined)
}

export class InMemoryTaskStore implements TaskStore {
  private readonly entries = new Map<string, { task: Task; expiresAt: number }>()

  constructor(private readonly ttlMs: number = DEFAULT_TTL_MS) {}

  async get(id: string): Promise<Task | undefined> {
    this.gc()
    const entry = this.entries.get(id)
    if (!entry) return undefined
    return clone(entry.task)
  }

  async put(task: Task): Promise<void> {
    this.gc()
    this.entries.set(task.id, { task: clone(task), expiresAt: Date.now() + this.ttlMs })
  }

  async createIfAbsent(task: Task): Promise<boolean> {
    this.gc()
    if (this.entries.has(task.id)) return false
    this.entries.set(task.id, { task: clone(task), expiresAt: Date.now() + this.ttlMs })
    return true
  }

  async compareAndSet(expected: Task, next: Task): Promise<boolean> {
    this.gc()
    const entry = this.entries.get(expected.id)
    if (!entry || JSON.stringify(entry.task) !== JSON.stringify(expected)) return false
    this.entries.set(expected.id, { task: clone(next), expiresAt: Date.now() + this.ttlMs })
    return true
  }

  async delete(id: string): Promise<void> {
    const entry = this.entries.get(id)
    if (entry && hasPendingPaymentRecovery(entry.task)) return
    this.entries.delete(id)
  }

  /**
   * Sweep expired tasks. Called inline on every read/write — cheap for the
   * Map sizes this is designed for (10s–1000s of concurrent tasks).
   */
  private gc(): void {
    const now = Date.now()
    for (const [id, entry] of this.entries) {
      if (entry.expiresAt <= now && !hasPendingPaymentRecovery(entry.task)) {
        this.entries.delete(id)
      }
    }
  }
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}
