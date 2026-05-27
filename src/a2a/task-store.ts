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
  delete(id: string): Promise<void>
}

const DEFAULT_TTL_MS = 60 * 60 * 1000

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

  async delete(id: string): Promise<void> {
    this.entries.delete(id)
  }

  /**
   * Sweep expired tasks. Called inline on every read/write — cheap for the
   * Map sizes this is designed for (10s–1000s of concurrent tasks).
   */
  private gc(): void {
    const now = Date.now()
    for (const [id, entry] of this.entries) {
      if (entry.expiresAt <= now) this.entries.delete(id)
    }
  }
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}
