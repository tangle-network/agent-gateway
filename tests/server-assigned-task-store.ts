import type { TaskStore } from '../src/a2a/task-store'
import type { Task } from '../src/a2a/types'

/**
 * Test adapter that lets a scenario refer to a server-created task by a
 * stable fixture name when the response is intentionally blocked.
 */
export class ServerAssignedTaskStore implements TaskStore {
  private assignedId: string | undefined

  constructor(
    private readonly inner: TaskStore,
    private readonly alias: string,
  ) {}

  get serverAssignedId(): string | undefined {
    return this.assignedId
  }

  private resolve(id: string): string {
    return id === this.alias ? this.assignedId ?? id : id
  }

  get(id: string): Promise<Task | undefined> {
    return this.inner.get(this.resolve(id))
  }

  put(task: Task): Promise<void> {
    return this.inner.put(task)
  }

  async createIfAbsent(task: Task): Promise<boolean> {
    const createIfAbsent = this.inner.createIfAbsent
    if (!createIfAbsent) throw new Error('inner task store must support createIfAbsent')
    try {
      const created = await createIfAbsent.call(this.inner, task)
      if (created && !this.assignedId) this.assignedId = task.id
      return created
    } catch (error) {
      if (!this.assignedId && await this.inner.get(task.id)) this.assignedId = task.id
      throw error
    }
  }

  compareAndSet(expected: Task, next: Task): Promise<boolean> {
    if (!this.inner.compareAndSet) throw new Error('inner task store must support compareAndSet')
    return this.inner.compareAndSet(
      { ...expected, id: this.resolve(expected.id) },
      { ...next, id: this.resolve(next.id) },
    )
  }

  compareAndSetExecution(
    expected: Task,
    next: Task,
    requestId: string,
    now: number,
  ): Promise<boolean> {
    if (!this.inner.compareAndSetExecution) {
      throw new Error('inner task store must support compareAndSetExecution')
    }
    return this.inner.compareAndSetExecution(
      { ...expected, id: this.resolve(expected.id) },
      { ...next, id: this.resolve(next.id) },
      requestId,
      now,
    )
  }

  delete(id: string): Promise<void> {
    return this.inner.delete(this.resolve(id))
  }
}
