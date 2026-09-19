export class TaskCancellationRegistry {
  private readonly controllers = new Map<string, AbortController>()
  private readonly finalizing = new Set<string>()

  register(taskId: string): AbortController {
    const controller = new AbortController()
    this.controllers.set(taskId, controller)
    return controller
  }

  clear(taskId: string): void {
    this.controllers.delete(taskId)
    this.finalizing.delete(taskId)
  }

  beginFinalization(taskId: string): boolean {
    const controller = this.controllers.get(taskId)
    if (!controller || controller.signal.aborted || this.finalizing.has(taskId)) return false
    this.finalizing.add(taskId)
    return true
  }

  isFinalizing(taskId: string): boolean {
    return this.finalizing.has(taskId)
  }

  has(taskId: string): boolean {
    const controller = this.controllers.get(taskId)
    return controller !== undefined && !controller.signal.aborted
  }

  cancel(taskId: string): boolean {
    if (this.finalizing.has(taskId)) return false
    const controller = this.controllers.get(taskId)
    if (!controller) return false
    controller.abort()
    this.controllers.delete(taskId)
    return true
  }
}
