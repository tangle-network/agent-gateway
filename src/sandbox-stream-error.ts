import type { SandboxStreamEvent } from './types'

/** Terminal failure reported by the sandbox event protocol. */
export class SandboxStreamError extends Error {
  readonly eventType: string
  readonly code?: string
  readonly details?: Record<string, unknown>

  constructor(event: SandboxStreamEvent, options?: ErrorOptions) {
    const rawMessage = event.data?.message
    const message = typeof rawMessage === 'string' && rawMessage.trim().length > 0
      ? rawMessage.trim()
      : 'Sandbox stream failed'
    super(message, options)
    this.name = 'SandboxStreamError'
    this.eventType = event.type ?? 'unknown'
    if (typeof event.data?.code === 'string' && event.data.code.length > 0) {
      this.code = event.data.code
    }
    if (event.data?.details && typeof event.data.details === 'object' && !Array.isArray(event.data.details)) {
      this.details = event.data.details
    }
  }
}

