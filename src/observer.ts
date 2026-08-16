/**
 * Observability hook surface.
 *
 * Consumers implement GatewayObserver to wire the gateway into their existing
 * telemetry stack (Langfuse, OTEL, structured logs, Prometheus, etc.) without
 * the gateway itself depending on any of those libraries.
 *
 * Every event carries a requestId so downstream metrics can correlate the
 * payment verification, sandbox execution, and settlement for one request.
 * When no observer is configured, the gateway stays silent.
 */

import type { GatewayUsageEvent, PaymentMethod } from './payment-types'
import type { AuthFailureReason, GatewayObserver, RequestContext } from './observer-types'

export type { AuthFailureReason, GatewayObserver, RequestContext } from './observer-types'

// ---------------------------------------------------------------------------
// Convenience implementations
// ---------------------------------------------------------------------------

/**
 * Structured-log observer. Emits one JSON line per event on the `log` function.
 * Default sink: console.log. Production consumers usually pipe their own
 * structured logger (pino, winston, the cf Logs binding).
 *
 * Usage:
 *   new ConsoleObserver(({ level, event, ...rest }) => logger.info({ event, ...rest }))
 */
export class ConsoleObserver implements GatewayObserver {
  constructor(
    private readonly log: (entry: Record<string, unknown>) => void = (e) => console.log(JSON.stringify(e)),
  ) {}

  private emit(level: 'info' | 'warn' | 'error', event: string, ctx: RequestContext, rest: Record<string, unknown> = {}) {
    this.log({
      level,
      event,
      time: new Date().toISOString(),
      requestId: ctx.requestId,
      agentSlug: ctx.agentSlug,
      durationMs: Date.now() - ctx.startMs,
      ...rest,
    })
  }

  onRequestStart(ctx: RequestContext) { this.emit('info', 'gateway.request.start', ctx) }
  onPaymentVerified(ctx: RequestContext, info: { method: PaymentMethod; consumerId: string; keyId?: string }) {
    this.emit('info', 'gateway.payment.verified', ctx, info)
  }
  onAuthFailure(ctx: RequestContext, reason: AuthFailureReason) {
    this.emit('warn', 'gateway.auth.failure', ctx, reason as unknown as Record<string, unknown>)
  }
  onRateLimited(ctx: RequestContext, info: { consumerId: string; retryAfterSeconds: number }) {
    this.emit('warn', 'gateway.rate_limit', ctx, info)
  }
  onBodyTooLarge(ctx: RequestContext, contentLength: number) {
    this.emit('warn', 'gateway.body_too_large', ctx, { contentLength })
  }
  onInjectionDetected(ctx: RequestContext, info: { consumerId: string; patterns: string[]; blocked: boolean }) {
    this.emit('warn', 'gateway.injection', ctx, info)
  }
  onRequestComplete(ctx: RequestContext, usage: GatewayUsageEvent) {
    this.emit('info', 'gateway.request.complete', ctx, usage as unknown as Record<string, unknown>)
  }
  onStreamError(ctx: RequestContext, info: { consumerId: string; errorMessage: string }) {
    this.emit('error', 'gateway.stream.error', ctx, info)
  }
  onSettlementError(ctx: RequestContext, info: { consumerId: string; method: PaymentMethod; errorMessage: string }) {
    this.emit('error', 'gateway.settlement.error', ctx, info)
  }
}

/**
 * Compose multiple observers into one. Errors in any individual observer
 * don't break the others (fire-and-forget telemetry).
 */
export class CompositeObserver implements GatewayObserver {
  constructor(private readonly observers: GatewayObserver[]) {}

  private async fanOut<K extends keyof GatewayObserver>(event: K, ...args: unknown[]): Promise<void> {
    for (const obs of this.observers) {
      const fn = obs[event] as ((...a: unknown[]) => void | Promise<void>) | undefined
      if (!fn) continue
      try {
        await fn.apply(obs, args)
      } catch (err) {
        console.warn(`[agent-gateway] observer ${event} threw:`, err instanceof Error ? err.message : err)
      }
    }
  }

  onRequestStart = (ctx: RequestContext) => this.fanOut('onRequestStart', ctx)
  onPaymentVerified = (ctx: RequestContext, info: Parameters<Required<GatewayObserver>['onPaymentVerified']>[1]) =>
    this.fanOut('onPaymentVerified', ctx, info)
  onAuthFailure = (ctx: RequestContext, reason: AuthFailureReason) =>
    this.fanOut('onAuthFailure', ctx, reason)
  onRateLimited = (ctx: RequestContext, info: Parameters<Required<GatewayObserver>['onRateLimited']>[1]) =>
    this.fanOut('onRateLimited', ctx, info)
  onBodyTooLarge = (ctx: RequestContext, contentLength: number) =>
    this.fanOut('onBodyTooLarge', ctx, contentLength)
  onInjectionDetected = (ctx: RequestContext, info: Parameters<Required<GatewayObserver>['onInjectionDetected']>[1]) =>
    this.fanOut('onInjectionDetected', ctx, info)
  onRequestComplete = (ctx: RequestContext, usage: GatewayUsageEvent) =>
    this.fanOut('onRequestComplete', ctx, usage)
  onStreamError = (ctx: RequestContext, info: Parameters<Required<GatewayObserver>['onStreamError']>[1]) =>
    this.fanOut('onStreamError', ctx, info)
  onSettlementError = (ctx: RequestContext, info: Parameters<Required<GatewayObserver>['onSettlementError']>[1]) =>
    this.fanOut('onSettlementError', ctx, info)
}

/**
 * Generate a request-id. Crypto-random 16 bytes, hex-encoded with an `req_` prefix.
 * Works in Workers, Node, and browsers — all have globalThis.crypto.
 */
export function generateRequestId(): string {
  const bytes = new Uint8Array(16)
  globalThis.crypto.getRandomValues(bytes)
  const hex = Array.from(bytes).map((b) => b.toString(16).padStart(2, '0')).join('')
  return `req_${hex}`
}
