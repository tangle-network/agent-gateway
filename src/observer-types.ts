import type {
  GatewayUsageEvent,
  PaymentMethod,
} from './payment-types'

export interface RequestContext {
  requestId: string
  agentSlug: string
  startMs: number
}

export interface AuthFailureReason {
  method: PaymentMethod
  code: string
  httpStatus: number
}

export interface GatewayObserver {
  /** Called at the start of every chat completions POST. */
  onRequestStart?: (ctx: RequestContext) => void | Promise<void>

  /** Called when a payment method has been successfully verified. */
  onPaymentVerified?: (ctx: RequestContext, info: {
    method: PaymentMethod
    consumerId: string
    keyId?: string
  }) => void | Promise<void>

  /** Called when auth fails — every branch. */
  onAuthFailure?: (ctx: RequestContext, reason: AuthFailureReason) => void | Promise<void>

  /** Called when a consumer hits the rate limit. */
  onRateLimited?: (ctx: RequestContext, info: {
    consumerId: string
    retryAfterSeconds: number
  }) => void | Promise<void>

  /** Called when the request body exceeds the 64KB limit. */
  onBodyTooLarge?: (ctx: RequestContext, contentLength: number) => void | Promise<void>

  /** Called when prompt-injection patterns are detected. */
  onInjectionDetected?: (ctx: RequestContext, info: {
    consumerId: string
    patterns: string[]
    blocked: boolean
  }) => void | Promise<void>

  /** Called after a successful stream completes and recordUsage has fired. */
  onRequestComplete?: (ctx: RequestContext, usage: GatewayUsageEvent) => void | Promise<void>

  /** Called when the sandbox throws. The error message is pre-scrubbed. */
  onStreamError?: (ctx: RequestContext, info: {
    consumerId: string
    errorMessage: string
  }) => void | Promise<void>

  /** Called when settlement fails. Payment already occurred; this is async bookkeeping. */
  onSettlementError?: (ctx: RequestContext, info: {
    consumerId: string
    method: PaymentMethod
    errorMessage: string
  }) => void | Promise<void>
}
