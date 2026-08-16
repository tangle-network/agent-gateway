/**
 * Stable dispatch surface shared by the OpenAI-compatible and A2A handlers.
 * Each implementation lives in the module that owns its state transitions.
 */

export type {
  A2ADispatchEvent,
  AuthorizedRequest,
  GatewayState,
  PaymentClaimHooks,
  SettleAndRecordOptions,
} from './dispatch-types'

export {
  estimateBillableInputTokens,
  estimateTokens,
  maximumBillableInputTokens,
  requiredX402Amount,
} from './dispatch-pricing'

export { authenticateAndGuard } from './dispatch-authorization'

export {
  beginPaymentExecution,
  claimPayment,
  markPaymentExecutionStarted,
  reclaimPayment,
  releasePayment,
  releasePaymentAfterFailure,
  renewPaymentExecution,
} from './dispatch-payment'

export {
  dispatchSandboxStream,
  dispatchSandboxStreamRich,
} from './dispatch-sandbox'

export { settleAndRecord } from './dispatch-settlement'
