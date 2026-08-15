/** Version of the gateway's method-specific MPP charge contract. */
export const MPP_CHARGE_PROTOCOL_VERSION = 1 as const

export type MppChargeOperationState = 'confirmed' | 'releasing' | 'released'

/** Pure authentication result for one method credential. */
export interface MppAuthenticatedCredential {
  consumerId: string
  /**
   * Stable, non-secret processor identity for this payment credential.
   * Return the same value for equivalent encodings of one credential.
   * The gateway hashes this value before it persists or claims it.
   */
  paymentIdentity: string
}

/** Durable result of one immediate MPP charge. */
export interface MppChargeOperation {
  protocolVersion: typeof MPP_CHARGE_PROTOCOL_VERSION
  operationId: string
  acquiredByRequestId: string
  method: string
  /** A complete Payment-Receipt header value. */
  receipt: string
  state: MppChargeOperationState
}

export interface MppChargeRequest {
  /**
   * Stable provider idempotency key. The adapter must bind every processor
   * operation to this value before it attempts confirmation.
   */
  operationId: string
  requestId: string
  agentId: string
  consumerId: string
  method: string
  /** Original decoded credential. It is available only on the live request. */
  credential: string
  amount: bigint
  currencyDecimals: number
}

export type MppChargeRecoveryResult =
  | MppChargeOperation
  /** `not-found` is final and must fence this operation ID against a later charge. */
  | { operationId: string; state: 'not-found' | 'pending' }

/**
 * Immediate-charge lifecycle for a non-BlueprinTEVM MPP method.
 *
 * `confirmPayment` runs after every request denial and before a response or
 * sandbox call. It must use `operationId` as its processor idempotency key,
 * confirm payment, verify final success, and only then return `confirmed`.
 *
 * Every method must also support id-only recovery and an idempotent release.
 * Recovery must inspect the existing processor operation. It must never
 * create a second charge when an acknowledgement is ambiguous.
 */
export interface MppChargeLifecycle {
  readonly protocolVersion: typeof MPP_CHARGE_PROTOCOL_VERSION
  confirmPayment(request: MppChargeRequest): Promise<MppChargeOperation>
  releasePayment(operation: MppChargeOperation, reason: string): Promise<MppChargeOperation>
  recoverPayment(operationId: string): Promise<MppChargeRecoveryResult>
}

/** Stable gateway identity. Neither the credential nor adapter identity is persisted. */
export async function mppPaymentOperationId(
  method: string,
  paymentIdentity: string,
): Promise<string> {
  const normalizedMethod = method.trim().toLowerCase()
  if (!normalizedMethod || normalizedMethod.length > 128) {
    throw new Error('MPP payment method is invalid')
  }
  if (!paymentIdentity || paymentIdentity.length > 8192) {
    throw new Error('MPP payment identity is invalid')
  }
  const digest = await globalThis.crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(`${normalizedMethod}\0${paymentIdentity}`),
  )
  const fingerprint = [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('')
  return `mpp:${normalizedMethod}:${fingerprint}`
}

export function assertMppChargeOperation(
  operation: MppChargeOperation,
  expected: { operationId: string; requestId: string; method: string },
  allowedStates: readonly MppChargeOperationState[],
  requireReceipt = true,
): void {
  if (operation.protocolVersion !== MPP_CHARGE_PROTOCOL_VERSION) {
    throw new Error('MPP charge operation protocol version mismatch')
  }
  if (operation.operationId !== expected.operationId) {
    throw new Error('MPP charge operation id mismatch')
  }
  if (operation.acquiredByRequestId !== expected.requestId) {
    throw new Error('MPP charge operation request owner mismatch')
  }
  if (operation.method.toLowerCase() !== expected.method.toLowerCase()) {
    throw new Error('MPP charge operation method mismatch')
  }
  if (!allowedStates.includes(operation.state)) {
    throw new Error(`MPP charge operation is in invalid state ${operation.state}`)
  }
  if (requireReceipt && (
    !operation.receipt ||
    operation.receipt.length > 8192 ||
    /[^\x20-\x7e]/.test(operation.receipt)
  )) {
    throw new Error('MPP charge operation has an invalid payment receipt')
  }
}
