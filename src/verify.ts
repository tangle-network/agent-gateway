import type { X402Config, MppConfig, ApiKeyInfo, GatewayConfig } from './types'
import { claimStoredNonce, nonceTtlSeconds, type NonceStore } from './nonce-store'
import {
  mppPaymentOperationId,
  type MppAuthenticatedCredential,
} from './mpp-payment'

export interface VerifiedMppCredential extends MppAuthenticatedCredential {
  /** Opaque replay key. BlueprinTEVM shares the x402 nonce namespace. */
  replayKey: string
}

/** Return the legacy opaque nonce key used by older consumers. */
export function mppReplayNonceKey(authHeader: string): string | undefined {
  const decoded = decodeMppCredential(authHeader)
  return decoded ? canonicalMppNonceKey(decoded.method, decoded.payload, decoded.credential) : undefined
}

/** Return the decoded MPP payload for a durable payment claim. */
export function mppPaymentPayload(authHeader: string): Record<string, unknown> | undefined {
  return decodeMppCredential(authHeader)?.payload
}

/** Return the decoded method credential for the post-guard charge lifecycle. */
export function mppPaymentCredential(authHeader: string): string | undefined {
  return decodeMppCredential(authHeader)?.credential
}

interface DecodedMppCredential {
  method: string
  credential: string
  payload: Record<string, unknown>
}

function decodeMppCredential(authHeader: string): DecodedMppCredential | undefined {
  const match = authHeader.match(/^Payment\s+(\S+)\s+(\S+)$/i)
  if (!match) return undefined
  const [, rawMethod, credentialB64] = match
  if (!/^[A-Za-z0-9_-]+$/.test(credentialB64)) return undefined
  try {
    const decoded = Buffer.from(credentialB64, 'base64url').toString('utf-8')
    let payload: Record<string, unknown> = {}
    try {
      const credential = JSON.parse(decoded) as unknown
      if (credential && typeof credential === 'object' && !Array.isArray(credential)) {
        const record = credential as Record<string, unknown>
        const nested = record.payload
        payload = nested && typeof nested === 'object' && !Array.isArray(nested)
          ? nested as Record<string, unknown>
          : record
      }
    } catch {
      // Method-specific verifiers may accept a non-JSON credential format.
    }
    return { method: rawMethod.toLowerCase(), credential: decoded, payload }
  } catch {
    return undefined
  }
}

function canonicalMppNonceKey(
  method: string,
  payload: Record<string, unknown>,
  credential: string,
): string {
  if (payload.nonce === undefined) {
    return `mpp:${method.toLowerCase()}:receipt:${Buffer.from(credential).toString('base64url')}`
  }
  const nonce = BigInt(String(payload.nonce)).toString()
  const commitment = payload.commitment
  if (method.toLowerCase() === 'blueprintevm' && typeof commitment === 'string' && commitment.length > 0) {
    return `${commitment.toLowerCase()}:${nonce}`
  }
  return `mpp:${method.toLowerCase()}:${String(payload.commitment ?? payload.from ?? 'unknown').toLowerCase()}:${nonce}`
}

function blueprintevmNonceKey(payload: Record<string, unknown>): string | undefined {
  if (payload.nonce === undefined) return undefined
  const nonce = BigInt(String(payload.nonce)).toString()
  const identity = payload.commitment ?? payload.from
  if (typeof identity !== 'string' || identity.length === 0) return undefined
  return `${identity.toLowerCase()}:${nonce}`
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

function legacyMppPaymentIdentity(
  method: string,
  payload: Record<string, unknown>,
  credential: string,
): string {
  const canonicalPayload = stableJson(payload)
  if (canonicalPayload !== '{}') return `legacy:${method}:${canonicalPayload}`
  return `legacy:${method}:credential:${Buffer.from(credential).toString('base64url')}`
}

/** Pure capability checks shared by discovery and every request protocol. */
export function isApiKeyAuthEnabled(
  config: Pick<GatewayConfig, 'verifyApiKey' | 'x402'>,
): boolean {
  return config.verifyApiKey !== undefined || config.x402.demoMode === true
}

/** MPP is enabled only when authentication and method settlement are complete. */
export function isMppAuthEnabled(
  config: Pick<GatewayConfig, 'mpp' | 'x402'>,
): boolean {
  const method = (config.mpp?.method ?? 'blueprintevm').toLowerCase()
  if (!config.mpp) return false
  const authenticated = typeof config.mpp.authenticateCredential === 'function' ||
    typeof config.mpp.verifySigner === 'function' ||
    (method === 'blueprintevm' && config.x402.verifySigner !== undefined) ||
    (method === 'blueprintevm' && config.x402.demoMode === true)
  if (!authenticated) return false
  return method === 'blueprintevm' || config.mpp.charge !== undefined
}

/**
 * Verify x402 SpendAuth signature (EIP-712).
 * Returns the signer address (commitment) if valid, null otherwise.
 *
 *   demoMode: true               — accepts any well-formed header
 *                                  shape after replay/expiry checks.
 *                                  Tests + local dev only.
 *   verifySigner present         — production verification path.
 *   neither                      — rejected by createAgentGateway and
 *                                  by this function as defense-in-depth.
 */
export async function verifyX402(
  spendAuthHeader: string,
  config: X402Config,
  nonceStore?: NonceStore,
  minimumAmount = 1n,
  markNonce = true,
): Promise<string | null> {
  try {
    const raw = JSON.parse(spendAuthHeader)
    if (!raw.commitment || !raw.signature || !raw.amount) return null
    if (raw.operator?.toLowerCase() !== config.operatorAddress.toLowerCase()) return null

    const amount = BigInt(raw.amount)
    const nonce = BigInt(raw.nonce)
    const expiry = BigInt(raw.expiry)

    // Reject expired payments
    if (expiry < BigInt(Math.floor(Date.now() / 1000))) return null

    // Reject payments that cannot cover the request's maximum charge. The
    // check runs before the host verifier because that callback can reserve or
    // settle funds as part of its production verification path.
    if (amount <= 0n || minimumAmount < 0n || amount < minimumAmount) return null

    const nonceKey = `${String(raw.commitment).toLowerCase()}:${nonce.toString()}`
    if (nonceStore?.hasSeen && await nonceStore.hasSeen(nonceKey)) return null

    if (config.verifySigner) {
      const verified = await config.verifySigner(raw, {
        protocolVersion: config.paymentProtocolVersion ?? (config.paymentOperations ? 2 : 1),
      })
      if (!verified) return null
    } else if (!config.demoMode) {
      return null
    }

    // Claim only after the signature is accepted. Otherwise invalid traffic
    // can burn a valid payer nonce and deny the real request.
    if (nonceStore && markNonce) {
      const ttl = nonceTtlSeconds(expiry)
      if (ttl === undefined) return null
      const claimed = await claimStoredNonce(nonceStore, nonceKey, ttl)
      if (!claimed) return null
    }

    return raw.commitment
  } catch {
    return null
  }
}

/**
 * Verify MPP (Machine Payments Protocol) Authorization: Payment header.
 *
 * MPP uses `Authorization: Payment <method> <credential>` format. The
 * credential is method-specific; `MppConfig.authenticateCredential` owns authentication
 * and returns the consumer plus stable payment identity. The built-in `blueprintevm` path can
 * reuse the x402 verifier for credentials with the compatible payload shape.
 *
 * Returns authenticated identity if valid, null otherwise.
 * In demo mode, accepts any well-formed Payment header with an identity.
 */
export async function verifyMppCredential(
  authHeader: string,
  config: MppConfig,
  x402Config: X402Config,
  nonceStore?: NonceStore,
  minimumAmount = 1n,
  markNonce = true,
): Promise<VerifiedMppCredential | null> {
  // MPP format: "Payment <method> <base64url-credential>"
  const match = authHeader.match(/^Payment\s+(\S+)\s+(\S+)$/i)
  if (!match) return null

  const [, rawMethod] = match
  const method = rawMethod.toLowerCase()
  if (method !== (config.method ?? 'blueprintevm').toLowerCase()) return null

  try {
    const decodedCredential = decodeMppCredential(authHeader)
    if (!decodedCredential) return null
    const { credential: decoded, payload } = decodedCredential

    // Validate common EVM fields before pure credential authentication.
    // BlueprinTEVM carries x402-equivalent token amounts and must cover the
    // same request ceiling as the X-Payment-Signature path.
    const operator = payload.operator ?? payload.to
    if (operator !== undefined) {
      if (typeof operator !== 'string' || operator.toLowerCase() !== x402Config.operatorAddress.toLowerCase()) {
        return null
      }
    }
    const paymentAmount = payload.amount ?? payload.value
    if (paymentAmount !== undefined) {
      const amount = BigInt(String(paymentAmount))
      if (amount <= 0n || (method === 'blueprintevm' && amount < minimumAmount)) return null
    } else if (method === 'blueprintevm') {
      return null
    }
    if (payload.nonce !== undefined) BigInt(String(payload.nonce))
    if (payload.expiry !== undefined && BigInt(String(payload.expiry)) < BigInt(Math.floor(Date.now() / 1000))) {
      return null
    }

    const blueprintevmKey = method === 'blueprintevm'
      ? blueprintevmNonceKey(payload)
      : undefined
    if (markNonce && blueprintevmKey && nonceStore?.hasSeen && await nonceStore.hasSeen(blueprintevmKey)) {
      return null
    }

    let authenticated: MppAuthenticatedCredential | null = null
    if (config.authenticateCredential) {
      authenticated = await config.authenticateCredential(payload, { method, credential: decoded })
    } else if (config.verifySigner) {
      const consumerId = await config.verifySigner(payload, { method, credential: decoded })
      authenticated = typeof consumerId === 'string' && consumerId.length > 0
        ? {
            consumerId,
            paymentIdentity: legacyMppPaymentIdentity(method, payload, decoded),
          }
        : null
    } else if (method === 'blueprintevm' && x402Config.verifySigner && payload.commitment) {
      const verified = await x402Config.verifySigner(payload, {
        protocolVersion: x402Config.paymentProtocolVersion ?? (x402Config.paymentOperations ? 2 : 1),
      })
      const paymentIdentity = blueprintevmNonceKey(payload) ?? stableJson(payload)
      authenticated = verified
        ? { consumerId: String(payload.commitment), paymentIdentity }
        : null
    } else if (x402Config.demoMode) {
      const identity = payload.commitment ?? payload.from
      if (typeof identity !== 'string' || identity.length === 0) return null
      const paymentIdentity = method === 'blueprintevm'
        ? blueprintevmNonceKey(payload) ?? stableJson(payload)
        : ''
      if (!paymentIdentity) return null
      authenticated = { consumerId: identity, paymentIdentity }
    } else {
      return null
    }
    if (
      !authenticated ||
      typeof authenticated.consumerId !== 'string' ||
      authenticated.consumerId.length === 0 ||
      typeof authenticated.paymentIdentity !== 'string' ||
      authenticated.paymentIdentity.length === 0
    ) return null

    const replayKey = method === 'blueprintevm'
      ? blueprintevmKey ??
        await mppPaymentOperationId(method, authenticated.paymentIdentity)
      : await mppPaymentOperationId(method, authenticated.paymentIdentity)
    if (!replayKey) return null
    if (markNonce && !blueprintevmKey && nonceStore?.hasSeen && await nonceStore.hasSeen(replayKey)) return null

    if (nonceStore && markNonce) {
      const expiry = payload.expiry === undefined
        ? BigInt(Math.floor(Date.now() / 1000) + 3600)
        : BigInt(String(payload.expiry))
      const ttl = nonceTtlSeconds(expiry)
      if (ttl === undefined) return null
      const claimed = await claimStoredNonce(nonceStore, replayKey, ttl)
      if (!claimed) return null
    }

    return { ...authenticated, replayKey }
  } catch {
    return null
  }
}

/**
 * Verify an MPP credential using the 0.7.1 public return shape.
 * Rich durable callers use verifyMppCredential instead.
 */
export async function verifyMpp(
  authHeader: string,
  config: MppConfig,
  x402Config: X402Config,
  nonceStore?: NonceStore,
  minimumAmount = 1n,
  markNonce = true,
): Promise<string | null> {
  const authenticated = await verifyMppCredential(
    authHeader,
    config,
    x402Config,
    nonceStore,
    minimumAmount,
    markNonce,
  )
  return authenticated?.consumerId ?? null
}

/**
 * Default API key verifier — accepts any `sk_agent_*` key (demo mode).
 * Override in GatewayConfig.verifyApiKey for production.
 */
export async function defaultVerifyApiKey(
  authHeader: string,
): Promise<ApiKeyInfo | null> {
  if (!authHeader.startsWith('Bearer sk_agent_')) return null
  const key = authHeader.slice(7)
  return {
    keyId: key.slice(0, 16),
    consumerId: `apikey:${key.slice(0, 16)}`,
  }
}
