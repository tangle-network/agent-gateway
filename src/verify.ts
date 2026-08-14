import type { X402Config, MppConfig, ApiKeyInfo, GatewayConfig } from './types'
import type { NonceStore } from './nonce-store'

/** Return the canonical opaque nonce key used by the final payment claim. */
export function mppReplayNonceKey(authHeader: string): string | undefined {
  const match = authHeader.match(/^Payment\s+(\S+)\s+(\S+)$/i)
  if (!match) return undefined
  const [, method, credentialB64] = match
  try {
    const decoded = Buffer.from(credentialB64, 'base64url').toString('utf-8')
    const credential = JSON.parse(decoded) as Record<string, unknown>
    const nested = credential.payload
    const payload = nested && typeof nested === 'object' && !Array.isArray(nested)
      ? nested as Record<string, unknown>
      : credential
    return canonicalMppNonceKey(method, payload)
  } catch {
    return undefined
  }
}

function canonicalMppNonceKey(method: string, payload: Record<string, unknown>): string | undefined {
  if (payload.nonce === undefined) return undefined
  const nonce = BigInt(String(payload.nonce)).toString()
  const commitment = payload.commitment
  // BlueprinTEVM carries the same SpendAuth identity as x402. Keep one
  // namespace so a credential cannot cross the two HTTP transports.
  if (method.toLowerCase() === 'blueprintevm' && typeof commitment === 'string' && commitment.length > 0) {
    return `${commitment.toLowerCase()}:${nonce}`
  }
  return `mpp:${method.toLowerCase()}:${String(payload.commitment ?? payload.from ?? 'unknown').toLowerCase()}:${nonce}`
}

/** Pure capability checks shared by discovery and every request protocol. */
export function isApiKeyAuthEnabled(
  config: Pick<GatewayConfig, 'verifyApiKey' | 'x402'>,
): boolean {
  return config.verifyApiKey !== undefined || config.x402.demoMode === true
}

/** MPP is enabled only when a real verifier or explicit demo mode exists. */
export function isMppAuthEnabled(
  config: Pick<GatewayConfig, 'mpp' | 'x402'>,
): boolean {
  const method = config.mpp?.method ?? 'blueprintevm'
  return Boolean(
    config.mpp &&
      (config.mpp.verifySigner !== undefined ||
        (method === 'blueprintevm' && config.x402.verifySigner !== undefined) ||
        config.x402.demoMode === true),
  )
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
    if (amount < minimumAmount || minimumAmount <= 0n) return null

    const nonceKey = `${String(raw.commitment).toLowerCase()}:${nonce.toString()}`
    if (nonceStore && await nonceStore.hasSeen(nonceKey)) return null

    if (config.verifySigner) {
      const verified = await config.verifySigner(raw, {
        protocolVersion: config.paymentProtocolVersion ?? (config.paymentOperations ? 2 : 1),
      })
      if (!verified) return null
    } else if (!config.demoMode) {
      return null
    }

    // Check and mark only after the signature is accepted. Otherwise an
    // invalid request can burn a valid payer nonce and deny the real request.
    if (nonceStore && markNonce) {
      // Mark seen with TTL matching the expiry window (max 1 hour)
      const ttl = Math.min(Number(expiry) - Math.floor(Date.now() / 1000), 3600)
      const claimed = await nonceStore.claim(nonceKey, Math.max(ttl, 60))
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
 * credential is method-specific; `MppConfig.verifySigner` owns verification
 * and returns the consumer identity. The built-in `blueprintevm` path can
 * reuse the x402 verifier for credentials with the compatible payload shape.
 *
 * Returns the signer address if valid, null otherwise.
 * In demo mode, accepts any well-formed Payment header with an identity.
 */
export async function verifyMpp(
  authHeader: string,
  config: MppConfig,
  x402Config: X402Config,
  nonceStore?: NonceStore,
  minimumAmount = 1n,
  markNonce = true,
): Promise<string | null> {
  // MPP format: "Payment <method> <base64url-credential>"
  const match = authHeader.match(/^Payment\s+(\S+)\s+(\S+)$/i)
  if (!match) return null

  const [, method, credentialB64] = match
  if (config.method && method !== config.method) return null

  try {
    if (!/^[A-Za-z0-9_-]+$/.test(credentialB64)) return null
    const decoded = Buffer.from(credentialB64, 'base64url').toString('utf-8')
    let payload: Record<string, unknown> = {}
    try {
      const credential = JSON.parse(decoded) as unknown
      if (credential && typeof credential === 'object' && !Array.isArray(credential)) {
        const nested = (credential as Record<string, unknown>).payload
        payload =
          nested && typeof nested === 'object' && !Array.isArray(nested)
            ? (nested as Record<string, unknown>)
            : (credential as Record<string, unknown>)
      }
    } catch {
      // Method-specific verifiers may accept a non-JSON credential format.
    }

    // Validate common EVM fields before a production verifier can reserve or
    // settle funds. BlueprinTEVM carries x402-equivalent token amounts and
    // must cover the same request ceiling as the X-Payment-Signature path.
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

    const nonceKey = nonceStore ? canonicalMppNonceKey(method, payload) ?? null : null
    if (nonceKey && await nonceStore!.hasSeen(nonceKey)) return null

    let consumerId: string | null = null
    if (config.verifySigner) {
      consumerId = await config.verifySigner(payload, { method, credential: decoded })
    } else if (method === 'blueprintevm' && x402Config.verifySigner && payload.commitment) {
      const verified = await x402Config.verifySigner(payload, {
        protocolVersion: x402Config.paymentProtocolVersion ?? (x402Config.paymentOperations ? 2 : 1),
      })
      consumerId = verified ? String(payload.commitment) : null
    } else if (x402Config.demoMode) {
      const identity = payload.commitment ?? payload.from
      if (typeof identity !== 'string' || identity.length === 0) return null
      consumerId = identity
    } else {
      return null
    }
    if (!consumerId) return null

    if (nonceStore && payload.nonce !== undefined && markNonce) {
      const expiry = payload.expiry === undefined
        ? Math.floor(Date.now() / 1000) + 3600
        : Number(payload.expiry)
      const ttl = Math.min(expiry - Math.floor(Date.now() / 1000), 3600)
      const claimed = await nonceStore.claim(nonceKey!, Math.max(ttl, 60))
      if (!claimed) return null
    }

    return consumerId
  } catch {
    return null
  }
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
