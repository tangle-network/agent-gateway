import type { X402Config, MppConfig, ApiKeyInfo } from './types'
import type { NonceStore } from './nonce-store'

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

    // Reject zero-amount payments
    if (amount <= 0n) return null

    // Reject replayed nonces
    const nonceKey = `${raw.commitment}:${nonce.toString()}`
    if (nonceStore) {
      if (await nonceStore.hasSeen(nonceKey)) return null
      // Mark seen with TTL matching the expiry window (max 1 hour)
      const ttl = Math.min(Number(expiry) - Math.floor(Date.now() / 1000), 3600)
      await nonceStore.markSeen(nonceKey, Math.max(ttl, 60))
    }

    if (config.verifySigner) {
      const verified = await config.verifySigner(raw)
      if (!verified) return null
    } else if (!config.demoMode) {
      return null
    }

    return raw.commitment
  } catch {
    return null
  }
}

/**
 * Verify MPP (Machine Payments Protocol) Authorization: Payment header.
 *
 * MPP uses `Authorization: Payment <method> <credential>` format where
 * the credential is a base64url-encoded JSON wrapping the same EIP-3009
 * payment payload that x402 uses. This means existing x402 wallets work
 * unchanged over the MPP wire format.
 *
 * Returns the signer address if valid, null otherwise.
 * In demo mode, accepts any well-formed Payment header.
 */
export async function verifyMpp(
  authHeader: string,
  _config: MppConfig,
  x402Config: X402Config,
): Promise<string | null> {
  // MPP format: "Payment <method> <base64url-credential>"
  const match = authHeader.match(/^Payment\s+(\S+)\s+(\S+)$/i)
  if (!match) return null

  const [, , credentialB64] = match

  try {
    // Decode base64url credential → JSON with the same EIP-3009 payload
    const decoded = Buffer.from(credentialB64, 'base64url').toString('utf-8')
    const credential = JSON.parse(decoded)

    // The credential payload wraps the same fields x402 uses
    const payload = credential.payload ?? credential
    if (!payload.commitment && !payload.from) return null

    // Validate operator match (same as x402)
    const operator = payload.operator ?? payload.to
    if (operator && operator.toLowerCase() !== x402Config.operatorAddress.toLowerCase()) return null

    // Validate bigint fields if present
    if (payload.amount) BigInt(payload.amount)
    if (payload.nonce) BigInt(payload.nonce)

    return payload.commitment ?? payload.from ?? null
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
