import { describe, it, expect, beforeEach } from 'vitest'
import { verifyX402, verifyMpp, defaultVerifyApiKey } from '../src/verify'
import { MemoryNonceStore } from '../src/nonce-store'
import type { X402Config, MppConfig } from '../src/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const baseConfig: X402Config = {
  operatorAddress,
  chainId: 3799,
  demoMode: true,
}

function buildSpendAuth(overrides: Record<string, unknown> = {}): string {
  const now = Math.floor(Date.now() / 1000)
  return JSON.stringify({
    commitment: '0xCommitmentAlice',
    signature: '0xSignatureBytes',
    amount: '20000',
    nonce: '42',
    operator: operatorAddress,
    expiry: String(now + 600),
    ...overrides,
  })
}

describe('verifyX402', () => {
  it('accepts a well-formed SpendAuth in demo mode and returns the commitment', async () => {
    const result = await verifyX402(buildSpendAuth(), baseConfig)
    expect(result).toBe('0xCommitmentAlice')
  })

  it('rejects malformed JSON — regression: parse crash must return null, not throw', async () => {
    expect(await verifyX402('not-json', baseConfig)).toBeNull()
    expect(await verifyX402('{broken', baseConfig)).toBeNull()
  })

  it('rejects missing required fields', async () => {
    expect(await verifyX402(JSON.stringify({ signature: '0x', amount: '1' }), baseConfig)).toBeNull()
    expect(await verifyX402(JSON.stringify({ commitment: '0xA', amount: '1' }), baseConfig)).toBeNull()
    expect(await verifyX402(JSON.stringify({ commitment: '0xA', signature: '0x' }), baseConfig)).toBeNull()
  })

  it('rejects operator mismatch — regression: consumer must not be able to pay the wrong operator', async () => {
    const wrongOp = buildSpendAuth({ operator: '0x2222222222222222222222222222222222222222' })
    expect(await verifyX402(wrongOp, baseConfig)).toBeNull()
  })

  it('rejects zero-amount payments — regression: free rides bypass billing', async () => {
    expect(await verifyX402(buildSpendAuth({ amount: '0' }), baseConfig)).toBeNull()
  })

  it('rejects expired payments — regression: forever-valid sigs enable drained-wallet attacks', async () => {
    const expired = buildSpendAuth({ expiry: String(Math.floor(Date.now() / 1000) - 10) })
    expect(await verifyX402(expired, baseConfig)).toBeNull()
  })

  it('rejects nonce replay — regression: double-spend of a single signed payment', async () => {
    const nonceStore = new MemoryNonceStore()
    const payload = buildSpendAuth({ nonce: '99' })

    const first = await verifyX402(payload, baseConfig, nonceStore)
    expect(first).toBe('0xCommitmentAlice')

    const second = await verifyX402(payload, baseConfig, nonceStore)
    expect(second).toBeNull()
  })

  it('isolates nonces per commitment — regression: commitment-less nonce tracking lets Alice replay Bob\'s nonce', async () => {
    const nonceStore = new MemoryNonceStore()
    const aliceNonce = buildSpendAuth({ commitment: '0xAlice', nonce: '1' })
    const bobNonce = buildSpendAuth({ commitment: '0xBob', nonce: '1' })

    expect(await verifyX402(aliceNonce, baseConfig, nonceStore)).toBe('0xAlice')
    expect(await verifyX402(bobNonce, baseConfig, nonceStore)).toBe('0xBob')
  })

  it('calls config.verifySigner in non-demo mode and rejects on false — regression: signature skipping in production', async () => {
    const calls: Array<Record<string, unknown>> = []
    const config: X402Config = {
      ...baseConfig,
      demoMode: false,
      verifySigner: async (payload) => {
        calls.push(payload)
        return false
      },
    }
    expect(await verifyX402(buildSpendAuth(), config)).toBeNull()
    expect(calls).toHaveLength(1)
    expect(calls[0].commitment).toBe('0xCommitmentAlice')
  })

  it('calls config.verifySigner and accepts on true', async () => {
    const config: X402Config = {
      ...baseConfig,
      demoMode: false,
      verifySigner: async () => true,
    }
    expect(await verifyX402(buildSpendAuth(), config)).toBe('0xCommitmentAlice')
  })
})

describe('verifyMpp', () => {
  const mppConfig: MppConfig = { realm: 'agents.tangle.tools', method: 'blueprintevm' }

  function buildCredential(payload: Record<string, unknown>): string {
    const json = JSON.stringify({ payload })
    const b64 = Buffer.from(json).toString('base64url')
    return `Payment blueprintevm ${b64}`
  }

  it('parses a valid Payment header and returns the signer', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: operatorAddress, amount: '1000', nonce: '5' })
    expect(await verifyMpp(header, mppConfig, baseConfig)).toBe('0xAlice')
  })

  it('falls back to the `from` field when no `commitment` present — regression: EIP-3009 wallets expose `from` only', async () => {
    const header = buildCredential({ from: '0xWallet', to: operatorAddress, value: '1000' })
    expect(await verifyMpp(header, mppConfig, baseConfig)).toBe('0xWallet')
  })

  it('rejects malformed Payment header shape', async () => {
    expect(await verifyMpp('Bearer sk_agent_123', mppConfig, baseConfig)).toBeNull()
    expect(await verifyMpp('Payment', mppConfig, baseConfig)).toBeNull()
    expect(await verifyMpp('Payment blueprintevm', mppConfig, baseConfig)).toBeNull()
  })

  it('rejects bad base64url — regression: decode crash must return null', async () => {
    expect(await verifyMpp('Payment blueprintevm !@#$not-b64$#@!', mppConfig, baseConfig)).toBeNull()
  })

  it('rejects operator mismatch', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: '0xWrongOp', amount: '1000' })
    expect(await verifyMpp(header, mppConfig, baseConfig)).toBeNull()
  })

  it('rejects non-numeric amount/nonce — regression: BigInt throw should become null, not crash', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: operatorAddress, amount: 'not-a-number' })
    expect(await verifyMpp(header, mppConfig, baseConfig)).toBeNull()
  })
})

describe('defaultVerifyApiKey', () => {
  it('accepts sk_agent_* bearer keys', async () => {
    const info = await defaultVerifyApiKey('Bearer sk_agent_testkey123')
    expect(info).not.toBeNull()
    expect(info!.consumerId).toMatch(/^apikey:sk_agent_/)
  })

  it('rejects wrong prefix — regression: sk_ ≠ sk_agent_, must not confuse key spaces', async () => {
    expect(await defaultVerifyApiKey('Bearer sk_testkey')).toBeNull()
    expect(await defaultVerifyApiKey('Bearer ak_testkey')).toBeNull()
  })

  it('rejects non-bearer schemes', async () => {
    expect(await defaultVerifyApiKey('Basic sk_agent_123')).toBeNull()
    expect(await defaultVerifyApiKey('sk_agent_123')).toBeNull()
  })
})
