import { describe, it, expect, beforeEach } from 'vitest'
import { verifyX402, verifyMppCredential, defaultVerifyApiKey } from '../src/verify'
import { MemoryNonceStore, type NonceStore } from '../src/nonce-store'
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

  it('rejects a request-specific underpayment before calling the production verifier', async () => {
    let calls = 0
    const config: X402Config = {
      ...baseConfig,
      demoMode: false,
      verifySigner: async () => {
        calls += 1
        return true
      },
    }

    expect(await verifyX402(buildSpendAuth({ amount: '19999' }), config, undefined, 20000n)).toBeNull()
    expect(calls).toBe(0)
  })

  it('accepts a positive authorization when the requested agent price is zero', async () => {
    expect(await verifyX402(buildSpendAuth({ amount: '1' }), baseConfig, undefined, 0n)).toBe('0xCommitmentAlice')
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

  it('rejects replay for the full signed lifetime beyond one hour', async () => {
    const nonceStore = new MemoryNonceStore()
    const payload = buildSpendAuth({
      nonce: '102',
      expiry: String(Math.floor(Date.now() / 1000) + 7_200),
    })

    expect(await verifyX402(payload, baseConfig, nonceStore)).toBe('0xCommitmentAlice')
    expect(await verifyX402(payload, baseConfig, nonceStore)).toBeNull()
  })

  it('retains a nonce until its signed expiry, beyond the old one-hour cap', async () => {
    const ttls: number[] = []
    const nonceStore: NonceStore = {
      hasSeen: async () => false,
      claim: async (_nonce, ttlSeconds) => {
        ttls.push(ttlSeconds)
        return true
      },
    }
    const now = Math.floor(Date.now() / 1000)
    const result = await verifyX402(
      buildSpendAuth({ nonce: '102', expiry: String(now + 7200) }),
      baseConfig,
      nonceStore,
    )

    expect(result).toBe('0xCommitmentAlice')
    expect(ttls[0]).toBeGreaterThan(3600)
    expect(ttls[0]).toBeLessThanOrEqual(7200)
  })

  it('fails closed for a 0.7.1 custom nonce store without atomic claims', async () => {
    const seen = new Set<string>()
    const nonceStore = {
      hasSeen: async (nonce: string) => seen.has(nonce),
      markSeen: async (nonce: string) => { seen.add(nonce) },
    } as unknown as NonceStore

    const payload = buildSpendAuth({ nonce: '101' })
    expect(await verifyX402(payload, baseConfig, nonceStore)).toBeNull()
    expect(await verifyX402(payload, baseConfig, nonceStore)).toBeNull()
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

  it('does not burn a nonce when signature verification rejects — regression: invalid traffic must not deny a valid payment', async () => {
    const nonceStore = new MemoryNonceStore()
    const payload = buildSpendAuth({ nonce: '100' })
    const rejectedConfig: X402Config = {
      ...baseConfig,
      demoMode: false,
      verifySigner: async () => false,
    }
    expect(await verifyX402(payload, rejectedConfig, nonceStore)).toBeNull()

    const acceptedConfig: X402Config = {
      ...rejectedConfig,
      verifySigner: async () => true,
    }
    expect(await verifyX402(payload, acceptedConfig, nonceStore)).toBe('0xCommitmentAlice')
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
    expect(await verifyMppCredential(header, mppConfig, baseConfig)).toMatchObject({
      consumerId: '0xAlice',
      replayKey: '0xalice:5',
    })
  })

  it('requires and calls a production MPP verifier, then rejects nonce replay', async () => {
    const header = buildCredential({
      commitment: '0xAlice',
      operator: operatorAddress,
      amount: '1000',
      nonce: '6',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    })
    const seen: Array<{ method: string; credential: string }> = []
    const config: MppConfig = {
      ...mppConfig,
      authenticateCredential: async (_payload, context) => {
        seen.push(context)
        return { consumerId: 'mpp:alice', paymentIdentity: 'payment:alice:6' }
      },
    }
    const productionX402: X402Config = { ...baseConfig, demoMode: false }
    const nonceStore = new MemoryNonceStore()

    expect(await verifyMppCredential(header, config, productionX402, nonceStore)).toMatchObject({
      consumerId: 'mpp:alice',
      replayKey: '0xalice:6',
    })
    expect(await verifyMppCredential(header, config, productionX402, nonceStore)).toBeNull()
    expect(seen).toHaveLength(1)
    expect(seen[0].method).toBe('blueprintevm')
    expect(seen[0].credential).toContain('commitment')
  })

  it('preserves Unicode in a web-compatible Base64URL credential', async () => {
    const header = buildCredential({
      commitment: '0xAlice',
      operator: operatorAddress,
      amount: '1000',
      nonce: '601',
      note: 'こんにちは 👋',
    })
    let credential = ''
    const result = await verifyMppCredential(header, {
      ...mppConfig,
      authenticateCredential: async (_payload, context) => {
        credential = context.credential
        return { consumerId: 'mpp:alice', paymentIdentity: 'payment:alice:601' }
      },
    }, { ...baseConfig, demoMode: false })

    expect(result?.consumerId).toBe('mpp:alice')
    expect(credential).toContain('こんにちは 👋')
  })

  it('adapts the 0.7.1 mpp.verifySigner contract to a stable payment identity', async () => {
    const header = buildCredential({
      commitment: '0xAlice',
      operator: operatorAddress,
      amount: '1000',
      nonce: '7',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }).replace('Payment blueprintevm ', 'Payment stripe ')
    const legacyConfig = {
      ...mppConfig,
      method: 'stripe',
      verifySigner: async (_payload: Record<string, unknown>, context: { method: string; credential: string }) => {
        expect(context.method).toBe('stripe')
        expect(context.credential).toContain('commitment')
        return 'mpp:alice'
      },
    }

    await expect(verifyMppCredential(
      header,
      legacyConfig,
      { ...baseConfig, demoMode: false },
    )).resolves.toMatchObject({
      consumerId: 'mpp:alice',
      replayKey: expect.stringMatching(/^mpp:stripe:[0-9a-f]{64}$/),
    })
  })

  it('shares the x402 nonce authority with equivalent BlueprinTEVM credentials', async () => {
    const nonceStore = new MemoryNonceStore()
    const payload = {
      commitment: '0xAlice',
      signature: '0xSignatureBytes',
      operator: operatorAddress,
      amount: '20000',
      nonce: '01',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }
    expect(await verifyX402(JSON.stringify({ ...payload, nonce: '1' }), baseConfig, nonceStore)).toBe('0xAlice')
    expect(await verifyMppCredential(
      buildCredential({ ...payload, commitment: '0xALICE' }),
      mppConfig,
      baseConfig,
      nonceStore,
    )).toBeNull()
  })

  it('rejects an underfunded blueprintevm credential before its verifier can reserve funds', async () => {
    let calls = 0
    const header = buildCredential({
      commitment: '0xAlice',
      operator: operatorAddress,
      amount: '19999',
      nonce: '8',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    })
    const config: MppConfig = {
      ...mppConfig,
      authenticateCredential: async () => {
        calls += 1
        return { consumerId: 'mpp:alice', paymentIdentity: 'underfunded' }
      },
    }

    expect(await verifyMppCredential(header, config, { ...baseConfig, demoMode: false }, undefined, 20000n)).toBeNull()
    expect(calls).toBe(0)
  })

  it('normalizes MPP method casing before applying the configured payment ceiling', async () => {
    const underfunded = buildCredential({
      commitment: '0xAlice',
      operator: operatorAddress,
      amount: '19999',
      nonce: '9',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }).replace('Payment blueprintevm ', 'Payment BLUEPRINTEVM ')

    expect(await verifyMppCredential(
      underfunded,
      { realm: 'agents.tangle.tools' },
      baseConfig,
      undefined,
      20000n,
    )).toBeNull()
    expect(await verifyMppCredential(
      underfunded,
      mppConfig,
      baseConfig,
      undefined,
      1n,
    )).toMatchObject({ consumerId: '0xAlice', replayKey: '0xalice:9' })
  })

  it('rejects MPP in production when no method verifier is configured', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: operatorAddress })
    const productionX402: X402Config = { ...baseConfig, demoMode: false }
    expect(await verifyMppCredential(header, mppConfig, productionX402)).toBeNull()
  })

  it('does not reuse the x402 verifier for a different MPP method', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: operatorAddress })
      .replace('Payment blueprintevm ', 'Payment stripe ')
    const productionX402: X402Config = {
      ...baseConfig,
      demoMode: false,
      verifySigner: async () => true,
    }
    expect(await verifyMppCredential(header, { ...mppConfig, method: 'stripe' }, productionX402)).toBeNull()
  })

  it('falls back to the `from` field when no `commitment` present — regression: EIP-3009 wallets expose `from` only', async () => {
    const header = buildCredential({ from: '0xWallet', to: operatorAddress, value: '1000' })
    expect(await verifyMppCredential(header, mppConfig, baseConfig)).toMatchObject({
      consumerId: '0xWallet',
    })
  })

  it('rejects malformed Payment header shape', async () => {
    expect(await verifyMppCredential('Bearer sk_agent_123', mppConfig, baseConfig)).toBeNull()
    expect(await verifyMppCredential('Payment', mppConfig, baseConfig)).toBeNull()
    expect(await verifyMppCredential('Payment blueprintevm', mppConfig, baseConfig)).toBeNull()
  })

  it('rejects bad base64url — regression: decode crash must return null', async () => {
    expect(await verifyMppCredential('Payment blueprintevm !@#$not-b64$#@!', mppConfig, baseConfig)).toBeNull()
  })

  it('rejects operator mismatch', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: '0xWrongOp', amount: '1000' })
    expect(await verifyMppCredential(header, mppConfig, baseConfig)).toBeNull()
  })

  it('rejects non-numeric amount/nonce — regression: BigInt throw should become null, not crash', async () => {
    const header = buildCredential({ commitment: '0xAlice', operator: operatorAddress, amount: 'not-a-number' })
    expect(await verifyMppCredential(header, mppConfig, baseConfig)).toBeNull()
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
