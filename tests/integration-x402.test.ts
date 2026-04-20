/**
 * End-to-end integration test — x402 SpendAuth payment flow.
 *
 * This is NOT a unit test. It proves the full loop:
 *
 *   1. Consumer (viem wallet) signs an EIP-712 SpendAuth authorizing payment
 *      to a specific operator, with a nonce + expiry.
 *   2. Consumer POSTs to the gateway with the serialized SpendAuth in
 *      X-Payment-Signature.
 *   3. Gateway calls verifyX402() which:
 *      (a) parses the header
 *      (b) checks operator match, amount > 0, expiry not passed
 *      (c) rejects replayed nonces via NonceStore
 *      (d) invokes config.verifySigner (production path) which recovers the
 *          EIP-712 signer address and asserts it matches the commitment
 *   4. Gateway resolves the agent, rate-limits the consumer, filters for
 *      injection, gets a sandbox, streams the response, records usage,
 *      and fires settlePayment.
 *   5. Consumer parses the SSE stream back to a string.
 *
 * Every step uses real code — real signatures (not fixtures), real Hono
 * dispatch, real ReadableStream parsing, real state mutations across
 * nonceStore + rateLimitStore + recordUsage callbacks.
 *
 * A replay of the same signed SpendAuth against a second request MUST be
 * rejected — this is the property that breaks if you get payment
 * verification wrong.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { Hono } from 'hono'
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts'
import { recoverTypedDataAddress, type Hex } from 'viem'
import { createAgentGateway } from '../src/middleware'
import type {
  AgentMeta,
  SandboxBox,
  SandboxStreamEvent,
  GatewayUsageEvent,
  PaymentResult,
} from '../src/types'
import { MemoryNonceStore } from '../src/nonce-store'
import { MemoryRateLimitStore } from '../src/rate-limit'

// ----- Domain constants (mirror the Tangle ShieldedCredits contract shape) -----

const CHAIN_ID = 3799
const OPERATOR_PRIVATE_KEY = generatePrivateKey()
const OPERATOR_ADDRESS = privateKeyToAccount(OPERATOR_PRIVATE_KEY).address
const CREDITS_ADDRESS: Hex = '0x00000000000000000000000000000000DeaDBeef'

const domain = {
  name: 'ShieldedCredits',
  version: '1',
  chainId: CHAIN_ID,
  verifyingContract: CREDITS_ADDRESS,
} as const

const types = {
  SpendAuth: [
    { name: 'operator', type: 'address' },
    { name: 'amount', type: 'uint256' },
    { name: 'nonce', type: 'uint256' },
    { name: 'expiry', type: 'uint256' },
  ],
} as const

// ----- Fixture builders -----

interface SpendAuthPayload {
  commitment: string
  signature: string
  operator: string
  amount: string
  nonce: string
  expiry: string
}

/**
 * Sign a SpendAuth with a consumer's real private key. Returns the
 * serialized JSON payload the client would send in X-Payment-Signature.
 *
 * This is the exact shape the gateway expects; no shortcuts.
 */
async function signSpendAuth(params: {
  consumerPrivateKey: Hex
  amount: bigint
  nonce: bigint
  expirySeconds?: number
}): Promise<SpendAuthPayload> {
  const account = privateKeyToAccount(params.consumerPrivateKey)
  const expiry = BigInt(Math.floor(Date.now() / 1000) + (params.expirySeconds ?? 600))

  const message = {
    operator: OPERATOR_ADDRESS,
    amount: params.amount,
    nonce: params.nonce,
    expiry,
  }

  const signature = await account.signTypedData({
    domain,
    types,
    primaryType: 'SpendAuth',
    message,
  })

  return {
    commitment: account.address,
    signature,
    operator: OPERATOR_ADDRESS,
    amount: params.amount.toString(),
    nonce: params.nonce.toString(),
    expiry: expiry.toString(),
  }
}

/** Real on-chain-style verifier — recovers address from signature and compares to commitment. */
async function verifySignerOnChain(payload: Record<string, unknown>): Promise<boolean> {
  try {
    const recovered = await recoverTypedDataAddress({
      domain,
      types,
      primaryType: 'SpendAuth',
      message: {
        operator: payload.operator as Hex,
        amount: BigInt(payload.amount as string),
        nonce: BigInt(payload.nonce as string),
        expiry: BigInt(payload.expiry as string),
      },
      signature: payload.signature as Hex,
    })
    // Commitment MUST match the signer — this is the invariant the whole
    // payment flow depends on. Reject if the signer address doesn't match
    // what the consumer claims is their commitment.
    return recovered.toLowerCase() === (payload.commitment as string).toLowerCase()
  } catch {
    return false
  }
}

// ----- Sandbox that emits a deterministic response -----

class ReplySandbox implements SandboxBox {
  constructor(private chunks: string[]) {}
  async *streamPrompt(): AsyncIterable<SandboxStreamEvent> {
    for (const delta of this.chunks) {
      yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta } }
    }
  }
}

// ----- Harness -----

interface Harness {
  app: Hono
  consumerPrivateKey: Hex
  consumerAddress: Hex
  usage: GatewayUsageEvent[]
  settlements: Array<{ payment: PaymentResult; cost: number }>
  verifyCalls: number
}

function buildHarness(chunks = ['Hello', ', ', 'world!']): Harness {
  const consumerPrivateKey = generatePrivateKey()
  const consumerAddress = privateKeyToAccount(consumerPrivateKey).address
  const usage: GatewayUsageEvent[] = []
  const settlements: Array<{ payment: PaymentResult; cost: number }> = []
  let verifyCalls = 0

  const agent: AgentMeta = {
    id: 'agent_production',
    ownerId: 'user_owner',
    slug: 'production-agent',
    systemPrompt: 'You are the production agent under test.',
    pricePerTokenUsd: 0.00005,
    platformFeePercent: 0.2,
    sandboxEndpoint: null,
    remoteSandboxId: null,
    remoteBearerToken: null,
    enabled: true,
  }

  const gw = createAgentGateway({
    resolveAgent: async (slug) => (slug === agent.slug ? agent : null),
    getSandbox: async () => new ReplySandbox(chunks),
    recordUsage: async (evt) => { usage.push(evt) },
    settlePayment: async (payment, cost) => { settlements.push({ payment, cost }) },
    x402: {
      operatorAddress: OPERATOR_ADDRESS,
      chainId: CHAIN_ID,
      creditsAddress: CREDITS_ADDRESS,
      demoMode: false, // PRODUCTION PATH — verifySigner is authoritative
      verifySigner: async (payload) => {
        verifyCalls += 1
        return verifySignerOnChain(payload)
      },
    },
    nonceStore: new MemoryNonceStore(),
    rateLimitStore: new MemoryRateLimitStore(),
  })

  const app = new Hono()
  app.route('/v1/agents', gw)
  return {
    app,
    consumerPrivateKey,
    consumerAddress,
    usage,
    settlements,
    get verifyCalls() { return verifyCalls },
  } as unknown as Harness
}

async function drainSseToText(res: Response): Promise<string> {
  const reader = res.body!.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let text = ''

  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value)
    const frames = buffer.split('\n\n')
    buffer = frames.pop() ?? ''
    for (const frame of frames) {
      const dataLine = frame.split('\n').find((l) => l.startsWith('data: '))
      if (!dataLine) continue
      const payload = dataLine.slice(6).trim()
      if (!payload || payload === '[DONE]') continue
      try {
        const parsed = JSON.parse(payload) as { choices?: Array<{ delta?: { content?: string } }> }
        const delta = parsed.choices?.[0]?.delta?.content
        if (typeof delta === 'string') text += delta
      } catch {
        // non-JSON frames skipped
      }
    }
  }
  return text
}

// ============================================================================

describe('x402 end-to-end — real EIP-712 signatures, real gateway, real sandbox', () => {
  let harness: Harness

  beforeEach(() => { harness = buildHarness() })

  it('happy path: consumer signs → gateway verifies signer address → sandbox streams → settlement fires', async () => {
    const spendAuth = await signSpendAuth({
      consumerPrivateKey: harness.consumerPrivateKey,
      amount: 20000n,
      nonce: 1n,
    })

    const res = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': JSON.stringify(spendAuth),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'ping' }] }),
    })

    // Transport invariants
    expect(res.status).toBe(200)
    expect(res.headers.get('Content-Type')).toMatch(/text\/event-stream/)
    expect(res.headers.get('X-Payment-Method')).toBe('x402')
    expect(res.headers.get('X-Request-Id')).toMatch(/^req_[0-9a-f]{32}$/)

    // Content invariants — we get exactly what the sandbox emitted
    const text = await drainSseToText(res)
    expect(text).toBe('Hello, world!')

    // Verification was actually called (this is the on-chain path, not demo)
    expect(harness.verifyCalls).toBe(1)

    // Usage recorded with the recovered signer address as consumer ID
    expect(harness.usage).toHaveLength(1)
    expect(harness.usage[0].consumerId.toLowerCase()).toBe(harness.consumerAddress.toLowerCase())
    expect(harness.usage[0].paymentMethod).toBe('x402')
    expect(harness.usage[0].totalCostUsd).toBeGreaterThan(0)

    // 80/20 split on the fees
    expect(harness.usage[0].ownerEarnedUsd).toBeCloseTo(harness.usage[0].totalCostUsd * 0.8, 10)
    expect(harness.usage[0].platformFeeUsd).toBeCloseTo(harness.usage[0].totalCostUsd * 0.2, 10)

    // Settlement callback fired with the x402 method
    expect(harness.settlements).toHaveLength(1)
    expect(harness.settlements[0].payment.method).toBe('x402')
    expect(harness.settlements[0].cost).toBe(harness.usage[0].totalCostUsd)
  })

  it('rejects a signature for a DIFFERENT operator — regression: consumer must not be able to pay the wrong operator and claim a match', async () => {
    // Sign with a DIFFERENT operator in the payload (tampered). Generate a
    // real address so viem's checksum validation doesn't reject at sign time.
    const otherOperator = privateKeyToAccount(generatePrivateKey()).address
    const account = privateKeyToAccount(harness.consumerPrivateKey)
    const tampered = await account.signTypedData({
      domain,
      types,
      primaryType: 'SpendAuth',
      message: {
        operator: otherOperator, // not our operator
        amount: 20000n,
        nonce: 1n,
        expiry: BigInt(Math.floor(Date.now() / 1000) + 600),
      },
    })

    const payload: SpendAuthPayload = {
      commitment: account.address,
      signature: tampered,
      operator: otherOperator,
      amount: '20000',
      nonce: '1',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }

    const res = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': JSON.stringify(payload),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'ping' }] }),
    })

    // Reject with 402 — operator check in verifyX402 fires BEFORE verifySigner,
    // so verifyCalls stays 0.
    expect(res.status).toBe(402)
    expect(harness.verifyCalls).toBe(0)
    expect(harness.usage).toHaveLength(0)
    expect(harness.settlements).toHaveLength(0)
  })

  it('rejects a signature with a mismatched commitment — regression: impersonation via forged commitment', async () => {
    // Sign with the correct operator but LIE about the commitment
    const realSigner = privateKeyToAccount(harness.consumerPrivateKey)
    const signature = await realSigner.signTypedData({
      domain,
      types,
      primaryType: 'SpendAuth',
      message: {
        operator: OPERATOR_ADDRESS,
        amount: 20000n,
        nonce: 2n,
        expiry: BigInt(Math.floor(Date.now() / 1000) + 600),
      },
    })

    const payload: SpendAuthPayload = {
      // Claim someone ELSE as the commitment while signing with our own key
      commitment: '0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
      signature,
      operator: OPERATOR_ADDRESS,
      amount: '20000',
      nonce: '2',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }

    const res = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': JSON.stringify(payload),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'ping' }] }),
    })

    // verifySigner was called but returned false — recovery address != commitment
    expect(res.status).toBe(402)
    expect(harness.verifyCalls).toBe(1)
    expect(harness.usage).toHaveLength(0)
  })

  it('replay of the same signed SpendAuth is rejected across requests — regression: double-spend of one signed payment', async () => {
    const spendAuth = await signSpendAuth({
      consumerPrivateKey: harness.consumerPrivateKey,
      amount: 20000n,
      nonce: 99n,
    })
    const payloadStr = JSON.stringify(spendAuth)

    // First request succeeds
    const first = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': payloadStr },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'first' }] }),
    })
    expect(first.status).toBe(200)
    await drainSseToText(first)

    // Same exact signed payload → rejected via nonce replay (never reaches verifySigner)
    const replay = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': payloadStr },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'replay' }] }),
    })
    expect(replay.status).toBe(402)

    // Only 1 verification call — replay short-circuits before hitting verifySigner
    expect(harness.verifyCalls).toBe(1)
    // Only 1 successful usage event + settlement
    expect(harness.usage).toHaveLength(1)
    expect(harness.settlements).toHaveLength(1)
  })

  it('expired SpendAuth is rejected — regression: forever-valid sigs enable drain attacks', async () => {
    const spendAuth = await signSpendAuth({
      consumerPrivateKey: harness.consumerPrivateKey,
      amount: 20000n,
      nonce: 5n,
      expirySeconds: -10, // expired 10 seconds ago
    })

    const res = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': JSON.stringify(spendAuth),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'ping' }] }),
    })

    expect(res.status).toBe(402)
    // Expiry check is in verifyX402 BEFORE verifySigner — stays at 0
    expect(harness.verifyCalls).toBe(0)
  })

  it('different consumers with different nonces both succeed — regression: nonce isolation across wallets', async () => {
    const alice = buildHarness()
    // Alice's wallet
    const aliceAuth = await signSpendAuth({
      consumerPrivateKey: alice.consumerPrivateKey,
      amount: 10000n,
      nonce: 1n,
    })

    // Bob's wallet (generates fresh key)
    const bobKey = generatePrivateKey()
    const bobAddr = privateKeyToAccount(bobKey).address
    const bobAuth = await signSpendAuth({
      consumerPrivateKey: bobKey,
      amount: 10000n,
      nonce: 1n, // same nonce, different commitment
    })

    const r1 = await alice.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': JSON.stringify(aliceAuth) },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'alice' }] }),
    })
    expect(r1.status).toBe(200)
    await drainSseToText(r1)

    const r2 = await alice.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': JSON.stringify(bobAuth) },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'bob' }] }),
    })
    expect(r2.status).toBe(200)
    await drainSseToText(r2)

    expect(alice.usage).toHaveLength(2)
    expect(alice.usage[0].consumerId.toLowerCase()).toBe(alice.consumerAddress.toLowerCase())
    expect(alice.usage[1].consumerId.toLowerCase()).toBe(bobAddr.toLowerCase())
  })

  it('zero-amount payment is rejected — regression: free-ride exploit', async () => {
    const spendAuth = await signSpendAuth({
      consumerPrivateKey: harness.consumerPrivateKey,
      amount: 0n, // free ride attempt
      nonce: 42n,
    })

    const res = await harness.app.request('/v1/agents/production-agent/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': JSON.stringify(spendAuth),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'ping' }] }),
    })

    expect(res.status).toBe(402)
    expect(harness.verifyCalls).toBe(0) // amount check fires before verifySigner
    expect(harness.usage).toHaveLength(0)
  })
})
