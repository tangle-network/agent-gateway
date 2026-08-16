import { Hono } from 'hono'
import { describe, expect, it } from 'vitest'
import { InMemoryTaskStore } from '../src/a2a/task-store'
import { A2A_ERROR_CODES, type Task } from '../src/a2a/types'
import { MemoryNonceStore } from '../src/nonce-store'
import { MemoryPaymentOperations } from '../src/payment-operations'
import { MemoryRateLimitStore } from '../src/rate-limit'
import { createAgentGateway } from '../src/middleware'
import { verifyX402 } from '../src/verify'
import type { AgentMeta, GatewayConfig, SandboxBox } from '../src/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'ab'.repeat(32)}`

const agent: AgentMeta = {
  id: 'agent-guards',
  ownerId: 'owner',
  slug: 'guards',
  pricePerTokenUsd: 0.00002,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

function paymentHeader(nonce = '1'): string {
  return JSON.stringify({
    commitment,
    signature: '0xsignature',
    amount: '1000000',
    nonce,
    operator: operatorAddress,
    expiry: String(Math.floor(Date.now() / 1000) + 600),
  })
}

function box(): SandboxBox {
  return {
    async *streamPrompt() {
      yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'ok' } }
      yield {
        type: 'sandbox.usage',
        data: {
          usage: {
            inputTokens: 1,
            outputTokens: 1,
            reasoningTokens: 0,
            toolTokens: 0,
            toolCallCount: 0,
            providerCostUsd: 0.00002,
            budgetEnforced: true,
          },
        },
      }
    },
  }
}

describe('final payment boundary protocol guards', () => {
  it('does not claim payment before the host authorization guard', async () => {
    let claims = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box(),
      recordUsage: async () => undefined,
      authorizeConsumer: async () => ({ allow: false, reason: 'blocked', code: 'blocked' }),
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        authorizePayment: async () => {
          claims += 1
          return true
        },
      },
      nonceStore: new MemoryNonceStore(),
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const response = await app.request('/v1/agents/guards/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('2') },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'hi' }] }),
    })
    expect(response.status).toBe(403)
    expect(claims).toBe(0)
  })

  it('checks an A2A task state before claiming a payment', async () => {
    const taskStore = new InMemoryTaskStore()
    const terminal: Task = {
      kind: 'task',
      id: 'existing-task',
      contextId: 'ctx',
      status: { state: 'completed', timestamp: new Date().toISOString() },
      history: [],
    }
    await taskStore.put(terminal)
    let claims = 0
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      resolveAgent: async () => agent,
      getSandbox: async () => box(),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        authorizePayment: async () => {
          claims += 1
          return true
        },
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore },
    }))

    const response = await app.request('/v1/agents/guards', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('3') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: terminal.id,
            parts: [{ kind: 'text', text: 'retry' }],
          },
        },
      }),
    })
    const body = await response.json() as { error?: { code: number } }
    expect(body.error?.code).toBe(A2A_ERROR_CODES.INVALID_PARAMS)
    expect(claims).toBe(0)
  })

  it('rejects a continuation before it can mutate another caller task', async () => {
    const taskStore = new InMemoryTaskStore()
    const paused: Task = {
      kind: 'task',
      id: 'victim-task',
      contextId: 'victim-context',
      status: { state: 'input-required', timestamp: new Date().toISOString() },
      history: [],
    }
    await taskStore.put(paused)
    let sandboxRuns = 0
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          sandboxRuns += 1
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'work' } }
        },
      }),
      recordUsage: async () => undefined,
      settlePayment: async () => undefined,
      verifyApiKey: async () => ({ consumerId: 'intruder', keyId: 'intruder', scopes: ['chat'] }),
      x402: { operatorAddress, chainId: 1, demoMode: true },
      a2a: {
        taskStore,
        authorizeTaskAccess: async (_task, context) => context.authorization === 'Bearer owner',
      },
    }))

    const response = await app.request('/v1/agents/guards', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer intruder' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: paused.id,
            parts: [{ kind: 'text', text: 'continue as attacker' }],
          },
        },
      }),
    })

    expect(response.status).toBe(403)
    const body = await response.json() as { error?: { code: number } }
    expect(body.error?.code).toBe(A2A_ERROR_CODES.TASK_ACCESS_DENIED)
    expect(sandboxRuns).toBe(0)
    expect(await taskStore.get(paused.id)).toEqual(paused)
  })

  it('keeps a paused task retryable when continuation payment fails', async () => {
    const taskStore = new InMemoryTaskStore()
    const paused: Task = {
      kind: 'task',
      id: 'paused-payment-task',
      contextId: 'paused-context',
      status: { state: 'input-required', timestamp: new Date().toISOString() },
      history: [],
    }
    await taskStore.put(paused)
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      resolveAgent: async () => agent,
      getSandbox: async () => box(),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 1,
        authorizePayment: async () => false,
      },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }))

    const response = await app.request('/v1/agents/guards', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('31') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: paused.id,
            parts: [{ kind: 'text', text: 'retry' }],
          },
        },
      }),
    })

    const body = await response.json() as { error?: { code: number } }
    expect(body.error?.code).toBe(A2A_ERROR_CODES.INTERNAL_ERROR)
    expect(await taskStore.get(paused.id)).toEqual(paused)
  })

  it('shares one canonical nonce authority across mixed verifier versions', async () => {
    const store = new MemoryNonceStore()
    const config = { operatorAddress, chainId: 1, demoMode: true }
    const header = paymentHeader('4')
    const upperHeader = header.replace(commitment, commitment.toUpperCase())

    await expect(verifyX402(header, config, store, 1n, false)).resolves.toBe(commitment)
    await expect(verifyX402(upperHeader, config, store, 1n, false)).resolves.toBe(commitment.toUpperCase())
    await expect(verifyX402(header, config, store, 1n, true)).resolves.toBe(commitment)
    expect(await store.claim(`${commitment}:4`, 60)).toBe(false)
  })

  it('lets only one mixed-version gateway own a payment at the final boundary', async () => {
    const nonceStore = new MemoryNonceStore()
    const operations = new MemoryPaymentOperations()
    let enteredLegacy!: () => void
    const legacyEntered = new Promise<void>((resolve) => { enteredLegacy = resolve })
    let releaseLegacy!: () => void
    const legacyRelease = new Promise<void>((resolve) => { releaseLegacy = resolve })
    const shared = {
      resolveAgent: async () => agent,
      getSandbox: async () => box(),
      recordUsage: async () => undefined,
      nonceStore,
    }
    const legacy = new Hono()
    legacy.route('/v1/agents', createAgentGateway({
      ...shared,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 1,
        authorizePayment: async () => {
          enteredLegacy()
          await legacyRelease
          return true
        },
      },
    }))
    const modern = new Hono()
    modern.route('/v1/agents', createAgentGateway({
      ...shared,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
    }))

    const request = (app: Hono) => app.request('/v1/agents/guards/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('5') },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'mixed deploy' }] }),
    })
    const legacyResponsePromise = request(legacy)
    await legacyEntered
    const modernResponse = await request(modern)
    await modernResponse.text()
    releaseLegacy()
    const legacyResponse = await legacyResponsePromise
    await legacyResponse.text()

    // The legacy gateway claims the shared nonce before its external callback.
    // The modern gateway must lose without invoking a second payment owner.
    expect(modernResponse.status).toBe(402)
    expect(legacyResponse.status).toBe(200)
    expect(operations.get(`x402:${commitment}:5`)).toBeUndefined()
  })

  it('lets only one separately configured version 2 gateway own a payment', async () => {
    const nonceStore = new MemoryNonceStore()
    let claimsReady = 0
    let releaseClaims!: () => void
    const claimsReleased = new Promise<void>((resolve) => { releaseClaims = resolve })
    let runs = 0
    let settlements = 0
    const stores = [
      new MemoryPaymentOperations({
        onSettle: async () => { settlements += 1 },
        onReclaim: async () => undefined,
      }),
      new MemoryPaymentOperations({
        onSettle: async () => { settlements += 1 },
        onReclaim: async () => undefined,
      }),
    ]
    const apps = stores.map((operations) => {
      const app = new Hono()
      app.route('/v1/agents', createAgentGateway({
        resolveAgent: async () => agent,
        getSandbox: async () => ({
          async *streamPrompt() {
            runs += 1
            yield* box().streamPrompt('run')
          },
        }),
        recordUsage: async () => undefined,
        nonceStore,
        x402: {
          operatorAddress,
          chainId: 1,
          demoMode: true,
          paymentProtocolVersion: 2,
          paymentOperations: operations,
          authorizePayment: async (payload, context) => {
            const operation = await operations.claimPayment(payload, context)
            claimsReady += 1
            if (claimsReady === 2) releaseClaims()
            await claimsReleased
            return operation
          },
        },
      }))
      return app
    })

    const responses = await Promise.all(apps.map((app) => app.request('/v1/agents/guards/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('32') },
      body: JSON.stringify({ max_tokens: 1, messages: [{ role: 'user', content: 'run once' }] }),
    })))
    await Promise.all(responses.map((response) => response.text()))

    expect(responses.map((response) => response.status).sort()).toEqual([200, 402])
    expect(runs).toBe(1)
    expect(settlements).toBe(1)
    expect(stores
      .map((store) => store.get(`x402:${commitment}:32`)?.state)
      .sort()).toEqual(['released', 'settled'])
  })

  it('does not let an idempotent retry release the payment owner', async () => {
    const nonceStore = new MemoryNonceStore()
    let settlements = 0
    let releases = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => { settlements += 1 },
      onRelease: async () => { releases += 1 },
      onReclaim: async () => undefined,
    })
    let runStarted!: () => void
    const sandboxStarted = new Promise<void>((resolve) => { runStarted = resolve })
    let finishRun!: () => void
    const runReleased = new Promise<void>((resolve) => { finishRun = resolve })
    let runs = 0
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          runStarted()
          await runReleased
          yield* box().streamPrompt('run')
        },
      }),
      recordUsage: async () => undefined,
      nonceStore,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
        authorizePayment: (payload, context) => operations.claimPayment(payload, context),
      },
    }))
    const request = () => app.request('/v1/agents/guards/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('33') },
      body: JSON.stringify({ max_tokens: 1, messages: [{ role: 'user', content: 'run once' }] }),
    })
    const requests = [request(), request()]
    await sandboxStarted
    const responses = await Promise.all(requests)
    expect(responses.map((response) => response.status).sort()).toEqual([200, 402])
    expect(operations.get(`x402:${commitment}:33`)?.state).toBe('executing')
    expect(releases).toBe(0)

    finishRun()
    await Promise.all(responses.map((response) => response.text()))
    expect(responses.map((response) => response.status).sort()).toEqual([200, 402])
    expect(runs).toBe(1)
    expect(settlements).toBe(1)
    expect(releases).toBe(0)
    expect(operations.get(`x402:${commitment}:33`)?.state).toBe('settled')
  })
})
