import { Hono } from 'hono'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { InMemoryTaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import { dispatchSandboxStreamRich, requiredX402Amount } from '../src/dispatch'
import { MemoryNonceStore, claimStoredNonce, type NonceStore } from '../src/nonce-store'
import { MemoryPaymentOperations } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore, type PaymentRecoveryRecord } from '../src/payment-recovery'
import { recoverPayment } from '../src/payment-recovery-worker'
import { verifyMpp } from '../src/verify'
import type { AgentMeta, GatewayConfig, SandboxStreamEvent } from '../src/types'
import type { MppConfig } from '../src/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'ab'.repeat(32)}`

const agent: AgentMeta = {
  id: 'agent-pr11',
  ownerId: 'owner-pr11',
  slug: 'pr11',
  systemPrompt: 'You are a test agent.',
  pricePerTokenUsd: 0.000001,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

afterEach(() => {
  vi.useRealTimers()
})

function paymentHeader(nonce: string): string {
  return JSON.stringify({
    commitment,
    signature: '0xsig',
    operator: operatorAddress,
    amount: '1000000000',
    nonce,
    expiry: String(Math.floor(Date.now() / 1000) + 600),
  })
}

function usage(inputTokens = 1) {
  return {
    inputTokens,
    outputTokens: 1,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd: 0.000002,
    budgetEnforced: true,
  }
}

function sandbox(events: SandboxStreamEvent[] = [
  { type: 'sandbox.usage', data: { usage: usage() } },
]): GatewayConfig['getSandbox'] extends (...args: never[]) => infer R ? R : never {
  return {
    async *streamPrompt() {
      yield* events
    },
  } as Awaited<ReturnType<GatewayConfig['getSandbox']>>
}

function durableConfig(
  overrides: Partial<GatewayConfig> = {},
): GatewayConfig {
  return {
    resolveAgent: async () => agent,
    getSandbox: async () => sandbox(),
    recordUsage: async () => undefined,
    x402: {
      operatorAddress,
      chainId: 1,
      demoMode: true,
      paymentProtocolVersion: 2,
      paymentOperations: new MemoryPaymentOperations({ onReclaim: async () => undefined }),
    },
    nonceStore: new MemoryNonceStore(),
    paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
    ...overrides,
  }
}

describe('PR #11 production regressions', () => {
  it('does not mark payment execution before sandbox acquisition succeeds', async () => {
    const operations = new MemoryPaymentOperations({ onReclaim: async () => undefined })
    let sandboxReady = false
    let beganBeforeSandbox = false
    const originalBegin = operations.beginPaymentExecution.bind(operations)
    operations.beginPaymentExecution = async (operation) => {
      if (!sandboxReady) beganBeforeSandbox = true
      return originalBegin(operation)
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      getSandbox: async () => {
        sandboxReady = true
        return sandbox()
      },
    })))

    const response = await app.request('/v1/agents/pr11/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('execution-order'),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'run' }] }),
    })
    await response.text()

    expect(beganBeforeSandbox).toBe(false)
  })

  it('renews the live execution callback while a provider stream is quiet', async () => {
    vi.useFakeTimers()
    const controller = new AbortController()
    let heartbeatCount = 0
    const config: GatewayConfig = {
      ...durableConfig(),
      paymentRecovery: { store: new MemoryPaymentRecoveryStore(), receiptTimeoutMs: 300 },
      getSandbox: async () => ({
        async *streamPrompt(_message: string, options?: { signal?: AbortSignal }) {
          await new Promise<void>((resolve) => {
            options?.signal?.addEventListener('abort', () => resolve(), { once: true })
          })
        },
      }),
    }

    const consume = (async () => {
      for await (const _event of dispatchSandboxStreamRich(
        agent,
        'run',
        commitment,
        config,
        controller.signal,
        undefined,
        4,
        undefined,
        true,
        undefined,
        undefined,
        async () => { heartbeatCount += 1 },
      )) {
        // The stream exits only after the request aborts.
      }
    })()
    await Promise.resolve()
    await Promise.resolve()
    await vi.advanceTimersByTimeAsync(1_000)

    expect(heartbeatCount).toBeGreaterThan(0)
    controller.abort()
    await consume
  })

  it('does not execute after a cancellation wins on another worker', async () => {
    const taskStore = new InMemoryTaskStore()
    let sandboxEntered!: () => void
    const sandboxReady = new Promise<void>((resolve) => { sandboxEntered = resolve })
    let releaseSandbox!: () => void
    const sandboxReleased = new Promise<void>((resolve) => { releaseSandbox = resolve })
    let runs = 0

    const makeConfig = (worker: 'runner' | 'canceler'): GatewayConfig => ({
      resolveAgent: async () => agent,
      getSandbox: async () => {
        if (worker === 'runner') {
          sandboxEntered()
          await sandboxReleased
        }
        return {
          async *streamPrompt() {
            runs += 1
            yield { type: 'sandbox.usage', data: { usage: usage() } }
          },
        }
      },
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 1,
        authorizePayment: async () => true,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    })

    const runner = new Hono()
    runner.route('/v1/agents', createAgentGateway(makeConfig('runner')))
    const canceler = new Hono()
    canceler.route('/v1/agents', createAgentGateway(makeConfig('canceler')))

    const request = runner.request('/v1/agents/pr11', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9001'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: 'pr11-cancel-race',
            contextId: 'pr11-context',
            messageId: 'pr11-message',
            parts: [{ kind: 'text', text: 'run' }],
          },
        },
      }),
    })
    await sandboxReady

    const cancel = await canceler.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'pr11-cancel-race' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseSandbox()
    const runnerResponse = await request
    await runnerResponse.text()

    expect(runs).toBe(0)
    expect((await taskStore.get('pr11-cancel-race'))?.status.state).toBe('canceled')
  })

  it('quotes retained A2A history before charging a continuation', async () => {
    const taskStore = new InMemoryTaskStore()
    let invocations = 0
    const longHistory = 'history '.repeat(500)
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          invocations += 1
          if (invocations === 1) {
            yield {
              type: 'input-required',
              data: { inputRequired: { prompt: 'What next?' } },
            }
            yield { type: 'sandbox.usage', data: { usage: usage(1) } }
            return
          }
          yield { type: 'sandbox.usage', data: { usage: usage(200) } }
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 1,
        authorizePayment: async () => true,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const send = (nonce: string, text: string) => app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader(nonce),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: nonce,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: 'pr11-continuation',
            contextId: 'pr11-context',
            messageId: `message-${nonce}`,
            parts: [{ kind: 'text', text }],
          },
        },
      }),
    })

    const first = await send('9004', longHistory)
    expect((await first.json() as { result?: { status?: { state?: string } } }).result?.status?.state)
      .toBe('input-required')
    const second = await send('9005', 'continue')

    expect(second.status).toBe(200)
    expect((await second.json() as { result?: { status?: { state?: string } } }).result?.status?.state)
      .toBe('completed')
  })

  it('uses the configured complete provider input bound before quoting', async () => {
    let quotedMessages: Array<{ role: string; content: string }> | undefined
    const inputTokenBound = ({ messages }: { messages: Array<{ role: string; content: string }> }) => {
      quotedMessages = messages
      return 4_096
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      maxOutputTokens: 1_024,
      defaultOutputTokens: 1_024,
      inputTokenBound,
    })))

    const response = await app.request('/v1/agents/pr11/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: [
          { role: 'user', content: 'first turn' },
          { role: 'assistant', content: 'prior answer' },
          { role: 'user', content: 'current turn' },
        ],
      }),
    })
    const body = await response.json() as {
      error?: { x402?: { required_amount?: string } }
    }

    expect(response.status).toBe(402)
    expect(quotedMessages).toEqual([
      { role: 'user', content: 'first turn' },
      { role: 'assistant', content: 'prior answer' },
      { role: 'user', content: 'current turn' },
    ])
    expect(body.error?.x402?.required_amount).toBe(
      requiredX402Amount(agent.pricePerTokenUsd, 4_096, 1_024, 6, 1_024, 1_024)
        .toString(),
    )
  })

  it('reconciles an x402 claiming row when the provider has no operation', async () => {
    const now = Date.now()
    const id = 'x402:missing-provider-operation'
    const store = new MemoryPaymentRecoveryStore()
    const record: PaymentRecoveryRecord = {
      version: 1,
      id,
      revision: 0,
      state: 'claiming',
      payment: { kind: 'x402', operationId: id },
      attribution: {
        requestId: 'request-pr11',
        agentId: agent.id,
        agentSlug: agent.slug,
        consumerId: commitment,
        paymentMethod: 'x402',
        startMs: now,
        pricePerTokenUsd: agent.pricePerTokenUsd,
        platformFeePercent: agent.platformFeePercent,
        requiredAmount: '1',
        currencyDecimals: 6,
        maxOutputTokens: 1,
        executionBudget: {
          maxInputTokens: 1,
          maxOutputTokens: 1,
          maxReasoningTokens: 0,
          maxToolTokens: 0,
          maxToolCalls: 0,
          maxProviderCostUsd: 0.000001,
        },
      },
      workStarted: false,
      usageRecorded: false,
      attempts: 0,
      nextAttemptAt: now,
      lease: { id: 'live-fence', expiresAt: now - 1 },
      createdAt: now,
      updatedAt: now,
    }
    await store.createIfAbsent(record)
    const config = durableConfig({
      paymentRecovery: { store },
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: new MemoryPaymentOperations({ onReclaim: async () => undefined }),
      },
    })

    const recovered = await recoverPayment(id, config, { force: true })

    expect(recovered?.state).toBe('reconciled')
  })

  it('does not leave an abandoned working A2A task after payment recovery', async () => {
    const taskStore = new InMemoryTaskStore()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const recoveryId = 'x402:abandoned-working-task'
    const now = Date.now()
    await recoveryStore.createIfAbsent({
      version: 1,
      id: recoveryId,
      revision: 0,
      state: 'reconciled',
      payment: { kind: 'x402', operationId: recoveryId },
      attribution: {
        requestId: 'request-abandoned',
        agentId: agent.id,
        agentSlug: agent.slug,
        consumerId: commitment,
        paymentMethod: 'x402',
        startMs: now,
        pricePerTokenUsd: agent.pricePerTokenUsd,
        platformFeePercent: agent.platformFeePercent,
        requiredAmount: '1',
        currencyDecimals: 6,
        maxOutputTokens: 1,
        executionBudget: {
          maxInputTokens: 1,
          maxOutputTokens: 1,
          maxReasoningTokens: 0,
          maxToolTokens: 0,
          maxToolCalls: 0,
          maxProviderCostUsd: 0.000001,
        },
      },
      workStarted: true,
      usageRecorded: false,
      attempts: 0,
      nextAttemptAt: Number.MAX_SAFE_INTEGER,
      lease: undefined,
      createdAt: now,
      updatedAt: now,
    })
    await taskStore.put({
      kind: 'task',
      id: 'abandoned-task',
      contextId: 'abandoned-context',
      status: { state: 'working', timestamp: new Date(now).toISOString() },
      metadata: { gatewayPaymentRecovery: { version: 1, id: recoveryId } },
    })
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      paymentRecovery: { store: recoveryStore },
      a2a: { taskStore },
    })))

    const response = await app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/get',
        params: { id: 'abandoned-task' },
      }),
    })
    const body = await response.json() as { result?: { status?: { state?: string } } }

    expect(response.status).toBe(200)
    expect(body.result?.status?.state).toBe('failed')
  })

  it('rejects a legacy check-then-mark nonce store instead of racing two payments', async () => {
    const seen = new Set<string>()
    const legacyStore: NonceStore = {
      hasSeen: async () => false,
      markSeen: async (nonce) => {
        await Promise.resolve()
        seen.add(nonce)
      },
    }

    const results = await Promise.allSettled([
      claimStoredNonce(legacyStore, 'replayed', 60),
      claimStoredNonce(legacyStore, 'replayed', 60),
    ])

    expect(results.every((result) => result.status === 'rejected')).toBe(true)
    expect(seen).toHaveLength(0)
  })

  it('preserves the 0.7.1 verifyMpp return contract', async () => {
    const config: MppConfig = {
      realm: 'gateway.test',
      method: 'stripe',
      verifySigner: async () => 'consumer-pr11',
    }
    const credential = Buffer.from(JSON.stringify({ sharedPaymentToken: 'token' })).toString('base64url')

    const result = await verifyMpp(
      `Payment stripe ${credential}`,
      config,
      { operatorAddress, chainId: 1, demoMode: true },
    )

    expect(result).toBe('consumer-pr11')
  })

  it('keeps the public markSeen method on the in-memory nonce store', async () => {
    const store = new MemoryNonceStore()
    expect(typeof store.markSeen).toBe('function')
    await store.markSeen!('compatibility', 60)
    expect(await store.hasSeen('compatibility')).toBe(true)
  })

  it('does not initialize A2A when an OpenAI-only gateway omits it', () => {
    expect(() => createAgentGateway({
      resolveAgent: async () => agent,
      getSandbox: async () => sandbox(),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: false,
        verifySigner: async () => true,
      },
    })).not.toThrow()
  })

  it('keeps the OpenAI surface available when an old A2A store is configured', async () => {
    const legacyTaskStore = {
      get: async () => undefined,
      put: async () => undefined,
      delete: async () => undefined,
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      resolveAgent: async () => agent,
      getSandbox: async () => sandbox(),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: false,
        verifySigner: async () => true,
      },
      a2a: { taskStore: legacyTaskStore },
    }))

    const discovery = await app.request('/v1/agents/pr11/chat/completions')
    const a2a = await app.request('/v1/agents/pr11', { method: 'POST', body: '{}' })

    expect(discovery.status).toBe(200)
    expect(a2a.status).toBe(503)
  })
})
