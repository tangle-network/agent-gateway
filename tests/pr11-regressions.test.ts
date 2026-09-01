import { Hono } from 'hono'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { InMemoryTaskStore, type TaskStore } from '../src/a2a/task-store'
import { SqlTaskStore, type SqlAdapter } from '../src/a2a/task-store-sql'
import { InMemoryPushNotificationStore } from '../src/a2a/push-notifications'
import { recoverPaymentReleaseIfNeeded } from '../src/a2a/payment-recovery'
import { createAgentGateway } from '../src/middleware'
import { dispatchSandboxStreamRich, requiredX402Amount } from '../src/dispatch'
import { MemoryNonceStore, claimStoredNonce, type NonceStore } from '../src/nonce-store'
import { MemoryPaymentOperations } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore, type PaymentRecoveryRecord } from '../src/payment-recovery'
import { recoverPayment } from '../src/payment-recovery-worker'
import { verifyMpp } from '../src/verify'
import type { AgentMeta, GatewayConfig, SandboxStreamEvent } from '../src/types'
import { A2A_ERROR_CODES, type Task } from '../src/a2a/types'
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

function paymentHeader(nonce: string, expiry = Math.floor(Date.now() / 1000) + 600): string {
  return JSON.stringify({
    commitment,
    signature: '0xsig',
    operator: operatorAddress,
    amount: '1000000000',
    nonce,
    expiry: String(expiry),
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
  it('claims one terminal webhook when cancellation races fenced settlement on two workers', async () => {
    const taskStore = new InMemoryTaskStore()
    const pushStore = new InMemoryPushNotificationStore()
    const nonceStore = new MemoryNonceStore()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let settlements = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => { settlements += 1 },
      onReclaim: async () => undefined,
    })
    let sandboxEntered!: () => void
    const sandboxReady = new Promise<void>((resolve) => { sandboxEntered = resolve })
    let releaseSandbox!: () => void
    const sandboxReleased = new Promise<void>((resolve) => { releaseSandbox = resolve })
    let deliveries = 0
    const receivedTaskIds: string[] = []
    const webhook = new Hono()
    webhook.post('/terminal', async (context) => {
      deliveries += 1
      const body = await context.req.json() as { taskId?: string }
      if (body.taskId) receivedTaskIds.push(body.taskId)
      return context.text('ok')
    })
    const pushFetcher = async (
      _input: string | URL | Request,
      init?: RequestInit,
    ): Promise<Response> => webhook.fetch(new Request('https://receiver.local/terminal', init))

    const config = (sandbox: GatewayConfig['getSandbox']): GatewayConfig => ({
      resolveAgent: async () => agent,
      getSandbox: sandbox,
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore,
      paymentRecovery: { store: recoveryStore },
      a2a: {
        taskStore,
        pushStore,
        pushFetcher: pushFetcher as typeof fetch,
        authorizeTaskAccess: async () => true,
      },
    })

    const runner = new Hono()
    runner.route('/v1/agents', createAgentGateway(config(async () => ({
      async *streamPrompt() {
        sandboxEntered()
        yield { type: 'sandbox.usage', data: { usage: usage() } }
        await sandboxReleased
      },
    }))))
    const canceler = new Hono()
    canceler.route('/v1/agents', createAgentGateway(config(async () => ({
      async *streamPrompt() {
        throw new Error('canceler must not execute the sandbox')
      },
    }))))

    const running = runner.request('/v1/agents/pr11', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9011'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: 'pr11-push-race',
            contextId: 'pr11-push-context',
            messageId: 'pr11-push-message',
            parts: [{ kind: 'text', text: 'run' }],
          },
        },
      }),
    })
    await sandboxReady
    await pushStore.set('pr11-push-race', { id: 'terminal', url: 'https://hook.example/terminal' })

    const cancel = await canceler.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'pr11-push-race' },
      }),
    })
    expect(cancel.status).toBe(200)
    expect((await cancel.json() as { error?: { code?: number } }).error?.code)
      .toBe(A2A_ERROR_CODES.TASK_NOT_CANCELABLE)

    releaseSandbox()
    const runnerResponse = await running
    expect(runnerResponse.status).toBe(200)
    expect((await runnerResponse.json() as { result?: { status?: { state?: string } } })
      .result?.status?.state).toBe('completed')
    expect(settlements).toBe(1)
    expect(deliveries).toBe(1)
    expect(receivedTaskIds).toEqual(['pr11-push-race'])
    expect((await taskStore.get('pr11-push-race'))?.status.state).toBe('completed')
  })

  it('rejects production v1 authorization callbacks without durable recovery', () => {
    expect(() => createAgentGateway(durableConfig({
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 1,
        authorizePayment: async () => true,
      },
    }))).toThrow(/production x402 version 1 cannot use authorizePayment/)
  })

  it('recovers a timed-out v2 reserve by operation id without consuming the nonce', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-08-15T12:00:00.000Z'))
    let providerReservations = 0
    let providerRecoveries = 0
    const operations = new MemoryPaymentOperations({
      onClaim: async () => {
        providerReservations += 1
        throw new Error('provider timeout after reserve')
      },
      onReclaim: async () => { providerRecoveries += 1 },
    })
    const nonceStore = new MemoryNonceStore()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const config = durableConfig({
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore,
      paymentRecovery: { store: recoveryStore, retryDelayMs: 1 },
    })
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const response = await app.request('/v1/agents/pr11/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9012', Math.floor(Date.now() / 1000) + 1),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'reserve' }] }),
    })
    await response.text()

    const operationId = `x402:${commitment}:9012`
    expect(response.status).toBe(402)
    expect(providerReservations).toBe(1)
    expect(operations.get(operationId)?.state).toBe('claiming')
    expect(await nonceStore.hasSeen(`${commitment}:9012`)).toBe(false)

    vi.advanceTimersByTime(2_000)
    const recovered = await recoverPayment(operationId, config, { force: true })
    expect(recovered?.state).toBe('reconciled')
    expect(operations.get(operationId)?.state).toBe('reclaimed')
    expect(providerRecoveries).toBe(1)
  })

  it('rejects production v1 settlement before nonce claim or provider mutation', async () => {
    const nonceStore = new MemoryNonceStore()
    let sandboxRuns = 0
    let settlementCalls = 0
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      ...durableConfig({ nonceStore }),
      getSandbox: async () => ({
        async *streamPrompt() {
          sandboxRuns += 1
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      settlePayment: async () => {
        settlementCalls += 1
        throw new Error('provider acknowledgement lost')
      },
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 1,
      },
      paymentRecovery: undefined,
    }))

    const response = await app.request('/v1/agents/pr11/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9013'),
      },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'must not run' }] }),
    })

    expect(response.status).toBe(402)
    expect(await nonceStore.hasSeen(`${commitment}:9013`)).toBe(false)
    expect(sandboxRuns).toBe(0)
    expect(settlementCalls).toBe(0)
  })

  it('requires a webhook HMAC secret for production push delivery', () => {
    expect(() => createAgentGateway(durableConfig({
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: false,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: new MemoryPaymentOperations({ onReclaim: async () => undefined }),
      },
      a2a: {
        pushStore: new InMemoryPushNotificationStore(),
        authorizeTaskAccess: async () => true,
      },
    }))).toThrow(/production A2A push requires a webhookSecret/)
  })

  it('does not send an unsigned webhook if a production secret disappears at runtime', async () => {
    const taskStore = new InMemoryTaskStore()
    const pushStore = new InMemoryPushNotificationStore()
    let sandboxStarted!: () => void
    const sandboxReady = new Promise<void>((resolve) => { sandboxStarted = resolve })
    let releaseSandbox!: () => void
    const sandboxReleased = new Promise<void>((resolve) => { releaseSandbox = resolve })
    let deliveries = 0
    const config = durableConfig({
      getSandbox: async () => ({
        async *streamPrompt() {
          sandboxStarted()
          await sandboxReleased
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: false,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: new MemoryPaymentOperations({ onReclaim: async () => undefined }),
      },
      a2a: {
        taskStore,
        pushStore,
        webhookSecret: 'runtime-secret',
        pushFetcher: async () => {
          deliveries += 1
          return new Response('ok')
        },
        pushUrlValidator: async () => true,
        authorizeTaskAccess: async () => true,
      },
    })
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const request = app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9014'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: 'runtime-secret-task',
            contextId: 'runtime-secret-context',
            messageId: 'runtime-secret-message',
            parts: [{ kind: 'text', text: 'run' }],
          },
        },
      }),
    })
    await sandboxReady
    await pushStore.set('runtime-secret-task', { id: 'cfg', url: 'https://hook.example/terminal' })
    config.a2a!.webhookSecret = undefined
    releaseSandbox()

    const response = await request
    expect(response.status).toBe(200)
    expect((await response.json() as { result?: { status?: { state?: string } } })
      .result?.status?.state).toBe('completed')
    expect(deliveries).toBe(0)
  })

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

  it('rejects cross-worker cancellation after the durable fence and before provider start', async () => {
    const innerStore = new InMemoryTaskStore()
    let fenceWritten!: () => void
    const fenceReady = new Promise<void>((resolve) => { fenceWritten = resolve })
    let releaseFence!: () => void
    const fenceReleased = new Promise<void>((resolve) => { releaseFence = resolve })
    let blocked = false
    const taskStore: TaskStore = {
      get: (id) => innerStore.get(id),
      put: (task) => innerStore.put(task),
      createIfAbsent: (task) => innerStore.createIfAbsent(task),
      delete: (id) => innerStore.delete(id),
      async compareAndSet(expected, next) {
        const won = await innerStore.compareAndSet(expected, next)
        if (won && !blocked && next.metadata?.gatewayExecution !== undefined) {
          blocked = true
          fenceWritten()
          await fenceReleased
        }
        return won
      },
      compareAndSetExecution: (expected, next, requestId, now) =>
        innerStore.compareAndSetExecution(expected, next, requestId, now),
    }
    let providerStarted = false
    let releaseProvider!: () => void
    const providerReleased = new Promise<void>((resolve) => { releaseProvider = resolve })

    const makeConfig = (worker: 'runner' | 'canceler'): GatewayConfig => ({
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          if (worker === 'runner') {
            providerStarted = true
            await providerReleased
          }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
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
    })

    const runner = new Hono()
    runner.route('/v1/agents', createAgentGateway(makeConfig('runner')))
    const canceler = new Hono()
    canceler.route('/v1/agents', createAgentGateway(makeConfig('canceler')))

    const running = runner.request('/v1/agents/pr11', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9002'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: 'pr11-active-cancel-race',
            contextId: 'pr11-active-cancel-context',
            messageId: 'pr11-active-cancel-message',
            parts: [{ kind: 'text', text: 'run' }],
          },
        },
      }),
    })
    await fenceReady

    const cancel = await canceler.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'pr11-active-cancel-race' },
      }),
    })
    const cancelBody = await cancel.json() as { error?: { code?: number } }
    expect(cancel.status).toBe(200)
    expect(cancelBody.error?.code).toBe(A2A_ERROR_CODES.TASK_NOT_CANCELABLE)
    expect(providerStarted).toBe(false)
    expect((await taskStore.get('pr11-active-cancel-race'))?.status.state).toBe('working')

    releaseFence()
    releaseProvider()
    const runningResponse = await running
    const runningBody = await runningResponse.json() as { result?: { status?: { state?: string } } }
    expect(runningBody.result?.status?.state).toBe('completed')
    expect((await taskStore.get('pr11-active-cancel-race'))?.metadata?.gatewayExecution)
      .toBeUndefined()
  })

  it('keeps the submission lease until the task enters working state', async () => {
    const innerStore = new InMemoryTaskStore()
    let transitionStarted!: () => void
    const transitionReady = new Promise<void>((resolve) => { transitionStarted = resolve })
    let releaseTransition!: () => void
    const transitionReleased = new Promise<void>((resolve) => { releaseTransition = resolve })
    let blocked = false
    const taskStore: TaskStore = {
      get: (id) => innerStore.get(id),
      put: (task) => innerStore.put(task),
      createIfAbsent: (task) => innerStore.createIfAbsent(task),
      delete: (id) => innerStore.delete(id),
      async compareAndSet(expected, next) {
        if (!blocked && next.status.state === 'working') {
          blocked = true
          transitionStarted()
          await transitionReleased
        }
        return innerStore.compareAndSet(expected, next)
      },
      compareAndSetExecution: (expected, next, requestId, now) =>
        innerStore.compareAndSetExecution(expected, next, requestId, now),
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      a2a: { taskStore },
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        verifySigner: async () => true,
        paymentOperations: new MemoryPaymentOperations({ onReclaim: async () => undefined }),
      },
    })))

    const send = app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('9010'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            taskId: 'submission-lease-task',
            contextId: 'submission-lease-context',
            messageId: 'submission-lease-message',
            parts: [{ kind: 'text', text: 'run' }],
          },
        },
      }),
    })
    await transitionReady

    const pending = await taskStore.get('submission-lease-task')
    expect(pending?.status.state).toBe('submitted')
    expect(pending?.metadata?.gatewaySubmission).toBeDefined()
    expect(pending?.metadata?.gatewayPaymentRecovery).toBeDefined()

    releaseTransition()
    const response = await send
    expect(response.status).toBe(200)
    expect((await response.json() as { result?: { status?: { state?: string } } })
      .result?.status?.state).toBe('completed')
  })

  it('rejects cancellation when a heartbeat wins the cancellation CAS race', async () => {
    const innerStore = new InMemoryTaskStore()
    let cancelAttempts = 0
    const taskStore: TaskStore = {
      get: (id) => innerStore.get(id),
      put: (task) => innerStore.put(task),
      createIfAbsent: (task) => innerStore.createIfAbsent(task),
      delete: (id) => innerStore.delete(id),
      async compareAndSet(expected, next) {
        if (next.status.state === 'canceled' && cancelAttempts++ === 0) {
          await innerStore.put({
            ...expected,
            metadata: {
              ...(expected.metadata ?? {}),
              gatewayExecution: {
                version: 1,
                requestId: 'heartbeat-owner',
                lease: { id: 'heartbeat-owner', expiresAt: Date.now() + 60_000 },
              },
            },
          })
          return false
        }
        return innerStore.compareAndSet(expected, next)
      },
      compareAndSetExecution: (expected, next, requestId, now) =>
        innerStore.compareAndSetExecution(expected, next, requestId, now),
    }
    await taskStore.put({
      kind: 'task',
      id: 'cancel-heartbeat-task',
      contextId: 'cancel-heartbeat-context',
      status: { state: 'working', timestamp: new Date().toISOString() },
      metadata: {
        gatewayOrigin: { version: 1, agentId: agent.id, agentSlug: agent.slug },
      },
    })
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      a2a: { taskStore },
    })))

    const response = await app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/cancel',
        params: { id: 'cancel-heartbeat-task' },
      }),
    })
    const body = await response.json() as { result?: Task; error?: unknown }

    expect(response.status).toBe(200)
    expect((body.error as { code?: number } | undefined)?.code)
      .toBe(A2A_ERROR_CODES.TASK_NOT_CANCELABLE)
    expect(body.result).toBeUndefined()
    expect(cancelAttempts).toBe(1)
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

    const recoveryNow = now + 10_000
    const recovered = await recoverPayment(id, config, { force: true, now: recoveryNow })

    expect(recovered?.state).toBe('reconciled')
    expect((await store.get(id))?.reconciledAt).toBe(recoveryNow)
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

  it('fails a working task after its execution fence expires', async () => {
    const taskStore = new InMemoryTaskStore()
    const now = Date.now()
    await taskStore.put({
      kind: 'task',
      id: 'expired-execution-task',
      contextId: 'expired-execution-context',
      status: { state: 'working', timestamp: new Date(now).toISOString() },
      metadata: {
        gatewayOrigin: { version: 1, agentId: agent.id, agentSlug: agent.slug },
        gatewayExecution: {
          version: 1,
          requestId: 'abandoned-execution-request',
          lease: { id: 'abandoned-execution-request', expiresAt: now - 1 },
        },
      },
    })
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      a2a: { taskStore },
    })))

    const response = await app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/get',
        params: { id: 'expired-execution-task' },
      }),
    })
    const body = await response.json() as { result?: Task }

    expect(body.result?.status.state).toBe('failed')
    expect(body.result?.metadata?.gatewayExecution).toBeUndefined()
    expect(body.result?.metadata?.gatewayExecutionRecovery).toBeDefined()
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

  it('keeps A2A unavailable when production omits its task store', async () => {
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
    }))

    const discovery = await app.request('/v1/agents/pr11/chat/completions')
    const card = await app.request('/v1/agents/pr11/.well-known/agent.json')
    const a2a = await app.request('/v1/agents/pr11', { method: 'POST', body: '{}' })

    expect(discovery.status).toBe(200)
    expect(card.status).toBe(503)
    expect(a2a.status).toBe(503)
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

  it('does not retain an abandoned submission marker as payment recovery', async () => {
    vi.useFakeTimers()
    const store = new InMemoryTaskStore(10)
    const task: Task = {
      kind: 'task',
      id: 'expired-submission',
      contextId: 'expired-context',
      status: { state: 'submitted', timestamp: new Date().toISOString() },
      metadata: {
        gatewaySubmission: {
          version: 1,
          lease: { id: 'submission-lease', expiresAt: Date.now() + 5 * 60 * 1000 },
          agentId: agent.id,
          agentSlug: agent.slug,
          requestId: 'submission-request',
          consumerId: commitment,
        },
      },
    }
    await store.put(task)
    await vi.advanceTimersByTimeAsync(11)

    expect(await store.get(task.id)).toBeUndefined()
  })

  it('keeps generated input-required message ids stable across retries', async () => {
    const makeApp = (nonce: string) => {
      const app = new Hono()
      const taskStore = new InMemoryTaskStore()
      app.route('/v1/agents', createAgentGateway({
        resolveAgent: async () => agent,
        getSandbox: async () => sandbox([
          { type: 'input-required', data: { inputRequired: { prompt: 'Need one more detail' } } },
          { type: 'sandbox.usage', data: { usage: usage() } },
        ]),
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
      }))
      return app.request('/v1/agents/pr11', {
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
              taskId: 'stable-input-required',
              contextId: 'stable-context',
              messageId: 'stable-request',
              parts: [{ kind: 'text', text: 'run' }],
            },
          },
        }),
      })
    }

    const first = await makeApp('stable-1')
    const second = await makeApp('stable-2')
    const firstBody = await first.json() as { result?: { status?: { message?: { messageId?: string } } } }
    const secondBody = await second.json() as { result?: { status?: { message?: { messageId?: string } } } }

    expect(firstBody.result?.status?.message?.messageId).toBe(
      secondBody.result?.status?.message?.messageId,
    )
  })

  it('rejects oversized chunked A2A bodies without trusting Content-Length', async () => {
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig()))
    const response = await app.fetch(new Request('http://gateway.test/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/get',
        params: { id: 'oversized' },
        padding: 'x'.repeat(70_000),
      }),
    }))

    expect(response.status).toBe(413)
  })

  it('retries SQL task creation after removing an expired colliding row', async () => {
    interface Row {
      id: string
      context_id: string
      state: string
      payload: string
      updated_at: number
    }
    const rows = new Map<string, Row>()
    const db: SqlAdapter = {
      async exec(sql, params = []) {
        const statement = sql.trim()
        if (statement.startsWith('CREATE TABLE') || statement.startsWith('CREATE INDEX')) {
          return { rowsAffected: 0 }
        }
        if (statement.startsWith('INSERT INTO')) {
          const [id, contextId, state, payload, updatedAt] = params as [
            string,
            string,
            string,
            string,
            number,
          ]
          if (rows.has(id)) throw new Error('duplicate primary key')
          rows.set(id, { id, context_id: contextId, state, payload, updated_at: updatedAt })
          return { rowsAffected: 1 }
        }
        if (statement.startsWith('DELETE FROM')) {
          const [id, payload, updatedAt] = params as [string, string, number]
          const row = rows.get(id)
          if (!row || row.payload !== payload || row.updated_at !== updatedAt) {
            return { rowsAffected: 0 }
          }
          rows.delete(id)
          return { rowsAffected: 1 }
        }
        throw new Error(`unrecognized SQL: ${statement}`)
      },
      async query<TRow>(_sql: string, params: readonly unknown[] = []): Promise<TRow[]> {
        const row = rows.get(params[0] as string)
        return (row ? [{ payload: row.payload, updated_at: row.updated_at }] : []) as TRow[]
      },
    }
    const store = new SqlTaskStore(db, { ttlMs: 1_000 })
    const oldTask: Task = {
      kind: 'task',
      id: 'sql-expired-task',
      contextId: 'sql-context',
      status: { state: 'submitted', timestamp: new Date().toISOString() },
    }
    expect(await store.createIfAbsent(oldTask)).toBe(true)
    rows.get(oldTask.id)!.updated_at = Date.now() - 10_000
    const replacement: Task = {
      ...oldTask,
      status: { state: 'failed', timestamp: new Date().toISOString() },
    }

    expect(await store.createIfAbsent(replacement)).toBe(true)
    expect((await store.get(replacement.id))?.status.state).toBe('failed')
  })

  it('does not follow push notification redirects', async () => {
    const { deliverPushNotifications } = await import('../src/a2a/push-notifications')
    const pushStore = {
      async list() {
        return [{ id: 'redirect', url: 'https://hook.example/redirect' }]
      },
    }
    const fetcher = vi.fn(async (_url: string, _init: RequestInit) => (
      new Response(null, { status: 302, headers: { Location: 'http://169.254.169.254/' } })
    ))
    const results = await deliverPushNotifications({
      task: {
        kind: 'task',
        id: 'redirect-task',
        contextId: 'redirect-context',
        status: { state: 'completed', timestamp: new Date().toISOString() },
      },
      store: {
        list: pushStore.list,
        set: async () => undefined,
        get: async () => undefined,
        delete: async () => undefined,
      },
      webhookSecret: 'test-webhook-secret',
      fetcher: fetcher as unknown as typeof fetch,
    })

    expect(results[0]).toMatchObject({ ok: false, status: 302 })
    expect(results[0]?.error).toContain('redirect')
    expect(fetcher.mock.calls[0]?.[1]).toMatchObject({ redirect: 'manual' })
  })

  it('does not consume a rejected push claim and delivers on a valid retry', async () => {
    const { deliverPushNotifications } = await import('../src/a2a/push-notifications')
    const task: Task = {
      kind: 'task',
      id: 'push-validation-retry',
      contextId: 'push-validation-context',
      status: { state: 'completed', timestamp: new Date().toISOString() },
    }
    const pushStore = new InMemoryPushNotificationStore()
    await pushStore.set(task.id, { id: 'retry', url: 'https://receiver.example/hook' })
    const webhook = new Hono()
    let deliveries = 0
    webhook.post('/hook', async (context) => {
      deliveries += 1
      await context.req.json()
      return context.text('ok')
    })
    const fetcher = async (
      input: string | URL | Request,
      init?: RequestInit,
    ): Promise<Response> => webhook.fetch(
      input instanceof Request ? new Request(input, init) : new Request(input.toString(), init),
    )
    let policyAllows = false
    let claims = 0

    const deliver = () => deliverPushNotifications({
      task,
      store: pushStore,
      webhookSecret: 'test-webhook-secret',
      requireUrlValidator: true,
      urlValidator: async () => policyAllows,
      claimDelivery: async () => {
        claims += 1
        return true
      },
      fetcher,
    })

    const rejected = await deliver()
    expect(rejected[0]).toMatchObject({ ok: false, error: 'push notification URL was rejected by host policy' })
    expect(claims).toBe(0)
    expect(deliveries).toBe(0)

    policyAllows = true
    const delivered = await deliver()
    expect(delivered[0]).toMatchObject({ ok: true, status: 200 })
    expect(claims).toBe(1)
    expect(deliveries).toBe(1)
  })

  it('rejects unsigned direct push delivery before reading configs or fetching', async () => {
    const { deliverPushNotifications } = await import('../src/a2a/push-notifications')
    const list = vi.fn(async () => [{ id: 'unsigned', url: 'https://hook.example/terminal' }])
    const fetcher = vi.fn(async () => new Response('unexpected', { status: 200 }))

    await expect(deliverPushNotifications({
      task: {
        kind: 'task',
        id: 'unsigned-task',
        contextId: 'unsigned-context',
        status: { state: 'completed', timestamp: new Date().toISOString() },
      },
      store: {
        list,
        set: async () => undefined,
        get: async () => undefined,
        delete: async () => undefined,
      },
      webhookSecret: '   ',
      fetcher: fetcher as unknown as typeof fetch,
    })).rejects.toThrow('non-empty webhookSecret')

    expect(list).not.toHaveBeenCalled()
    expect(fetcher).not.toHaveBeenCalled()
  })

  it('closes a malformed execution marker through tasks/get and preserves payment recovery', async () => {
    const taskStore = new InMemoryTaskStore()
    const task: Task = {
      kind: 'task',
      id: 'malformed-execution-get',
      contextId: 'malformed-execution-context',
      status: { state: 'working', timestamp: new Date().toISOString() },
      metadata: {
        gatewayExecution: {
          version: 1,
          requestId: 'worker-a',
          lease: { id: 'worker-a' },
        },
        gatewayPaymentRecovery: { version: 1, id: 'payment-recovery-get' },
      },
    }
    await taskStore.put(task)
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      a2a: { taskStore },
    })))

    const response = await app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/get',
        params: { id: task.id },
      }),
    })
    const body = await response.json() as { result?: Task; error?: unknown }

    expect(response.status).toBe(200)
    expect(body.error).toBeUndefined()
    expect(body.result?.status.state).toBe('failed')
    expect(body.result?.metadata?.gatewayExecution).toBeUndefined()
    expect(body.result?.metadata?.gatewayExecutionRecovery).toMatchObject({
      error: expect.stringContaining('malformed'),
    })
    expect(body.result?.metadata?.gatewayPaymentRecovery)
      .toEqual({ version: 1, id: 'payment-recovery-get' })
    expect((await taskStore.get(task.id))?.status.state).toBe('failed')
  })

  it('closes a malformed execution marker through tasks/resubscribe and emits final state', async () => {
    const taskStore = new InMemoryTaskStore()
    const task: Task = {
      kind: 'task',
      id: 'malformed-execution-resubscribe',
      contextId: 'malformed-execution-context',
      status: { state: 'working', timestamp: new Date().toISOString() },
      metadata: {
        gatewayExecution: { version: 1, requestId: 'worker-b', lease: null },
        gatewayPaymentRecovery: { version: 1, id: 'payment-recovery-resubscribe' },
      },
    }
    await taskStore.put(task)
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(durableConfig({
      a2a: { taskStore },
    })))

    const response = await app.request('/v1/agents/pr11', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/resubscribe',
        params: { id: task.id },
      }),
    })
    const eventLine = (await response.text()).split('\n')
      .find((line) => line.startsWith('data: '))
    const event = eventLine
      ? JSON.parse(eventLine.slice('data: '.length)) as { result?: {
          final?: boolean
          status?: Task['status']
          taskId?: string
        } }
      : undefined

    expect(response.status).toBe(200)
    expect(event?.result?.taskId).toBe(task.id)
    expect(event?.result?.status?.state).toBe('failed')
    expect(event?.result?.final).toBe(true)
    expect((await taskStore.get(task.id))?.metadata?.gatewayExecution).toBeUndefined()
    expect((await taskStore.get(task.id))?.metadata?.gatewayPaymentRecovery)
      .toEqual({ version: 1, id: 'payment-recovery-resubscribe' })
  })

  it('clears the execution marker when malformed payment release metadata fails a task', async () => {
    const taskStore = new InMemoryTaskStore()
    const task: Task = {
      kind: 'task',
      id: 'malformed-payment-release',
      contextId: 'malformed-payment-release-context',
      status: { state: 'working', timestamp: new Date().toISOString() },
      metadata: {
        gatewayExecution: {
          version: 1,
          requestId: 'worker-a',
          lease: { id: 'worker-a', expiresAt: Date.now() + 60_000 },
        },
        gatewayPaymentRelease: { version: 1 },
      },
    }
    await taskStore.put(task)
    const deliverPush = vi.fn(async () => undefined)

    const recovered = await recoverPaymentReleaseIfNeeded(task, {
      taskStore,
      releasePayment: async () => undefined,
      releasePaymentAfterFailure: async () => undefined,
      recoverDurablePayment: async () => undefined,
      deliverPush,
    })

    expect(recovered.status.state).toBe('failed')
    expect(recovered.metadata?.gatewayExecution).toBeUndefined()
    expect(recovered.metadata?.gatewayPaymentRelease).toBeUndefined()
    expect(recovered.metadata?.gatewayPaymentReleaseRecovery).toMatchObject({
      error: expect.stringContaining('record is missing'),
    })
    expect((await taskStore.get(task.id))?.metadata?.gatewayExecution).toBeUndefined()
    expect(deliverPush).toHaveBeenCalledOnce()
  })

  it('rejects direct private push destinations before fetch', async () => {
    const { deliverPushNotifications } = await import('../src/a2a/push-notifications')
    const fetcher = vi.fn(async () => new Response('unexpected', { status: 200 }))
    const results = await deliverPushNotifications({
      task: {
        kind: 'task',
        id: 'private-task',
        contextId: 'private-context',
        status: { state: 'completed', timestamp: new Date().toISOString() },
      },
      store: {
        list: async () => [{ id: 'private', url: 'https://127.0.0.1/internal' }],
        set: async () => undefined,
        get: async () => undefined,
        delete: async () => undefined,
      },
      webhookSecret: 'test-webhook-secret',
      fetcher: fetcher as unknown as typeof fetch,
    })

    expect(results[0]?.ok).toBe(false)
    expect(results[0]?.error).toContain('safe HTTPS destination')
    expect(fetcher).not.toHaveBeenCalled()
  })

  it('rejects IPv4-mapped IPv6 private push destinations before fetch', async () => {
    const { deliverPushNotifications } = await import('../src/a2a/push-notifications')
    const fetcher = vi.fn(async () => new Response('unexpected', { status: 200 }))
    const urls = [
      'https://[::ffff:169.254.169.254]/metadata',
      'https://[::ffff:a9fe:a9fe]/metadata',
      'https://[::127.0.0.1]/metadata',
      'https://[::a9fe:a9fe]/metadata',
    ]

    for (const [index, url] of urls.entries()) {
      const results = await deliverPushNotifications({
        task: {
          kind: 'task',
          id: `mapped-private-task-${index}`,
          contextId: 'private-context',
          status: { state: 'completed', timestamp: new Date().toISOString() },
        },
        store: {
          list: async () => [{ id: `mapped-private-${index}`, url }],
          set: async () => undefined,
          get: async () => undefined,
          delete: async () => undefined,
        },
        webhookSecret: 'test-webhook-secret',
        fetcher: fetcher as unknown as typeof fetch,
      })

      expect(results[0]?.ok).toBe(false)
      expect(results[0]?.error).toContain('safe HTTPS destination')
    }

    expect(fetcher).not.toHaveBeenCalled()
  })
})
