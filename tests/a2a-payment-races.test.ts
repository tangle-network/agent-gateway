import { Hono } from 'hono'
import { describe, expect, it } from 'vitest'

import { InMemoryTaskStore, type TaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import type { MppChargeLifecycle, MppChargeOperation } from '../src/mpp-payment'
import { MemoryNonceStore } from '../src/nonce-store'
import { MemoryPaymentOperations, type PaymentOperation } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore } from '../src/payment-recovery'
import { recoverPayment } from '../src/payment-recovery-worker'
import type { AgentMeta, GatewayConfig, SandboxBox } from '../src/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'ab'.repeat(32)}`

function createTestGateway(config: GatewayConfig) {
  return createAgentGateway({
    ...config,
    paymentRecovery: config.paymentRecovery ?? { store: new MemoryPaymentRecoveryStore() },
  })
}

const agent: AgentMeta = {
  id: 'agent-a2a-races',
  ownerId: 'owner',
  slug: 'a2a-races',
  systemPrompt: '',
  pricePerTokenUsd: 0.000001,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

function paymentHeader(nonce: string): string {
  return JSON.stringify({
    commitment,
    signature: '0xsig',
    operator: operatorAddress,
    amount: '1000000000',
    nonce,
    expiry: String(Math.floor(Date.now() / 1000) + 300),
  })
}

function stripePaymentHeader(token: string): string {
  const credential = Buffer.from(JSON.stringify({ sharedPaymentToken: token })).toString('base64url')
  return `Payment stripe ${credential}`
}

class A2AChargeLifecycle implements MppChargeLifecycle {
  readonly protocolVersion = 1 as const
  readonly operations = new Map<string, MppChargeOperation>()
  confirmations = 0
  releases = 0
  confirmationGate?: Promise<void>
  confirmationStarted?: () => void
  loseConfirmationAcknowledgement = false

  async confirmPayment(request: Parameters<MppChargeLifecycle['confirmPayment']>[0]) {
    this.confirmations += 1
    this.confirmationStarted?.()
    await this.confirmationGate
    const operation: MppChargeOperation = {
      protocolVersion: 1,
      operationId: request.operationId,
      acquiredByRequestId: request.requestId,
      method: request.method,
      receipt: `stripe-receipt=${request.operationId}`,
      state: 'confirmed',
    }
    this.operations.set(request.operationId, operation)
    if (this.loseConfirmationAcknowledgement) {
      throw new Error('Stripe confirmation acknowledgement lost')
    }
    return operation
  }

  async releasePayment(operation: MppChargeOperation) {
    const current = this.operations.get(operation.operationId)
    if (current?.state === 'released') return current
    this.releases += 1
    const released: MppChargeOperation = { ...operation, state: 'released' }
    this.operations.set(operation.operationId, released)
    return released
  }

  async recoverPayment(operationId: string) {
    return this.operations.get(operationId) ?? { operationId, state: 'not-found' as const }
  }
}

function message(text: string, taskId: string) {
  return {
    kind: 'message',
    role: 'user',
    taskId,
    contextId: 'ctx-race',
    messageId: `message-${text}`,
    parts: [{ kind: 'text', text }],
  }
}

function usage() {
  return {
    inputTokens: 1,
    outputTokens: 1,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd: 0.000002,
    budgetEnforced: true,
  }
}

describe('A2A payment ownership races', () => {
  it('does not execute a continuation canceled during payment authorization', async () => {
    const taskStore = new InMemoryTaskStore()
    await taskStore.put({
      kind: 'task',
      id: 'task-payment-cancel',
      contextId: 'ctx-race',
      status: { state: 'input-required', timestamp: new Date().toISOString() },
      history: [message('initial', 'task-payment-cancel')],
    })
    let authorizeEntered!: () => void
    const authorizationStarted = new Promise<void>((resolve) => { authorizeEntered = resolve })
    let finishAuthorization!: () => void
    const authorizationReleased = new Promise<void>((resolve) => { finishAuthorization = resolve })
    let runs = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 1,
        authorizePayment: async () => {
          authorizeEntered()
          await authorizationReleased
          return true
        },
      },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const continuation = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('76') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('continue', 'task-payment-cancel') },
      }),
    })
    await authorizationStarted

    const canceled = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-payment-cancel' },
      }),
    })
    expect(canceled.status).toBe(200)
    finishAuthorization()
    const response = await continuation
    const body = await response.json() as { result?: { status?: { state?: string } } }

    expect(body.result?.status?.state).toBe('canceled')
    expect(runs).toBe(0)
    expect((await taskStore.get('task-payment-cancel'))?.status.state).toBe('canceled')
  })

  it('retains a continuation task until ambiguous Stripe confirmation reconciles', async () => {
    const taskStore = new InMemoryTaskStore()
    await taskStore.put({
      kind: 'task',
      id: 'task-stripe-continuation-recovery',
      contextId: 'ctx-race',
      status: { state: 'input-required', timestamp: new Date().toISOString() },
      history: [message('initial', 'task-stripe-continuation-recovery')],
    })
    const lifecycle = new A2AChargeLifecycle()
    lifecycle.loseConfirmationAcknowledgement = true
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let runs = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => undefined,
      x402: { operatorAddress, chainId: 1, demoMode: true },
      mpp: {
        realm: 'gateway.test',
        method: 'stripe',
        authenticateCredential: async (payload) => ({
          consumerId: 'stripe:customer',
          paymentIdentity: String(payload.sharedPaymentToken),
        }),
        charge: lifecycle,
      },
      paymentRecovery: { store: recoveryStore },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const continuation = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: stripePaymentHeader('spt_continuation_ack'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('continue', 'task-stripe-continuation-recovery') },
      }),
    })
    const failed = await continuation.json() as { error?: { code?: number } }
    const retained = await taskStore.get('task-stripe-continuation-recovery')
    const marker = retained?.metadata?.gatewayPaymentRecovery as { id: string }

    expect(failed.error?.code).toBe(-32603)
    expect(retained?.status.state).toBe('input-required')
    expect(marker.id).toMatch(/^mpp:stripe:/)
    await taskStore.delete('task-stripe-continuation-recovery')
    expect(await taskStore.get('task-stripe-continuation-recovery')).toBeDefined()
    expect(runs).toBe(0)
    expect(lifecycle.confirmations).toBe(1)

    expect((await recoverPayment(marker.id, config, { force: true }))?.state).toBe('reconciled')
    const fetched = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/get',
        params: { id: 'task-stripe-continuation-recovery' },
      }),
    })
    expect(fetched.status).toBe(200)
    expect(lifecycle.releases).toBe(1)
    expect((await taskStore.get('task-stripe-continuation-recovery'))?.metadata?.gatewayPaymentRecovery)
      .toBeUndefined()
  })

  it('releases payment when cancellation interrupts execution before sandbox start', async () => {
    const taskStore = new InMemoryTaskStore()
    let executionStarted!: () => void
    const executionReady = new Promise<void>((resolve) => { executionStarted = resolve })
    let releaseExecution!: () => void
    const executionReleased = new Promise<void>((resolve) => { releaseExecution = resolve })
    let sandboxStarted = false

    class BlockingExecutionOperations extends MemoryPaymentOperations {
      override async beginPaymentExecution(operation: PaymentOperation): Promise<PaymentOperation> {
        const executing = await super.beginPaymentExecution(operation)
        executionStarted()
        await executionReleased
        return executing
      }
    }

    const operations = new BlockingExecutionOperations()
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        streamPrompt() {
          sandboxStarted = true
          return (async function* () {
            yield { type: 'sandbox.usage', data: { usage: usage() } }
          })()
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))

    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('82') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-before-sandbox') },
      }),
    })
    await executionReady

    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-before-sandbox' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseExecution()

    const response = await send
    const body = await response.json() as { result?: { status?: { state?: string } }; error?: unknown }

    expect(body.error).toBeUndefined()
    expect(body.result?.status?.state).toBe('canceled')
    expect(sandboxStarted).toBe(false)
    expect(operations.get(`x402:${commitment}:82`)?.state).toBe('released')
  })

  it('allows only one concurrent continuation to claim and settle a task', async () => {
    const taskStore = new InMemoryTaskStore()
    await taskStore.put({
      kind: 'task',
      id: 'task-continuation',
      contextId: 'ctx-race',
      status: { state: 'input-required', timestamp: new Date().toISOString() },
      history: [message('initial', 'task-continuation')],
    })
    let runs = 0
    let settlements = 0
    const box: SandboxBox = {
      async *streamPrompt() {
        runs += 1
        await new Promise((resolve) => setTimeout(resolve, 5))
        yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'done' } }
        yield { type: 'sandbox.usage', data: { usage: usage() } }
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      settlePayment: async () => { settlements += 1 },
      verifyApiKey: async () => ({ consumerId: 'consumer', keyId: 'key', scopes: ['chat'] }),
      x402: { operatorAddress, chainId: 1, demoMode: true },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const request = (text: string) => app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer sk_agent_race' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: text,
        method: 'message/send',
        params: { message: message(text, 'task-continuation') },
      }),
    })

    const [first, second] = await Promise.all([request('one'), request('two')])
    const bodies = await Promise.all([first.json(), second.json()]) as Array<{
      result?: { status?: { state?: string }; history?: unknown[] }
      error?: { code?: number }
    }>
    expect(bodies.filter((body) => body.result?.status?.state === 'completed')).toHaveLength(1)
    expect(bodies.filter((body) => body.error?.code === -32602)).toHaveLength(1)
    expect(runs).toBe(1)
    expect(settlements).toBe(1)
    const finalTask = await taskStore.get('task-continuation')
    expect(finalTask?.history).toHaveLength(2)
  })

  it('returns a canceled synchronous task without losing payment ownership', async () => {
    const taskStore = new InMemoryTaskStore()
    const operations = new MemoryPaymentOperations()
    let outputSeen!: () => void
    const outputReady = new Promise<void>((resolve) => { outputSeen = resolve })
    let releaseSandbox!: () => void
    const sandboxReleased = new Promise<void>((resolve) => { releaseSandbox = resolve })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
          outputSeen()
          await sandboxReleased
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('75') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-sync-cancel') },
      }),
    })
    await outputReady

    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-sync-cancel' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseSandbox()
    const response = await send
    const body = await response.json() as { result?: { status?: { state?: string } }; error?: unknown }

    expect(body.error).toBeUndefined()
    expect(body.result?.status?.state).toBe('canceled')
    expect((await taskStore.get('task-sync-cancel'))?.status.state).toBe('canceled')
    expect(operations.get(`x402:${commitment}:75`)?.state).toBe('retained')
  })

  it('settles when cancellation wins the final task update', async () => {
    const innerStore = new InMemoryTaskStore()
    let finalizationSeen!: () => void
    const finalizationReady = new Promise<void>((resolve) => { finalizationSeen = resolve })
    let releaseFinalization!: () => void
    const finalizationReleased = new Promise<void>((resolve) => { releaseFinalization = resolve })
    const taskStore: TaskStore = {
      get: (id) => innerStore.get(id),
      put: (task) => innerStore.put(task),
      createIfAbsent: (task) => innerStore.createIfAbsent(task),
      delete: (id) => innerStore.delete(id),
      async compareAndSet(expected, next) {
        if (next.metadata?.gatewayFinalizing) {
          finalizationSeen()
          await finalizationReleased
        }
        return innerStore.compareAndSet(expected, next)
      },
    }
    let settlements = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => { settlements += 1 },
      onReclaim: async () => undefined,
    })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('78') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-finalization-cancel') },
      }),
    })
    await finalizationReady

    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-finalization-cancel' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseFinalization()
    const response = await send
    const body = await response.json() as { result?: { status?: { state?: string } }; error?: unknown }

    expect(body.error).toBeUndefined()
    expect(body.result?.status?.state).toBe('canceled')
    expect(settlements).toBe(1)
    expect(operations.get(`x402:${commitment}:78`)?.state).toBe('settled')
  })

  it('settles a stream when cancellation wins the final task update', async () => {
    const innerStore = new InMemoryTaskStore()
    let cancellationStored!: () => void
    const cancellationReady = new Promise<void>((resolve) => { cancellationStored = resolve })
    let releaseCancellation!: () => void
    const cancellationReleased = new Promise<void>((resolve) => { releaseCancellation = resolve })
    let cancellationBlocked = false
    const taskStore: TaskStore = {
      get: (id) => innerStore.get(id),
      put: (task) => innerStore.put(task),
      createIfAbsent: (task) => innerStore.createIfAbsent(task),
      delete: (id) => innerStore.delete(id),
      async compareAndSet(expected, next) {
        const transitioned = await innerStore.compareAndSet(expected, next)
        if (transitioned && next.status.state === 'canceled' && !cancellationBlocked) {
          cancellationBlocked = true
          cancellationStored()
          await cancellationReleased
        }
        return transitioned
      },
    }
    let sandboxDrained!: () => void
    const sandboxReady = new Promise<void>((resolve) => { sandboxDrained = resolve })
    let releaseSandbox!: () => void
    const sandboxReleased = new Promise<void>((resolve) => { releaseSandbox = resolve })
    let settlements = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => { settlements += 1 },
      onReclaim: async () => undefined,
    })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
          sandboxDrained()
          await sandboxReleased
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const stream = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('80') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/stream',
        params: { message: message('run', 'task-stream-finalization-cancel') },
      }),
    })
    const reader = stream.body!.getReader()
    await sandboxReady

    const cancelPromise = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-stream-finalization-cancel' },
      }),
    })
    await cancellationReady
    releaseSandbox()
    let wire = ''
    const decoder = new TextDecoder()
    while (true) {
      const chunk = await reader.read()
      if (chunk.done) break
      wire += decoder.decode(chunk.value, { stream: true })
    }
    releaseCancellation()
    const cancel = await cancelPromise

    expect(cancel.status).toBe(200)
    expect(wire).toContain('"state":"canceled"')
    expect(settlements).toBe(1)
    expect(operations.get(`x402:${commitment}:80`)?.state).toBe('settled')
  })

  it('returns after cancel when a sandbox stops emitting events', async () => {
    const taskStore = new InMemoryTaskStore()
    const operations = new MemoryPaymentOperations()
    let outputSeen!: () => void
    const outputReady = new Promise<void>((resolve) => { outputSeen = resolve })
    const never = new Promise<void>(() => undefined)
    let sandboxSignal: AbortSignal | undefined
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt(_message, opts) {
          sandboxSignal = opts?.signal
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
          outputSeen()
          await never
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('79') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-silent-cancel') },
      }),
    })
    await outputReady
    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-silent-cancel' },
      }),
    })
    expect(cancel.status).toBe(200)
    const response = await Promise.race([
      send,
      new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error('send did not cancel')), 100)),
    ])
    const body = await response.json() as { result?: { status?: { state?: string } }; error?: unknown }

    expect(body.error).toBeUndefined()
    expect(body.result?.status?.state).toBe('canceled')
    expect(sandboxSignal?.aborted).toBe(true)
    expect(operations.get(`x402:${commitment}:79`)?.state).toBe('retained')
  })

  it('retains ownership when cancellation follows delivered output without a receipt', async () => {
    const taskStore = new InMemoryTaskStore()
    const operations = new MemoryPaymentOperations()
    let outputSeen!: () => void
    const outputReady = new Promise<void>((resolve) => { outputSeen = resolve })
    let releaseSandbox!: () => void
    const sandboxReleased = new Promise<void>((resolve) => { releaseSandbox = resolve })
    const box: SandboxBox = {
      async *streamPrompt() {
        yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
        outputSeen()
        await sandboxReleased
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))
    const streamPromise = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('77'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/stream',
        params: { message: message('run', 'task-cancel') },
      }),
    })
    const stream = await streamPromise
    const reader = stream.body!.getReader()
    await outputReady

    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-cancel' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseSandbox()
    while (!(await reader.read()).done) {
      // Drain the stream so its recovery path runs.
    }

    expect(operations.get(`x402:${commitment}:77`)?.state).toBe('retained')
    const canceled = await taskStore.get('task-cancel')
    expect(canceled?.status.state).toBe('canceled')
  })

  it('keeps a canceled task recoverable when settlement acknowledgement is lost', async () => {
    const innerStore = new InMemoryTaskStore()
    let finalizationSeen!: () => void
    const finalizationReady = new Promise<void>((resolve) => { finalizationSeen = resolve })
    let releaseFinalization!: () => void
    const finalizationReleased = new Promise<void>((resolve) => { releaseFinalization = resolve })
    let firstFinalization = true
    const taskStore: TaskStore = {
      get: (id) => innerStore.get(id),
      put: (task) => innerStore.put(task),
      createIfAbsent: (task) => innerStore.createIfAbsent(task),
      delete: (id) => innerStore.delete(id),
      async compareAndSet(expected, next) {
        if (firstFinalization && next.metadata?.gatewayFinalizing) {
          firstFinalization = false
          finalizationSeen()
          await finalizationReleased
        }
        return innerStore.compareAndSet(expected, next)
      },
    }
    let settlementAttempts = 0
    let recoveryAttempts = 0
    let records = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => {
        settlementAttempts += 1
        throw new Error('settlement acknowledgement lost')
      },
      onReclaim: async () => { recoveryAttempts += 1 },
    })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => { records += 1 },
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))

    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('83') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-canceled-settlement-recovery') },
      }),
    })
    await finalizationReady

    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-canceled-settlement-recovery' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseFinalization()

    const response = await send
    const body = await response.json() as { result?: { status?: { state?: string } }; error?: unknown }
    expect(body.error).toBeUndefined()
    expect(body.result?.status?.state).toBe('canceled')
    expect(settlementAttempts).toBe(1)
    expect(operations.get(`x402:${commitment}:83`)?.state).toBe('settling')

    const retained = await taskStore.get('task-canceled-settlement-recovery')
    const marker = retained?.metadata?.gatewayFinalizing as {
      lease: { id: string; expiresAt: number }
      recoveryAttempts?: number
    }
    expect(retained?.status.state).toBe('canceled')
    expect(marker.lease.expiresAt).toBeGreaterThan(Date.now())
    expect(marker.recoveryAttempts).toBe(1)

    await taskStore.put({
      ...retained!,
      metadata: {
        ...retained!.metadata,
        gatewayFinalizing: {
          ...marker,
          lease: { ...marker.lease, expiresAt: Date.now() - 1 },
        },
      },
    })
    const recovered = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 3,
        method: 'tasks/get',
        params: { id: 'task-canceled-settlement-recovery' },
      }),
    })
    const recoveredBody = await recovered.json() as {
      result?: { status?: { state?: string } }
    }
    expect(recoveredBody.result?.status?.state).toBe('canceled')
    expect((await taskStore.get('task-canceled-settlement-recovery'))?.metadata?.gatewayFinalizing).toBeUndefined()
    expect(operations.get(`x402:${commitment}:83`)?.state).toBe('settled')
    expect(recoveryAttempts).toBe(1)
    expect(records).toBe(1)
  })

  it('persists and recovers an ambiguous release acknowledgement', async () => {
    const taskStore = new InMemoryTaskStore()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let executionStarted!: () => void
    const executionReady = new Promise<void>((resolve) => { executionStarted = resolve })
    let releaseExecution!: () => void
    const executionReleased = new Promise<void>((resolve) => { releaseExecution = resolve })
    let sandboxStarted = false
    let releaseCalls = 0
    let recoveryCalls = 0

    class BlockingExecutionOperations extends MemoryPaymentOperations {
      override async beginPaymentExecution(operation: PaymentOperation): Promise<PaymentOperation> {
        const executing = await super.beginPaymentExecution(operation)
        executionStarted()
        await executionReleased
        return executing
      }
    }

    const operations = new BlockingExecutionOperations({
      onRelease: async () => {
        releaseCalls += 1
        throw new Error('release acknowledgement lost after refund')
      },
      onReclaim: async () => { recoveryCalls += 1 },
    })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        streamPrompt() {
          sandboxStarted = true
          return (async function* () {
            yield { type: 'sandbox.usage', data: { usage: usage() } }
          })()
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))

    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('84') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-release-recovery') },
      }),
    })
    await executionReady

    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-release-recovery' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseExecution()

    const response = await send
    const body = await response.json() as { result?: { status?: { state?: string } }; error?: unknown }
    expect(body.error).toBeUndefined()
    expect(body.result?.status?.state).toBe('canceled')
    expect(sandboxStarted).toBe(false)
    expect(operations.get(`x402:${commitment}:84`)?.state).toBe('releasing')
    expect(releaseCalls).toBe(1)

    const retained = await taskStore.get('task-release-recovery')
    const marker = retained?.metadata?.gatewayPaymentRecovery as { id: string }
    expect(retained?.status.state).toBe('canceled')
    expect(marker.id).toBe(`x402:${commitment}:84`)
    expect((await recoveryStore.get(marker.id))?.state).toBe('releasing')
    expect((await recoverPayment(marker.id, config, { force: true }))?.state).toBe('reconciled')
    const recovered = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 3,
        method: 'tasks/get',
        params: { id: 'task-release-recovery' },
      }),
    })
    const recoveredBody = await recovered.json() as {
      result?: { status?: { state?: string } }
    }
    expect(recoveredBody.result?.status?.state).toBe('canceled')
    expect((await taskStore.get('task-release-recovery'))?.metadata?.gatewayPaymentRecovery).toBeUndefined()
    expect(operations.get(`x402:${commitment}:84`)?.state).toBe('released')
    expect(releaseCalls).toBe(1)
    expect(recoveryCalls).toBe(1)
  })

  it('recovers an ambiguous release after shared nonce ownership fails', async () => {
    const taskStore = new InMemoryTaskStore()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const nonceStore = {
      hasSeen: async () => false,
      claim: async () => false,
    }
    let sandboxStarted = false
    let releaseCalls = 0
    let recoveryCalls = 0
    let failRecovery = true
    const operations = new MemoryPaymentOperations({
      onRelease: async () => {
        releaseCalls += 1
        throw new Error('release acknowledgement lost after refund')
      },
      onReclaim: async () => {
        recoveryCalls += 1
        if (failRecovery) {
          failRecovery = false
          throw new Error('release recovery acknowledgement also lost')
        }
      },
    })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        streamPrompt() {
          sandboxStarted = true
          return (async function* () {
            yield { type: 'sandbox.usage', data: { usage: usage() } }
          })()
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore,
      paymentRecovery: { store: recoveryStore },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))

    const response = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('86') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-nonce-release-recovery') },
      }),
    })
    const body = await response.json() as { error?: { code?: number } }
    expect(body.error?.code).toBe(-32603)
    expect(sandboxStarted).toBe(false)
    expect(operations.get(`x402:${commitment}:86`)?.state).toBe('releasing')
    expect(releaseCalls).toBe(1)
    expect(recoveryCalls).toBe(0)

    const retained = await taskStore.get('task-nonce-release-recovery')
    const marker = retained?.metadata?.gatewayPaymentRecovery as { id: string }
    expect(retained?.status.state).toBe('failed')
    expect(marker.id).toBe(`x402:${commitment}:86`)
    await expect(recoverPayment(marker.id, config, { force: true })).rejects.toThrow(
      'release recovery acknowledgement also lost',
    )
    expect(recoveryCalls).toBe(1)
    expect((await recoverPayment(marker.id, config, { force: true }))?.state).toBe('reconciled')

    const recovered = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/get',
        params: { id: 'task-nonce-release-recovery' },
      }),
    })
    expect(recovered.status).toBe(200)
    expect((await taskStore.get('task-nonce-release-recovery'))?.metadata?.gatewayPaymentRecovery)
      .toBeUndefined()
    expect(operations.get(`x402:${commitment}:86`)?.state).toBe('released')
    expect(recoveryCalls).toBe(2)
  })

  it('records usage once when an inserted acknowledgement is lost', async () => {
    const taskStore = new InMemoryTaskStore()
    const operations = new MemoryPaymentOperations({ onReclaim: async () => undefined })
    const usageRequestIds = new Set<string>()
    let recordCalls = 0
    let rows = 0
    let firstCall = true
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid output' } }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async (event) => {
        recordCalls += 1
        if (!usageRequestIds.has(event.requestId)) {
          usageRequestIds.add(event.requestId)
          rows += 1
        }
        if (firstCall) {
          firstCall = false
          throw new Error('usage acknowledgement lost after insert')
        }
      },
      x402: {
        operatorAddress,
        chainId: 1,
        verifySigner: async () => true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createTestGateway(config))

    const send = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': paymentHeader('85') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-usage-recovery') },
      }),
    })
    const body = await send.json() as { error?: { code?: number } }
    expect(body.error?.code).toBe(-32603)
    expect(recordCalls).toBe(1)
    expect(rows).toBe(1)
    expect(operations.get(`x402:${commitment}:85`)?.state).toBe('settled')

    const retained = await taskStore.get('task-usage-recovery')
    const marker = retained?.metadata?.gatewayFinalizing as {
      lease: { id: string; expiresAt: number }
      usageRecorded: boolean
    }
    expect(retained?.status.state).toBe('working')
    expect(marker.usageRecorded).toBe(false)
    await taskStore.put({
      ...retained!,
      metadata: {
        ...retained!.metadata,
        gatewayFinalizing: {
          ...marker,
          lease: { ...marker.lease, expiresAt: Date.now() - 1 },
        },
      },
    })

    const recovered = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/get',
        params: { id: 'task-usage-recovery' },
      }),
    })
    const recoveredBody = await recovered.json() as {
      result?: { status?: { state?: string } }
    }
    expect(recoveredBody.result?.status?.state).toBe('completed')
    expect(recordCalls).toBe(2)
    expect(rows).toBe(1)
    expect(usageRequestIds.size).toBe(1)
    expect((await taskStore.get('task-usage-recovery'))?.metadata?.gatewayFinalizing).toBeUndefined()
    expect(operations.get(`x402:${commitment}:85`)?.state).toBe('settled')
  })

  it('cancels during generic MPP confirmation without executing or stranding the charge', async () => {
    const taskStore = new InMemoryTaskStore()
    const lifecycle = new A2AChargeLifecycle()
    let confirmationStarted!: () => void
    const confirmationReady = new Promise<void>((resolve) => { confirmationStarted = resolve })
    lifecycle.confirmationStarted = confirmationStarted
    let releaseConfirmation!: () => void
    lifecycle.confirmationGate = new Promise<void>((resolve) => { releaseConfirmation = resolve })
    let runs = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => undefined,
      x402: { operatorAddress, chainId: 1, demoMode: true },
      mpp: {
        realm: 'gateway.test',
        method: 'stripe',
        authenticateCredential: async (payload) => ({
          consumerId: 'stripe:customer',
          paymentIdentity: String(payload.sharedPaymentToken),
        }),
        charge: lifecycle,
      },
      paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const send = app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: stripePaymentHeader('spt_cancel_confirm'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-stripe-confirm-cancel') },
      }),
    })
    await confirmationReady
    expect((await taskStore.get('task-stripe-confirm-cancel'))?.metadata?.gatewayPaymentRecovery)
      .toBeDefined()
    await taskStore.delete('task-stripe-confirm-cancel')
    expect(await taskStore.get('task-stripe-confirm-cancel')).toBeDefined()
    const cancel = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/cancel',
        params: { id: 'task-stripe-confirm-cancel' },
      }),
    })
    expect(cancel.status).toBe(200)
    releaseConfirmation()
    const response = await send
    const body = await response.json() as { result?: { status?: { state?: string } } }

    expect(body.result?.status?.state).toBe('canceled')
    expect(runs).toBe(0)
    expect(lifecycle.confirmations).toBe(1)
    expect(lifecycle.releases).toBe(1)
    expect([...lifecycle.operations.values()][0]?.state).toBe('released')
    expect((await taskStore.get('task-stripe-confirm-cancel'))?.metadata?.gatewayPaymentRecovery)
      .toBeUndefined()
  })

  it('returns the confirmed generic MPP receipt on A2A send and stream responses', async () => {
    const taskStore = new InMemoryTaskStore()
    const lifecycle = new A2AChargeLifecycle()
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid' } }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async () => undefined,
      x402: { operatorAddress, chainId: 1, demoMode: true },
      mpp: {
        realm: 'gateway.test',
        method: 'stripe',
        authenticateCredential: async (payload) => ({
          consumerId: 'stripe:customer',
          paymentIdentity: String(payload.sharedPaymentToken),
        }),
        charge: lifecycle,
      },
      paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const request = (method: 'message/send' | 'message/stream', taskId: string, token: string) =>
      app.request('/v1/agents/a2a-races', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: stripePaymentHeader(token),
        },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: taskId,
          method,
          params: { message: message('run', taskId) },
        }),
      })

    const sent = await request('message/send', 'task-stripe-send-receipt', 'spt_send_receipt')
    await sent.text()
    const streamed = await request('message/stream', 'task-stripe-stream-receipt', 'spt_stream_receipt')
    await streamed.text()

    expect(sent.status).toBe(200)
    expect(sent.headers.get('Payment-Receipt')).toContain('stripe-receipt=')
    expect(sent.headers.get('X-Payment-Operation-Id')).toMatch(/^mpp:stripe:/)
    expect(streamed.status).toBe(200)
    expect(streamed.headers.get('Payment-Receipt')).toContain('stripe-receipt=')
    expect(streamed.headers.get('X-Payment-Operation-Id')).toMatch(/^mpp:stripe:/)
    expect(lifecycle.confirmations).toBe(2)
  })

  it('recovers generic MPP usage acknowledgement loss from the durable task receipt', async () => {
    const taskStore = new InMemoryTaskStore()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const lifecycle = new A2AChargeLifecycle()
    const requestIds = new Set<string>()
    let recordCalls = 0
    let firstAcknowledgement = true
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid' } }
          yield { type: 'sandbox.usage', data: { usage: usage() } }
        },
      }),
      recordUsage: async (event) => {
        recordCalls += 1
        requestIds.add(event.requestId)
        if (firstAcknowledgement) {
          firstAcknowledgement = false
          throw new Error('usage acknowledgement lost after insert')
        }
      },
      x402: { operatorAddress, chainId: 1, demoMode: true },
      mpp: {
        realm: 'gateway.test',
        method: 'stripe',
        authenticateCredential: async (payload) => ({
          consumerId: 'stripe:customer',
          paymentIdentity: String(payload.sharedPaymentToken),
        }),
        charge: lifecycle,
      },
      paymentRecovery: { store: recoveryStore },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const sent = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: stripePaymentHeader('spt_usage_ack'),
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: { message: message('run', 'task-stripe-usage-recovery') },
      }),
    })
    const failed = await sent.json() as { error?: { code?: number } }
    expect(failed.error?.code).toBe(-32603)
    const retained = await taskStore.get('task-stripe-usage-recovery')
    const finalization = retained?.metadata?.gatewayFinalizing as {
      lease: { id: string; expiresAt: number }
    }
    expect(retained?.metadata?.gatewayPaymentRecovery).toBeDefined()
    await taskStore.put({
      ...retained!,
      metadata: {
        ...retained!.metadata,
        gatewayFinalizing: {
          ...finalization,
          lease: { ...finalization.lease, expiresAt: Date.now() - 1 },
        },
      },
    })

    const recovered = await app.request('/v1/agents/a2a-races', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tasks/get',
        params: { id: 'task-stripe-usage-recovery' },
      }),
    })
    const body = await recovered.json() as { result?: { status?: { state?: string } } }

    expect(body.result?.status?.state).toBe('completed')
    expect(recordCalls).toBe(2)
    expect(requestIds.size).toBe(1)
    expect(lifecycle.confirmations).toBe(1)
    expect(lifecycle.releases).toBe(0)
    expect((await taskStore.get('task-stripe-usage-recovery'))?.metadata).toBeUndefined()
  })
})
