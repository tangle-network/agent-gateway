import { Hono } from 'hono'
import { describe, expect, it } from 'vitest'

import { InMemoryTaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import { MemoryNonceStore } from '../src/nonce-store'
import { MemoryPaymentOperations } from '../src/payment-operations'
import type { AgentMeta, GatewayConfig, SandboxBox } from '../src/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'ab'.repeat(32)}`

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
    app.route('/v1/agents', createAgentGateway(config))
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
    const body = await response.json() as { error?: { code?: number } }

    expect(body.error?.code).toBe(-32602)
    expect(runs).toBe(0)
    expect((await taskStore.get('task-payment-cancel'))?.status.state).toBe('canceled')
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
    app.route('/v1/agents', createAgentGateway(config))
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
    app.route('/v1/agents', createAgentGateway(config))
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
    expect(operations.get(`x402:${commitment}:75`)?.state).toBe('claimed')
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
    app.route('/v1/agents', createAgentGateway(config))
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

    expect(operations.get(`x402:${commitment}:77`)?.state).toBe('claimed')
    const canceled = await taskStore.get('task-cancel')
    expect(canceled?.status.state).toBe('canceled')
  })
})
