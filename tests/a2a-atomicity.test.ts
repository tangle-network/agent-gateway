import { Hono } from 'hono'
import { describe, expect, it } from 'vitest'

import { InMemoryTaskStore, type TaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import { MemoryNonceStore } from '../src/nonce-store'
import { MemoryPaymentOperations, type PaymentOperation } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore } from '../src/payment-recovery'
import type { AgentMeta, GatewayConfig, SandboxBox, SandboxUsageReceipt } from '../src/types'
import type { Artifact, Task } from '../src/a2a/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'cd'.repeat(32)}`

const agent: AgentMeta = {
  id: 'agent-a2a-atomicity',
  ownerId: 'owner',
  slug: 'a2a-atomicity',
  systemPrompt: '',
  pricePerTokenUsd: 0.000001,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

const receipt: SandboxUsageReceipt = {
  inputTokens: 1,
  outputTokens: 1,
  reasoningTokens: 0,
  toolTokens: 0,
  toolCallCount: 0,
  providerCostUsd: 0.000002,
  budgetEnforced: true,
}

const artifact: Artifact = {
  artifactId: 'task-restart-artifact',
  name: 'response',
  parts: [{ kind: 'text', text: 'recovered output' }],
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

function requestBody(taskId: string, text: string) {
  return {
    jsonrpc: '2.0',
    id: text,
    method: 'message/send',
    params: {
      message: {
        kind: 'message',
        role: 'user',
        taskId,
        contextId: 'ctx-atomicity',
        messageId: `message-${text}`,
        parts: [{ kind: 'text', text }],
      },
    },
  }
}

class TwoWorkerCreateBarrier implements TaskStore {
  private readonly inner = new InMemoryTaskStore()
  private entered = 0
  private release!: () => void
  private readonly bothEntered = new Promise<void>((resolve) => { this.release = resolve })

  get(id: string): Promise<Task | undefined> {
    return this.inner.get(id)
  }

  put(task: Task): Promise<void> {
    return this.inner.put(task)
  }

  delete(id: string): Promise<void> {
    return this.inner.delete(id)
  }

  async createIfAbsent(task: Task): Promise<boolean> {
    this.entered += 1
    if (this.entered === 2) this.release()
    await this.bothEntered
    return this.inner.createIfAbsent(task)
  }

  compareAndSet(expected: Task, next: Task): Promise<boolean> {
    return this.inner.compareAndSet(expected, next)
  }
}

function atomicityConfig(
  taskStore: TaskStore,
  counters: { runs: number; records: number; settlements: number },
): GatewayConfig {
  const sandbox: SandboxBox = {
    async *streamPrompt() {
      counters.runs += 1
      yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'done' } }
      yield { type: 'sandbox.usage', data: { usage: receipt } }
    },
  }
  return {
    resolveAgent: async (slug) => (slug === agent.slug ? agent : null),
    getSandbox: async () => sandbox,
    recordUsage: async () => { counters.records += 1 },
    settlePayment: async () => { counters.settlements += 1 },
    x402: {
      operatorAddress,
      chainId: 1,
      paymentProtocolVersion: 1,
      verifySigner: async () => true,
    },
    nonceStore: new MemoryNonceStore(),
    a2a: {
      taskStore,
      authorizeTaskAccess: async () => true,
    },
  }
}

function recoveredOperation(operation: PaymentOperation) {
  return {
    protocolVersion: operation.protocolVersion,
    operationId: operation.operationId,
    acquiredByRequestId: operation.acquiredByRequestId,
    ...(operation.executionStartedAt !== undefined
      ? { executionStartedAt: operation.executionStartedAt }
      : {}),
    ...(operation.retentionReason ? { retentionReason: operation.retentionReason } : {}),
    nonceKey: operation.nonceKey,
    authorizationId: operation.authorizationId,
    reservedAmount: operation.reservedAmount.toString(),
    settledAmount: operation.settledAmount.toString(),
    refundAmount: operation.refundAmount.toString(),
    expiresAt: operation.expiresAt,
    state: operation.state,
  }
}

describe('A2A task atomicity and restart recovery', () => {
  it('rejects a non-atomic task store outside explicit demo mode', () => {
    const legacyStore: TaskStore = {
      get: async () => undefined,
      put: async () => undefined,
      delete: async () => undefined,
    }
    const config = atomicityConfig(legacyStore, { runs: 0, records: 0, settlements: 0 })

    expect(() => createAgentGateway(config)).toThrow(
      /A2A production task store must implement createIfAbsent and compareAndSet/,
    )
  })

  it('lets exactly one of two workers create, execute, and settle a task', async () => {
    const taskStore = new TwoWorkerCreateBarrier()
    const counters = { runs: 0, records: 0, settlements: 0 }
    const first = new Hono()
    const second = new Hono()
    first.route('/v1/agents', createAgentGateway(atomicityConfig(taskStore, counters)))
    second.route('/v1/agents', createAgentGateway(atomicityConfig(taskStore, counters)))

    const request = (app: Hono, nonce: string, text: string) => app.request(
      `/v1/agents/${agent.slug}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Payment-Signature': paymentHeader(nonce),
        },
        body: JSON.stringify(requestBody('task-two-workers', text)),
      },
    )

    const [firstResponse, secondResponse] = await Promise.all([
      request(first, '101', 'one'),
      request(second, '102', 'two'),
    ])
    const bodies = await Promise.all([
      firstResponse.json(),
      secondResponse.json(),
    ]) as Array<{ result?: { status?: { state?: string } }; error?: { code?: number } }>
    expect(bodies.filter((body) => body.result?.status?.state === 'completed')).toHaveLength(1)
    expect(bodies.filter((body) => body.error?.code === -32602)).toHaveLength(1)
    expect(counters.runs).toBe(1)
    expect(counters.records).toBe(1)
    expect(counters.settlements).toBe(1)
  })

  it('replays a crashed finalization after restart and clears the lease marker', async () => {
    const taskStore = new InMemoryTaskStore()
    const counters = { records: 0, settlements: 0 }
    const operations = new MemoryPaymentOperations({
      onSettle: async () => { counters.settlements += 1 },
      onReclaim: async () => undefined,
    })
    const paymentOperation = await operations.claimPayment(
      {
        commitment,
        nonce: '103',
        amount: '1000000000',
        expiry: String(Math.floor(Date.now() / 1000) + 300),
      },
      {
        requestId: 'crashed-request',
        agentId: agent.id,
        requiredAmount: 1n,
        maxOutputTokens: 1024,
        executionBudget: {
          maxInputTokens: 1024,
          maxOutputTokens: 1024,
          maxReasoningTokens: 0,
          maxToolTokens: 0,
          maxToolCalls: 0,
          maxProviderCostUsd: 1,
        },
      },
    )
    const executing = await operations.beginPaymentExecution(paymentOperation)
    const executionBudget = {
      maxInputTokens: 1024,
      maxOutputTokens: 1024,
      maxReasoningTokens: 0,
      maxToolTokens: 0,
      maxToolCalls: 0,
      maxProviderCostUsd: 1,
    }
    const task: Task = {
      kind: 'task',
      id: 'task-restart',
      contextId: 'ctx-restart',
      status: { state: 'working', timestamp: new Date().toISOString() },
      artifacts: [artifact],
      metadata: {
        gatewayFinalizing: {
          version: 1,
          lease: { id: 'crashed-lease', expiresAt: Date.now() - 1 },
          agentSlug: agent.slug,
          requestId: 'crashed-request',
          consumerId: 'consumer-restart',
          paymentMethod: 'x402',
          startMs: Date.now() - 100,
          operationId: executing.operationId,
          paymentOperation: recoveredOperation(executing),
          receipt,
          artifact,
          inputRequired: false,
          maxOutputTokens: 1024,
          executionBudget,
        },
      },
    }
    await taskStore.put(task)

    const config: GatewayConfig = {
      resolveAgent: async (slug) => (slug === agent.slug ? agent : null),
      getSandbox: async () => ({ async *streamPrompt() { throw new Error('restart recovery must not execute sandbox') } }),
      recordUsage: async () => { counters.records += 1 },
      x402: {
        operatorAddress,
        chainId: 1,
        paymentProtocolVersion: 2,
        verifySigner: async () => true,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
      a2a: {
        taskStore,
        authorizeTaskAccess: async () => true,
      },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))

    const response = await app.request(`/v1/agents/${agent.slug}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/get',
        params: { id: 'task-restart' },
      }),
    })
    const body = await response.json() as {
      result?: Task
      error?: unknown
    }

    expect(body.error).toBeUndefined()
    expect(body.result?.status.state).toBe('completed')
    expect(body.result?.artifacts).toEqual([artifact])
    expect((await taskStore.get('task-restart'))?.metadata?.gatewayFinalizing).toBeUndefined()
    expect(operations.get(executing.operationId)?.state).toBe('settled')
    expect(counters.records).toBe(1)
    expect(counters.settlements).toBe(1)
  })

  it('keeps a settling payment recoverable after a lost settlement acknowledgement', async () => {
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
    const claimed = await operations.claimPayment(
      {
        commitment,
        nonce: '104',
        amount: '1000000000',
        expiry: String(Math.floor(Date.now() / 1000) + 300),
      },
      {
        requestId: 'retry-request',
        agentId: agent.id,
        requiredAmount: 1n,
        maxOutputTokens: 1024,
        executionBudget: {
          maxInputTokens: 1024,
          maxOutputTokens: 1024,
          maxReasoningTokens: 0,
          maxToolTokens: 0,
          maxToolCalls: 0,
          maxProviderCostUsd: 1,
        },
      },
    )
    const executing = await operations.beginPaymentExecution(claimed)
    const executionBudget = {
      maxInputTokens: 1024,
      maxOutputTokens: 1024,
      maxReasoningTokens: 0,
      maxToolTokens: 0,
      maxToolCalls: 0,
      maxProviderCostUsd: 1,
    }
    const taskStore = new InMemoryTaskStore()
    await taskStore.put({
      kind: 'task',
      id: 'task-retry',
      contextId: 'ctx-retry',
      status: { state: 'working', timestamp: new Date().toISOString() },
      metadata: {
        gatewayFinalizing: {
          version: 1,
          lease: { id: 'retry-lease', expiresAt: Date.now() - 1 },
          agentSlug: agent.slug,
          requestId: 'retry-request',
          consumerId: 'consumer-retry',
          paymentMethod: 'x402',
          startMs: Date.now() - 100,
          operationId: executing.operationId,
          paymentOperation: recoveredOperation(executing),
          receipt,
          artifact,
          inputRequired: false,
          maxOutputTokens: 1024,
          executionBudget,
        },
      },
    })
    const config: GatewayConfig = {
      resolveAgent: async (slug) => (slug === agent.slug ? agent : null),
      getSandbox: async () => ({ async *streamPrompt() { throw new Error('recovery must not execute sandbox') } }),
      recordUsage: async () => { records += 1 },
      x402: {
        operatorAddress,
        chainId: 1,
        paymentProtocolVersion: 2,
        verifySigner: async () => true,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const getTask = () => app.request(`/v1/agents/${agent.slug}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tasks/get',
        params: { id: 'task-retry' },
      }),
    })

    const firstBody = await (await getTask()).json() as { result?: Task }
    expect(firstBody.result?.status.state).toBe('working')
    expect(operations.get(executing.operationId)?.state).toBe('settling')
    expect(settlementAttempts).toBe(1)
    const retained = await taskStore.get('task-retry')
    const marker = retained?.metadata?.gatewayFinalizing as {
      operationId: string
      recoveryAttempts?: number
      lease: { id: string; expiresAt: number }
    }
    expect(marker.operationId).toBe(executing.operationId)
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
    const secondBody = await (await getTask()).json() as { result?: Task }
    expect(secondBody.result?.status.state).toBe('completed')
    expect((await taskStore.get('task-retry'))?.metadata?.gatewayFinalizing).toBeUndefined()
    expect(operations.get(executing.operationId)?.state).toBe('settled')
    expect(recoveryAttempts).toBe(1)
    expect(records).toBe(1)
  })
})
