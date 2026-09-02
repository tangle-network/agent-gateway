import { Hono } from 'hono'
import { describe, expect, it } from 'vitest'

import { InMemoryTaskStore, type TaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import { MemoryNonceStore } from '../src/nonce-store'
import type { AgentMeta, GatewayConfig, SandboxBox } from '../src/types'
import type { Task } from '../src/a2a/types'
import { ServerAssignedTaskStore } from './server-assigned-task-store'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'ef'.repeat(32)}`

const agentA: AgentMeta = {
  id: 'agent-lifecycle-a',
  ownerId: 'owner',
  slug: 'lifecycle-a',
  systemPrompt: '',
  pricePerTokenUsd: 0.000001,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

const agentB: AgentMeta = { ...agentA, id: 'agent-lifecycle-b', slug: 'lifecycle-b' }

const usage = {
  inputTokens: 1,
  outputTokens: 1,
  reasoningTokens: 0,
  toolTokens: 0,
  toolCallCount: 0,
  providerCostUsd: 0.000002,
  budgetEnforced: true,
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

function message(taskId: string | undefined, text = 'hello') {
  return {
    kind: 'message',
    role: 'user',
    ...(taskId ? { taskId, contextId: `context-${taskId}` } : {}),
    messageId: `message-${taskId ?? 'new'}-${text}`,
    parts: [{ kind: 'text', text }],
  }
}

function body(method: string, taskId?: string, text = 'hello') {
  return {
    jsonrpc: '2.0',
    id: `${method}-${taskId ?? 'new'}`,
    method,
    params: method.startsWith('tasks/') ? { id: taskId } : { message: message(taskId, text) },
  }
}

function standardSandbox(): SandboxBox {
  return {
    async *streamPrompt() {
      yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'done' } }
      yield { type: 'sandbox.usage', data: { usage } }
    },
  }
}

function gatewayConfig(taskStore: TaskStore, overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    resolveAgent: async (slug) => (slug === agentA.slug ? agentA : null),
    getSandbox: async () => standardSandbox(),
    recordUsage: async () => undefined,
    x402: { operatorAddress, chainId: 1, demoMode: true },
    nonceStore: new MemoryNonceStore(),
    a2a: { taskStore, authorizeTaskAccess: async () => true },
    ...overrides,
  }
}

async function post(app: Hono, slug: string, requestBody: unknown, headers: Record<string, string> = {}) {
  return app.request(`/v1/agents/${slug}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(requestBody),
  })
}

describe('A2A lifecycle recovery and ownership', () => {
  it('retries legacy settlement from the durable task record after a crash', async () => {
    const taskStore = new ServerAssignedTaskStore(
      new InMemoryTaskStore(),
      'task-legacy-recovery',
    )
    let settlementCalls = 0
    let usageCalls = 0
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      ...gatewayConfig(taskStore, {
        resolveAgent: async (slug) => (slug === agentA.slug ? agentA : null),
        recordUsage: async () => { usageCalls += 1 },
        settlePayment: async () => {
          settlementCalls += 1
          if (settlementCalls === 1) throw new Error('legacy settlement acknowledgement lost')
        },
        x402: {
          operatorAddress,
          chainId: 1,
          demoMode: true,
          paymentProtocolVersion: 1,
          verifySigner: async () => true,
        },
      }),
    }))

    const first = await post(
      app,
      agentA.slug,
      body('message/send'),
      { 'X-Payment-Signature': paymentHeader('901') },
    )
    const firstBody = await first.json() as { error?: { code?: number } }
    expect(firstBody.error?.code).toBe(-32603)
    expect(settlementCalls).toBe(1)

    const retained = await taskStore.get('task-legacy-recovery')
    expect(retained?.metadata?.gatewayFinalizing).toBeDefined()
    const finalizing = retained?.metadata?.gatewayFinalizing as {
      lease: { id: string; expiresAt: number }
    }
    await taskStore.put({
      ...retained!,
      metadata: {
        ...retained!.metadata,
        gatewayFinalizing: {
          ...finalizing,
          lease: { ...finalizing.lease, expiresAt: Date.now() - 1 },
        },
      },
    })

    const recovered = await post(app, agentA.slug, body('tasks/get', 'task-legacy-recovery'))
    const recoveredBody = await recovered.json() as { result?: Task }
    expect(recoveredBody.result?.status.state).toBe('completed')
    expect(settlementCalls).toBe(2)
    expect(usageCalls).toBe(1)
    expect((await taskStore.get('task-legacy-recovery'))?.metadata?.gatewayFinalizing).toBeUndefined()
  })

  it('does not overwrite a newer cancellation or recovery marker on failure', async () => {
    class InterleavingFailureStore implements TaskStore {
      private readonly inner = new InMemoryTaskStore()

      async get(id: string) {
        return this.inner.get(id)
      }

      async put(task: Task) {
        if (task.status.state === 'failed') {
          const current = await this.inner.get(task.id)
          if (current) {
            await this.inner.put({
              ...current,
              status: { state: 'canceled', timestamp: new Date().toISOString() },
              metadata: {
                ...(current.metadata ?? {}),
                gatewayPaymentRecovery: { version: 1, id: 'recovery-race' },
              },
            })
          }
        }
        await this.inner.put(task)
      }

      async createIfAbsent(task: Task) {
        return this.inner.createIfAbsent(task)
      }

      async compareAndSet(expected: Task, next: Task) {
        if (next.status.state === 'failed') {
          const current = await this.inner.get(expected.id)
          if (current) {
            await this.inner.put({
              ...current,
              status: { state: 'canceled', timestamp: new Date().toISOString() },
              metadata: {
                ...(current.metadata ?? {}),
                gatewayPaymentRecovery: { version: 1, id: 'recovery-race' },
              },
            })
          }
        }
        return this.inner.compareAndSet(expected, next)
      }

      compareAndSetExecution(
        expected: Task,
        next: Task,
        requestId: string,
        now: number,
      ) {
        return this.inner.compareAndSetExecution(expected, next, requestId, now)
      }

      async delete(id: string) {
        return this.inner.delete(id)
      }
    }

    const taskStore = new ServerAssignedTaskStore(
      new InterleavingFailureStore(),
      'task-stale-failure',
    )
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(gatewayConfig(taskStore, {
      getSandbox: async () => ({
        async *streamPrompt() {
          throw new Error('sandbox failed before a receipt')
        },
      }),
    })))

    const response = await post(
      app,
      agentA.slug,
      body('message/send'),
      { 'X-Payment-Signature': paymentHeader('902') },
    )
    expect((await response.json() as { error?: unknown }).error).toBeDefined()

    const stored = await taskStore.get('task-stale-failure')
    expect(stored?.status.state).toBe('canceled')
    expect(stored?.metadata?.gatewayPaymentRecovery).toEqual({ version: 1, id: 'recovery-race' })
  })

  it('rejects task access and continuation through a different originating agent', async () => {
    const taskStore = new ServerAssignedTaskStore(
      new InMemoryTaskStore(),
      'task-origin-bound',
    )
    let resolvedOriginAgent = agentA
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      ...gatewayConfig(taskStore),
      resolveAgent: async (slug) => slug === agentA.slug
        ? resolvedOriginAgent
        : slug === agentB.slug
          ? agentB
          : null,
    }))

    const created = await post(
      app,
      agentA.slug,
      body('message/send'),
      { 'X-Payment-Signature': paymentHeader('903') },
    )
    expect((await created.json() as { result?: Task }).result?.status.state).toBe('completed')
    const original = await taskStore.get('task-origin-bound')
    expect(original?.metadata?.gatewayOrigin).toEqual({
      version: 1,
      agentId: agentA.id,
      agentSlug: agentA.slug,
    })

    const getFromOtherAgent = await post(app, agentB.slug, body('tasks/get', 'task-origin-bound'))
    expect(getFromOtherAgent.status).toBe(403)
    expect((await getFromOtherAgent.json() as { error?: { code?: number } }).error?.code)
      .toBe(-32008)

    resolvedOriginAgent = { ...agentA, id: 'agent-lifecycle-replaced' }
    const getAfterAgentReplacement = await post(app, agentA.slug, body('tasks/get', 'task-origin-bound'))
    expect(getAfterAgentReplacement.status).toBe(403)
    expect((await getAfterAgentReplacement.json() as { error?: { code?: number } }).error?.code)
      .toBe(-32008)

    const cancelFromOtherAgent = await post(app, agentB.slug, body('tasks/cancel', 'task-origin-bound'))
    expect(cancelFromOtherAgent.status).toBe(403)
    expect((await cancelFromOtherAgent.json() as { error?: { code?: number } }).error?.code)
      .toBe(-32008)
    expect((await taskStore.get('task-origin-bound'))?.status.state).toBe('completed')
  })

  it('allows task access through an alias that resolves to the originating agent', async () => {
    const taskStore = new ServerAssignedTaskStore(
      new InMemoryTaskStore(),
      'task-alias-bound',
    )
    const alias = 'workspace-agent-alias'
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway({
      ...gatewayConfig(taskStore),
      resolveAgent: async (slug) => slug === agentA.slug || slug === alias ? agentA : null,
    }))

    const created = await post(
      app,
      agentA.slug,
      body('message/send'),
      { 'X-Payment-Signature': paymentHeader('904') },
    )
    const createdBody = await created.json() as { result?: Task }
    expect(createdBody).toMatchObject({ result: { status: { state: 'completed' } } })

    const fetched = await post(app, alias, body('tasks/get', 'task-alias-bound'))
    expect(fetched.status).toBe(200)
    expect((await fetched.json() as { result?: Task }).result?.id).toBe(taskStore.serverAssignedId)
  })

  it('expires a task created before payment authorization can finish', async () => {
    class CrashAfterCreateStore implements TaskStore {
      private readonly inner = new InMemoryTaskStore()
      private crashed = false

      get(id: string) { return this.inner.get(id) }
      put(task: Task) { return this.inner.put(task) }
      compareAndSet(expected: Task, next: Task) { return this.inner.compareAndSet(expected, next) }
      compareAndSetExecution(expected: Task, next: Task, requestId: string, now: number) {
        return this.inner.compareAndSetExecution(expected, next, requestId, now)
      }
      delete(id: string) { return this.inner.delete(id) }

      async createIfAbsent(task: Task) {
        const created = await this.inner.createIfAbsent(task)
        if (created && !this.crashed) {
          this.crashed = true
          throw new Error('process crashed after task creation')
        }
        return created
      }
    }

    const taskStore = new ServerAssignedTaskStore(
      new CrashAfterCreateStore(),
      'task-created-before-claim',
    )
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(gatewayConfig(taskStore)))

    await post(
      app,
      agentA.slug,
      body('message/send'),
      { 'X-Payment-Signature': paymentHeader('904') },
    )

    const created = await taskStore.get('task-created-before-claim')
    expect(created?.status.state).toBe('submitted')
    expect(created?.metadata?.gatewaySubmission).toBeDefined()
    const submission = created?.metadata?.gatewaySubmission as {
      lease: { id: string; expiresAt: number }
    }
    await taskStore.put({
      ...created!,
      metadata: {
        ...created!.metadata,
        gatewaySubmission: {
          ...submission,
          lease: { ...submission.lease, expiresAt: Date.now() - 1 },
        },
      },
    })

    const recovered = await post(app, agentA.slug, body('tasks/get', 'task-created-before-claim'))
    const recoveredBody = await recovered.json() as { result?: Task }
    expect(recoveredBody.result?.status.state).toBe('failed')
    expect(recoveredBody.result?.status.state).not.toBe('submitted')
  })
})

describe('A2A client disconnect cancellation', () => {
  const delayedUsage = {
    inputTokens: 1,
    outputTokens: 1,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd: 0.000002,
    budgetEnforced: true,
  }

  class DetachedSandbox implements SandboxBox {
    readonly id = 'sandbox-disconnect'
    dispatchCount = 0
    interruptCount = 0
    readonly eventReads: Array<{ executionId?: string; since?: string }> = []
    private readonly startedPromise: Promise<void>
    private resolveStarted!: () => void
    private resolveRelease!: () => void
    private readonly releasePromise: Promise<void>
    private events: SandboxStreamEvent[] = []
    private executionId = ''
    private interrupted = false
    private done!: Promise<void>
    private resolveDone!: () => void

    constructor() {
      this.startedPromise = new Promise((resolve) => { this.resolveStarted = resolve })
      this.releasePromise = new Promise((resolve) => { this.resolveRelease = resolve })
    }

    async waitForStarted(): Promise<void> {
      return this.startedPromise
    }

    release(): void {
      this.resolveRelease()
    }

    async *streamPrompt(): AsyncIterable<SandboxStreamEvent> {
      throw new Error('the test requires detached dispatch')
    }

    async dispatchPrompt(_message: string, opts?: { sessionId?: string; turnId?: string }) {
      this.dispatchCount += 1
      const sessionId = opts?.sessionId ?? 'session-disconnect'
      this.executionId = `execution-${this.dispatchCount}`
      this.events = []
      this.interrupted = false
      this.done = new Promise((resolve) => { this.resolveDone = resolve })
      setTimeout(() => {
        void (async () => {
          this.events.push({
            type: 'message.part.updated',
            data: { part: { type: 'text' }, delta: 'started' },
          })
          this.resolveStarted()
          await this.releasePromise
          if (this.interrupted) {
            this.events.push({ type: 'error', data: { message: 'canceled' } })
          } else {
            this.events.push({ type: 'sandbox.usage', data: { usage: delayedUsage } })
          }
          this.events.push({ type: 'done', data: { usage: delayedUsage } })
          this.resolveDone()
        })()
      }, 0)
      return {
        sessionId,
        executionId: this.executionId,
        runControlRef: { environmentId: this.id, sessionId, executionId: this.executionId },
      }
    }

    session(sessionId: string) {
      return {
        events: (opts?: { executionId?: string; since?: string; signal?: AbortSignal }) => {
          this.eventReads.push({ executionId: opts?.executionId, since: opts?.since })
          return this.readEvents(opts)
        },
        result: async ({ executionId } = {}) => {
          if (executionId !== undefined && executionId !== this.executionId) {
            return { success: false, status: 'failed', executionId, error: 'wrong execution' }
          }
          await this.done
          return {
            success: !this.interrupted,
            status: this.interrupted ? 'canceled' : 'success',
            executionId: this.executionId,
            response: 'started',
            usage: delayedUsage,
          }
        },
        interrupt: async ({ executionId } = {}) => {
          const cancelled = executionId === undefined || executionId === this.executionId
          if (cancelled) {
            this.interruptCount += 1
            this.interrupted = true
            this.resolveRelease()
          }
          return { cancelled }
        },
      }
    }

    private async *readEvents(opts?: { executionId?: string; since?: string; signal?: AbortSignal }) {
      if (opts?.executionId !== undefined && opts.executionId !== this.executionId) return
      let index = 0
      while (true) {
        if (opts?.signal?.aborted) return
        if (index < this.events.length) yield this.events[index++]!
        else if (this.events.at(-1)?.type === 'done' || this.events.at(-1)?.type === 'error') return
        else await new Promise((resolve) => setTimeout(resolve, 0))
      }
    }
  }

  async function makeDisconnectHarness() {
    const taskStore = new InMemoryTaskStore()
    const sandbox = new DetachedSandbox()
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(gatewayConfig(taskStore, {
      getSandbox: async () => sandbox,
    })))
    return { app, sandbox, taskStore }
  }

  async function waitForTaskState(taskStore: InMemoryTaskStore, id: string, state: Task['status']['state']) {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const task = await taskStore.get(id)
      if (task?.status.state === state) return task
      await new Promise((resolve) => setTimeout(resolve, 0))
    }
    throw new Error(`task '${id}' did not reach '${state}'`)
  }

  it('keeps the detached run alive after the response reader disconnects', async () => {
    const { app, sandbox, taskStore } = await makeDisconnectHarness()
    const response = await post(
      app,
      agentA.slug,
      body('message/stream'),
      { 'X-Payment-Signature': paymentHeader('905') },
    )
    const reader = response.body!.getReader()
    const firstRead = reader.read()
    await sandbox.waitForStarted()
    const taskId = response.headers.get('X-Task-Id')!
    const executionMarker = (await taskStore.get(taskId))?.metadata?.gatewayExecution as Record<string, unknown>
    expect(executionMarker).toMatchObject({
      runControlRef: {
        environmentId: sandbox.id,
        sessionId: taskId,
        executionId: 'execution-1',
      },
    })
    await reader.cancel()
    const resubscribe = await post(
      app,
      agentA.slug,
      body('tasks/resubscribe', taskId),
      { 'X-Payment-Signature': paymentHeader('907') },
    )
    const resubscribeTextPromise = resubscribe.text()
    sandbox.release()
    await waitForTaskState(taskStore, taskId, 'completed')
    expect(sandbox.interruptCount).toBe(0)
    expect(sandbox.dispatchCount).toBe(1)
    const resubscribeText = await resubscribeTextPromise
    expect(resubscribeText).toContain('"state":"completed"')
    expect(sandbox.eventReads.filter(({ executionId }) => executionId === 'execution-1')).toHaveLength(2)
    await firstRead
  })

  it('interrupts only the exact detached run for tasks/cancel', async () => {
    const { app, sandbox, taskStore } = await makeDisconnectHarness()
    const response = await post(
      app,
      agentA.slug,
      body('message/stream'),
      { 'X-Payment-Signature': paymentHeader('908') },
    )
    const reader = response.body!.getReader()
    const firstRead = reader.read()
    await sandbox.waitForStarted()
    const taskId = response.headers.get('X-Task-Id')!
    const cancel = await post(
      app,
      agentA.slug,
      body('tasks/cancel', taskId),
      { 'X-Payment-Signature': paymentHeader('909') },
    )
    const canceled = (await cancel.json()) as { result?: Task }
    expect(canceled.result?.status.state).toBe('canceled')
    expect(sandbox.interruptCount).toBe(1)
    expect((await waitForTaskState(taskStore, taskId, 'canceled')).status.state).toBe('canceled')
    await reader.cancel()
    await firstRead
  })

  it('does not interrupt the run when the incoming request signal disconnects', async () => {
    const { app, sandbox, taskStore } = await makeDisconnectHarness()
    const requestAbort = new AbortController()
    const request = new Request(`http://localhost/v1/agents/${agentA.slug}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': paymentHeader('906'),
      },
      body: JSON.stringify(body('message/stream')),
      signal: requestAbort.signal,
    })
    const response = await app.fetch(request)
    const reader = response.body!.getReader()
    const firstRead = reader.read()
    await sandbox.waitForStarted()
    const taskId = response.headers.get('X-Task-Id')!
    requestAbort.abort()
    sandbox.release()
    await waitForTaskState(taskStore, taskId, 'completed')
    expect(sandbox.interruptCount).toBe(0)
    await reader.cancel()
    await firstRead
  })

  it('reconciles an expired detached run before tasks/get reports failure', async () => {
    const taskStore = new InMemoryTaskStore()
    const recoverySandbox: SandboxBox = {
      id: 'sandbox-recovery',
      async *streamPrompt() {},
      session: (sessionId) => ({
        events: async function* () {},
        result: async ({ executionId } = {}) => ({
          success: true,
          status: 'success',
          executionId,
          response: 'recovered',
          usage: delayedUsage,
        }),
        interrupt: async ({ executionId } = {}) => ({ cancelled: executionId === 'execution-recovery' }),
      }),
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(gatewayConfig(taskStore, {
      getSandbox: async () => recoverySandbox,
    })))
    const task: Task = {
      kind: 'task',
      id: 'task-expired-detached',
      contextId: 'context-expired-detached',
      status: { state: 'working', timestamp: new Date().toISOString() },
      history: [message(undefined, 'recover this')],
      metadata: {
        gatewayOrigin: { version: 1, agentId: agentA.id, agentSlug: agentA.slug },
        gatewaySubmission: {
          version: 1,
          lease: { id: 'submission-expired-detached', expiresAt: Date.now() - 1 },
          agentId: agentA.id,
          agentSlug: agentA.slug,
          requestId: 'request-expired-detached',
          consumerId: 'consumer-recovery',
        },
        gatewayExecution: {
          version: 1,
          requestId: 'request-expired-detached',
          lease: { id: 'request-expired-detached', expiresAt: Date.now() - 1 },
          runControlRef: {
            environmentId: 'sandbox-recovery',
            sessionId: 'session-recovery',
            executionId: 'execution-recovery',
          },
        },
      },
    }
    await taskStore.put(task)

    const response = await post(
      app,
      agentA.slug,
      body('tasks/get', task.id),
    )
    const recovered = (await response.json()) as { result?: Task }
    expect(response.status).toBe(200)
    expect(recovered.result?.status.state).toBe('completed')
    expect(recovered.result?.artifacts?.[0]?.parts).toEqual([
      { kind: 'text', text: 'recovered' },
    ])
  })
})
