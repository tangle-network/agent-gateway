import { Hono } from 'hono'
import { describe, expect, it } from 'vitest'

import { InMemoryTaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import { MemoryNonceStore } from '../src/nonce-store'
import type {
  AgentMeta,
  GatewayConfig,
  SandboxBox,
  SandboxPromptResult,
  SandboxStreamEvent,
  Task,
} from '../src/types'

const agent: AgentMeta = {
  id: 'detached-agent',
  ownerId: 'owner',
  slug: 'detached-agent',
  systemPrompt: '',
  pricePerTokenUsd: 0.000001,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

const usage = {
  inputTokens: 1,
  outputTokens: 1,
  reasoningTokens: 0,
  toolTokens: 0,
  toolCallCount: 0,
  providerCostUsd: 0,
  budgetEnforced: true,
}

class DetachedTestSandbox implements SandboxBox {
  readonly id = 'detached-test-sandbox'
  readonly dispatches: Array<{ message: string; options: Record<string, unknown> }> = []
  interruptCalls = 0
  private admitted = false
  constructor(private readonly failFirstDispatch = false) {}
  private readonly queued: SandboxStreamEvent[] = []
  private waiting: (() => void) | undefined
  private completed = false
  private resultValue: SandboxPromptResult | undefined
  private resultWaiting: Array<(result: SandboxPromptResult) => void> = []

  async *streamPrompt(): AsyncIterable<SandboxStreamEvent> {
    throw new Error('detached A2A must not use streamPrompt')
  }

  async dispatchPrompt(message: string, options?: Record<string, unknown>) {
    this.dispatches.push({ message, options: options ?? {} })
    if (this.failFirstDispatch && this.dispatches.length === 1) {
      throw new Error('sandbox admission response was lost')
    }
    this.admitted = true
    return {
      sessionId: options?.sessionId as string,
      executionId: options?.executionId as string,
      status: 'running',
      alreadyExisted: false,
      dispatched: true,
    } as never
  }

  session() {
    return {
      events: (options?: { signal?: AbortSignal }) => this.events(options?.signal),
      result: () => this.result(),
      interrupt: async () => {
        this.interruptCalls += 1
        return { cancelled: false }
      },
    }
  }

  complete(response = 'done'): void {
    this.push({ type: 'message.part.updated', data: { part: { type: 'text' }, delta: response } })
    this.push({ type: 'sandbox.usage', data: { usage } })
    this.push({ type: 'done', data: {} })
    this.completed = true
    this.resultValue = {
      success: true,
      status: 'success',
      response,
      usage,
      durationMs: 1,
    } as SandboxPromptResult
    for (const resolve of this.resultWaiting) resolve(this.resultValue)
    this.resultWaiting = []
    this.waiting?.()
    this.waiting = undefined
  }

  private push(event: SandboxStreamEvent): void {
    this.queued.push(event)
    this.waiting?.()
    this.waiting = undefined
  }

  private async *events(signal?: AbortSignal): AsyncIterable<SandboxStreamEvent> {
    while (true) {
      if (this.queued.length > 0) {
        yield this.queued.shift()!
        continue
      }
      if (this.completed) return
      if (signal?.aborted) return
      await new Promise<void>((resolve) => {
        this.waiting = resolve
        signal?.addEventListener('abort', resolve, { once: true })
      })
    }
  }

  private result(): Promise<SandboxPromptResult> {
    if (!this.admitted) return Promise.reject(new Error('sandbox session was not admitted'))
    if (this.resultValue) return Promise.resolve(this.resultValue)
    return new Promise((resolve) => this.resultWaiting.push(resolve))
  }
}

function config(taskStore: InMemoryTaskStore, sandbox: DetachedTestSandbox): GatewayConfig {
  return {
    resolveAgent: async (slug) => (slug === agent.slug ? agent : null),
    getSandbox: async () => sandbox,
    recordUsage: async () => undefined,
    verifyApiKey: async () => ({
      keyId: 'key',
      consumerId: 'consumer',
      scopes: ['chat'],
    }),
    x402: { operatorAddress: '0x1111111111111111111111111111111111111111', chainId: 1, demoMode: true },
    nonceStore: new MemoryNonceStore(),
    a2a: { taskStore, authorizeTaskAccess: async () => true },
  }
}

function messageBody(method: string, taskId?: string) {
  return {
    jsonrpc: '2.0',
    id: `${method}-${taskId ?? 'new'}`,
    method,
    params: method.startsWith('tasks/')
      ? { id: taskId }
      : {
          message: {
            kind: 'message',
            role: 'user',
            ...(taskId ? { taskId } : {}),
            messageId: `message-${taskId ?? 'new'}`,
            parts: [{ kind: 'text', text: 'hello' }],
          },
        },
  }
}

async function post(app: Hono, body: unknown): Promise<Response> {
  return app.request(`/v1/agents/${agent.slug}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: 'Bearer sk_agent_test',
    },
    body: JSON.stringify(body),
  })
}

async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let attempt = 0; attempt < 100 && !(await predicate()); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
  expect(await predicate()).toBe(true)
}

describe('detached A2A execution', () => {
  it('keeps the sandbox running when the message stream reader disconnects', async () => {
    const taskStore = new InMemoryTaskStore()
    const sandbox = new DetachedTestSandbox()
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config(taskStore, sandbox)))

    const response = await post(app, { ...messageBody('message/stream') })
    const reader = response.body!.getReader()
    await reader.read()
    await reader.cancel()
    await waitFor(() => sandbox.dispatches.length === 1)
    sandbox.complete()
    const taskId = sandbox.dispatches[0]?.options.sessionId as string
    await waitFor(async () => (await taskStore.get(taskId))?.status.state === 'completed')

    expect(sandbox.dispatches).toHaveLength(1)
    expect(sandbox.interruptCalls).toBe(0)
  })

  it('reconciles the exact SDK result through tasks/get after another worker takes over', async () => {
    const taskStore = new InMemoryTaskStore()
    const sandbox = new DetachedTestSandbox()
    await sandbox.dispatchPrompt('hello', {
      sessionId: 'task-recovery',
      executionId: 'a2a-execution-turn-recovery',
      turnId: 'turn-recovery',
    })
    const task: Task = {
      kind: 'task',
      id: 'task-recovery',
      contextId: 'context-recovery',
      status: { state: 'working', timestamp: new Date().toISOString() },
      history: [{
        kind: 'message',
        role: 'user',
        taskId: 'task-recovery',
        contextId: 'context-recovery',
        messageId: 'message-recovery',
        parts: [{ kind: 'text', text: 'hello' }],
      }],
      metadata: {
        gatewayOrigin: { version: 1, agentId: agent.id, agentSlug: agent.slug },
        gatewaySubmission: {
          version: 1,
          lease: { id: 'submission-recovery', expiresAt: Date.now() + 60_000 },
          agentId: agent.id,
          agentSlug: agent.slug,
          requestId: 'turn-recovery',
          consumerId: 'consumer',
          paymentMethod: 'apikey',
        },
        gatewayExecution: {
          version: 1,
          requestId: 'turn-recovery',
          lease: { id: 'turn-recovery', expiresAt: Date.now() - 1 },
          detached: {
            environmentId: sandbox.id,
            sessionId: 'task-recovery',
            executionId: 'a2a-execution-turn-recovery',
            turnId: 'turn-recovery',
          },
        },
      },
    }
    await taskStore.put(task)
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config(taskStore, sandbox)))
    const recovery = post(app, messageBody('tasks/get', task.id))
    sandbox.complete('recovered')
    const body = await (await recovery).json() as { result?: { status?: { state?: string }; artifacts?: unknown[] } }

    expect(body.result?.status?.state).toBe('completed')
    expect(body.result?.artifacts).toHaveLength(1)
    expect(sandbox.dispatches).toHaveLength(1)
  })

  it('does not mark a task canceled when the exact interrupt reports cancelled:false', async () => {
    const taskStore = new InMemoryTaskStore()
    const sandbox = new DetachedTestSandbox()
    const first = new Hono()
    first.route('/v1/agents', createAgentGateway(config(taskStore, sandbox)))
    const stream = await post(first, messageBody('message/stream'))
    const reader = stream.body!.getReader()
    await reader.read()
    await waitFor(() => sandbox.dispatches.length === 1)
    const taskId = sandbox.dispatches[0]?.options.sessionId as string
    const second = new Hono()
    second.route('/v1/agents', createAgentGateway(config(taskStore, sandbox)))
    const cancel = post(second, messageBody('tasks/cancel', taskId))
    sandbox.complete()
    const body = await (await cancel).json() as { error?: { code?: number } }

    expect(body.error?.code).toBe(-32002)
    expect(sandbox.interruptCalls).toBe(1)
    expect((await taskStore.get(taskId!))?.status.state).toBe('completed')
    await reader.cancel()
  })

  it('re-admits a marker when the worker stopped before sandbox admission', async () => {
    const taskStore = new InMemoryTaskStore()
    const sandbox = new DetachedTestSandbox(true)
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config(taskStore, sandbox)))

    const initial = await post(app, messageBody('message/stream'))
    await initial.text()
    const taskId = sandbox.dispatches[0]?.options.sessionId as string
    const recovery = post(app, messageBody('tasks/get', taskId))
    await waitFor(() => sandbox.dispatches.length === 2)
    sandbox.complete('re-admitted')
    const body = await (await recovery).json() as { result?: { status?: { state?: string } } }

    expect(body.result?.status?.state).toBe('completed')
    expect(sandbox.dispatches).toHaveLength(2)
  })
})
