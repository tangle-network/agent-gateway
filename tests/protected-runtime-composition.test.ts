import { afterEach, describe, expect, it, vi } from 'vitest'
import { createProtectedRuntimeChatProducer, createChatTurnRoutes, streamChatRouteAsSandboxEvents, type ProtectedRuntimeChatOptions, type ChatTurnMessageStore } from '@tangle-network/agent-app/chat-routes'
import { createMemoryTurnEventStore } from '@tangle-network/agent-app/stream'
import { createAgentGateway } from '../src/middleware'
import { MemoryPaymentOperations } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore } from '../src/payment-recovery'

type AgentCandidateModelPort = ProtectedRuntimeChatOptions['grant']['port']
const digest = 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as const
const model = 'anthropic/claude-haiku-4-5-20251001'

function fixture() {
  const resolved = { requested: model, model, snapshot: model, provider: 'anthropic', reasoningEffort: 'none' as const }
  const settle = vi.fn<AgentCandidateModelPort['settleGrant']>(async input => ({
    preparationId: input.preparationId, grantDigest: digest, closed: true, usageWithinLimits: true,
    calls: [{ callId: 'call-1', generationId: 'generation-1', traceSpanId: 'generation-1',
      model, status: 'succeeded', startedAtMs: 1, endedAtMs: 2,
      inputTokens: 3, accountedInputTokens: 3, outputTokens: 2, cachedInputTokens: 0,
      reasoningTokens: 0, costUsdNanos: 100_000, costProvenance: 'observed' }],
  }))
  const port: AgentCandidateModelPort = {
    resolve: async () => resolved,
    reserveGrant: async input => ({ preparationId: input.preparationId, digest,
      expiresAtMs: input.expiresAtMs, enforcedLimits: input.limits,
      network: { mode: 'gateway-only', domains: ['candidate-router.tangle.tools'] } }),
    activateGrant: async () => ({ env: { OPENAI_API_KEY: 'scoped-fixture-token',
      OPENAI_BASE_URL: 'https://candidate-router.tangle.tools/v1' } }),
    settleGrant: settle,
  }
  const options: ProtectedRuntimeChatOptions = {
    profile: { name: 'protected-test', harness: 'cli-base', prompt: { systemPrompt: 'Use the available tools.' },
      model: { provider: 'tangle-router', default: model, reasoningEffort: 'none', maxVisibleOutputTokens: 100 } },
    prompt: 'Read the current brief and save a useful finding.', tools: [], maxToolCalls: 2,
    grant: { port, resolve: { requested: model, harness: 'cli-base', reasoningEffort: 'none' },
      reserve: { executionId: 'execution-1', preparationId: 'preparation-1', bundleDigest: digest,
        expiresAtMs: Date.now() + 30_000, attempt: { number: 1, maxAttempts: 1, retryPolicy: 'none' },
        limits: { maxModelCalls: 3, maxInputTokens: 1000, maxOutputTokens: 100, maxCostUsd: 1 } },
      deadlineAtMs: Date.now() + 20_000 },
  }
  return { options, settle }
}

function response(message: Record<string, unknown>) {
  return Response.json({ id: 'completion-1', object: 'chat.completion', model,
    choices: [{ index: 0, message: { role: 'assistant', ...message }, finish_reason: message.tool_calls ? 'tool_calls' : 'stop' }],
    usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 }, cost: 0.0001 })
}


afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks() })

describe('protected Runtime failed-run gateway composition', () => {
  it('settles a paid call after an authorized tool fails through the persisted chat route', async () => {
    const { options, settle } = fixture()
    const abort = new AbortController()
    options.signal = abort.signal
    const effect = vi.fn(async () => {
      const failure = new Error('Synthetic tool database unavailable')
      abort.abort(failure)
      throw failure
    })
    options.tools = [{ name: 'save', description: 'Save a finding.', inputSchema: { type: 'object' }, run: effect }]
    options.profile.tools = { save: true }
    const inference = vi.fn(async () => response({ content: null, tool_calls: [
      { id: 'first', type: 'function', function: { name: 'save', arguments: '{}' } },
    ] }))
    vi.stubGlobal('fetch', inference)
    const rows: Array<Awaited<ReturnType<ChatTurnMessageStore['appendMessage']>>> = []
    const pending: Promise<unknown>[] = []
    const store: ChatTurnMessageStore = {
      listMessages: async threadId => rows.filter(row => row.threadId === threadId),
      appendMessage: async input => { const row = { id: `message-${rows.length}`, ...input }; rows.push(row); return row },
      updateMessage: async (id, patch) => { const row = rows.find(row => row.id === id); if (row) Object.assign(row, patch); return row ?? null },
      deleteMessage: async () => null,
    }
    const routes = createChatTurnRoutes({ projectId: 'synthetic-composed',
      authorize: async () => ({ ok: true, tenantId: 'publication', userId: 'payer', context: undefined }),
      store, turnStore: createMemoryTurnEventStore(), log: () => {},
      produce: () => createProtectedRuntimeChatProducer(options),
    })
    const amounts: bigint[] = []
    const recorded: unknown[] = []
    const recovery = new MemoryPaymentRecoveryStore()
    const operations = new MemoryPaymentOperations({ onSettle: async (_op, input) => { amounts.push(input.amount) }, onReclaim: async () => {} })
    const app = createAgentGateway({
      resolveAgent: async () => ({ id: 'composed', ownerId: 'owner', slug: 'composed', enabled: true,
        pricePerTokenUsd: 0.001, platformFeePercent: 0, sandboxEndpoint: null, remoteSandboxId: null, remoteBearerToken: null }),
      authorizeConsumer: async () => ({ allow: true }), recordUsage: async event => { recorded.push(event) },
      maxOutputTokens: 100, defaultOutputTokens: 100, unauthenticatedInputTokenBound: 1000,
      getSandbox: async () => ({ streamPrompt: () => streamChatRouteAsSandboxEvents({
        routes, request: new Request('https://synthetic.test/api/chat'), payload: { workspaceId: 'publication', threadId: 'payer-thread', content: options.prompt },
        waitUntil: promise => { pending.push(promise) },
      }) }),
      x402: { operatorAddress: '0x1', chainId: 1, demoMode: true, paymentProtocolVersion: 2, paymentOperations: operations },
      paymentRecovery: { store: recovery, retryDelayMs: 1 }, a2a: false,
    })
    const commitment = `0x${'cd'.repeat(32)}`
    const result = await app.request('/composed/chat/completions', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': JSON.stringify({
        commitment, signature: '0xsig', operator: '0x1', amount: '10000000', nonce: '47',
        expiry: String(Math.floor(Date.now() / 1000) + 600),
      }) }, body: JSON.stringify({ messages: [{ role: 'user', content: options.prompt }], stream: false }),
    })
    const body = await result.text()
    await Promise.all(pending)
    expect(result.status, body).toBe(500)
    expect(inference).toHaveBeenCalledOnce()
    expect(effect).toHaveBeenCalledOnce()
    expect(settle).toHaveBeenCalledOnce()
    expect(amounts).toEqual([5000n])
    expect(recorded).toHaveLength(1)
    expect(recorded[0]).toMatchObject({ inputTokens: 3, outputTokens: 2, providerCostUsd: 0.0001, toolCallCount: 1 })
    expect((await recovery.get(`x402:${commitment}:47`))?.state).toBe('reconciled')
    expect(body).not.toContain('scoped-fixture-token')
  })
})
