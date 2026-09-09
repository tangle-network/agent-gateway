import { DatabaseSync } from 'node:sqlite'
import { createHash } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { SqlApiKeyStore } from '../src/api-key-store-sql'
import { createAgentGateway } from '../src/middleware'
import { createApiKeyRequestClaim, createApiKeyUsageSettlement, verifyApiKeyFromStore } from '../src/api-keys'
import type { SandboxBox, SandboxStreamEvent } from '../src/types'
import { prepareApiKeyPrompt, ApiKeyBudgetUnsupportedError } from '../src/api-key-budget'
import { dispatchSandboxStreamRich } from '../src/dispatch-sandbox'

async function fixture(cap: number | null = 1) {
  const db = new DatabaseSync(':memory:')
  const adapter = {
    async exec(sql: string, params: unknown[] = []) { return { rowsAffected: Number(db.prepare(sql).run(...params as never[]).changes) } },
    async query<T>(sql: string, params: unknown[] = []) { return db.prepare(sql).all(...params as never[]) as T[] },
  }
  const store = new SqlApiKeyStore(adapter)
  await store.migrate()
  const token = 'ak_synthetic_fixture'
  const key = await store.create('owner', { name: 'fixture', keyHash: createHash('sha256').update(token).digest('hex'), keyPrefix: 'ak_', scopes: ['chat'], rateLimit: 60, dailyLimit: 100, spendingLimitCents: cap, expiresAt: null })
  return { db, store, adapter, token, key }
}


function gateway(store: SqlApiKeyStore, box: SandboxBox, maxProviderCostUsd = 0.01) {
  return createAgentGateway({
      resolveAgent: async () => ({ id: 'agent', ownerId: 'owner', slug: 'agent', enabled: true, pricePerTokenUsd: 0, platformFeePercent: 0, sandboxEndpoint: null, remoteSandboxId: null, remoteBearerToken: null }),
      authorizeConsumer: async () => ({ allow: true }),
      verifyApiKey: header => verifyApiKeyFromStore(header, store),
      claimApiKeyRequest: createApiKeyRequestClaim(store), apiKeyReservationLifecycle: store.reservations,
      settlePayment: createApiKeyUsageSettlement(store), recordUsage: async () => {}, getSandbox: async () => box,
      executionBudget: { maxProviderCostUsd }, a2a: false,
    })
}

function request(app: ReturnType<typeof createAgentGateway>, token: string) {
  return app.request('/agent/chat/completions', { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ messages: [{ role: 'user', content: 'x' }], stream: true }) })
}

describe('API key spending reservations', () => {
  it.each([true, false])('settles a failed run from its enforced receipt (stream=%s)', async (stream) => {
    const { db, store, token } = await fixture(20)
    const app = gateway(store, {
      async *streamPrompt() { throw new Error('Unbounded execution is forbidden') },
      async prepareBudgetedPrompt() { return { status: 'prepared', start: async function* () {
        yield { type: 'sandbox.usage', data: { usage: { inputTokens: 0, outputTokens: 1, toolCallCount: 0, providerCostUsd: 0.02, budgetEnforced: true } } }
        yield { type: 'error', data: { message: 'Tool failed after paid inference' } }
      } } },
    }, 0.1)
    try {
      const response = await app.request('/agent/chat/completions', { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ messages: [{ role: 'user', content: 'x' }], stream }) })
      expect(await response.text()).toContain('Tool failed after paid inference')
      expect((await store.list('owner'))[0].spentCents).toBe(2)
      expect(db.prepare('SELECT cost_cents FROM agent_api_key_usage').get()).toMatchObject({ cost_cents: 2 })
      expect((await store.claimRequest((await store.list('owner'))[0].id, 'remaining', undefined, 18)).allowed).toBe(true)
    } finally { db.close() }
  })

  it.each([undefined, false])('retains failed-run ownership without enforced usage (%s)', async (budgetEnforced) => {
    const { db, store, token } = await fixture()
    const app = gateway(store, {
      async *streamPrompt() { throw new Error('Unbounded execution is forbidden') },
      async prepareBudgetedPrompt() { return { status: 'prepared', start: async function* () {
        if (budgetEnforced !== undefined) yield { type: 'sandbox.usage', data: { usage: { inputTokens: 0, outputTokens: 1, toolCallCount: 0, providerCostUsd: 0.002, budgetEnforced } } }
        yield { type: 'error', data: { message: 'Tool failed after paid inference' } }
      } } },
    })
    try {
      expect(await (await request(app, token)).text()).toContain('Tool failed after paid inference')
      expect((await store.list('owner'))[0].spentCents).toBe(0)
      expect(db.prepare('SELECT state FROM agent_api_key_reservation').get()).toMatchObject({ state: 'executing' })
      expect((await request(app, token)).status).toBe(429)
    } finally { db.close() }
  })

  it.each(['preparation', 'execution-start'])('removes abort forwarding when %s fails', async (failure) => {
    const controller = new AbortController()
    const added = vi.spyOn(controller.signal, 'addEventListener')
    const removed = vi.spyOn(controller.signal, 'removeEventListener')
    const budget = { maxInputTokens: 1, maxOutputTokens: 1, maxReasoningTokens: 0, maxToolTokens: 0, maxToolCalls: 0, maxProviderCostUsd: 0.01 }
    const box: SandboxBox = {
      async *streamPrompt() { throw new Error('Unbounded execution is forbidden') },
      async prepareBudgetedPrompt() {
        if (failure === 'preparation') throw new Error('Preparation failed')
        return { status: 'prepared', start: async function* () { throw new Error('Execution must not start') } }
      },
    }
    const drain = async () => {
      for await (const _event of dispatchSandboxStreamRich(
        { id: 'agent', ownerId: 'owner', slug: 'agent', enabled: true, pricePerTokenUsd: 0, platformFeePercent: 0, sandboxEndpoint: null, remoteSandboxId: null, remoteBearerToken: null },
        'x', 'apikey:key', { resolveAgent: async () => null, authorizeConsumer: async () => ({ allow: true }), getSandbox: async () => box, recordUsage: async () => {}, x402: { demoMode: true } },
        controller.signal, undefined, 1, async () => { throw new Error('Start failed') }, false, undefined, 1, undefined,
        { consumerId: 'apikey:key', paymentMethod: 'apikey', keyInfo: null, requestId: 'request', messages: [], apiKeyReservation: { cents: 1, executionBudget: budget } },
      )) { /* Drain the stream until its expected failure. */ }
    }
    await expect(drain()).rejects.toThrow()
    expect(added).toHaveBeenCalledOnce()
    expect(removed).toHaveBeenCalledWith('abort', added.mock.calls[0][1])
  })

  it.each([undefined, null, {}, { status: 'other' }, { status: 'prepared', start: true }])(
    'rejects malformed preparation %j without starting either execution path', async (result) => {
      const { db, store, token } = await fixture()
      let unboundedStarts = 0
      const app = gateway(store, {
        async *streamPrompt() { unboundedStarts++; yield { type: 'finish' } },
        // Reflect models a JavaScript host that bypasses the return type.
        prepareBudgetedPrompt: async () => Reflect.apply(() => result, undefined, []),
      })
      try {
        const response = await request(app, token)
        expect(await response.text()).toContain('api_key.execution_budget_unsupported')
        expect(unboundedStarts).toBe(0)
        expect(db.prepare('SELECT state FROM agent_api_key_reservation').get()).toMatchObject({ state: 'released' })
      } finally { db.close() }
    },
  )

  it('adds reservation storage without rewriting existing keys or settled usage', async () => {
    const { db, store, key } = await fixture(10)
    try {
      db.exec('DROP TABLE agent_api_key_reservation')
      db.prepare('UPDATE agent_api_key SET spent_cents = 2 WHERE id = ?').run(key.id)
      db.prepare('INSERT INTO agent_api_key_usage VALUES (?, ?, ?, ?)').run('old-receipt', key.id, 3, 1)
      const before = await store.list('owner')
      await store.migrate()
      await store.migrate()
      expect(await store.list('owner')).toEqual(before)
      expect((await store.claimRequest(key.id, 'new', undefined, 6)).allowed).toBe(false)
      expect((await store.claimRequest(key.id, 'fits', undefined, 5)).allowed).toBe(true)
    } finally { db.close() }
  })

  it('rejects a missing quote when a key has acquired a finite cap', async () => {
    const { db, store, key } = await fixture(null)
    try {
      await store.claimRequest(key.id, 'uncapped')
      db.prepare('UPDATE agent_api_key SET spending_limit_cents = 1 WHERE id = ?').run(key.id)
      await expect(store.claimRequest(key.id, 'newly-capped')).rejects.toThrow('require a spending reservation quote')
    } finally { db.close() }
  })

  it('retains a handoff with no receipt even when the adapter fails before visible output', async () => {
    const { db, store, token } = await fixture()
    const app = gateway(store, {
      async *streamPrompt() { throw new Error('Unbounded execution is forbidden') },
      async prepareBudgetedPrompt() { return { status: 'prepared', start: async function* () { throw new DOMException('Provider stream aborted', 'AbortError') } } },
    })
    try {
      const response = await request(app, token)
      await response.text()
      expect(db.prepare('SELECT state FROM agent_api_key_reservation').get()).toMatchObject({ state: 'executing' })
      expect((await request(app, token)).status).toBe(429)
      expect((await store.list('owner'))[0].spentCents).toBe(0)
    } finally { db.close() }
  })

  it('releases an unsupported gateway reservation before compute', async () => {
    const { db, store, token } = await fixture()
    let started = 0
    const app = gateway(store, { async *streamPrompt() { started++; yield { type: 'finish' } } })
    try {
      for (let n = 0; n < 2; n++) {
        const response = await request(app, token)
        expect(await response.text()).toContain('api_key.execution_budget_unsupported')
      }
      expect(started).toBe(0)
      expect(db.prepare("SELECT count(*) AS n FROM agent_api_key_reservation WHERE state <> 'released'").get()).toMatchObject({ n: 0 })
    } finally { db.close() }
  })

  it('fails closed for unsupported capped hosts and leaves uncapped hosts usable', async () => {
    let started = 0
    const box: SandboxBox = { async *streamPrompt() { started++; yield { type: 'finish' } } }
    const budget = { maxInputTokens: 1, maxOutputTokens: 1, maxReasoningTokens: 0, maxToolTokens: 0, maxToolCalls: 0, maxProviderCostUsd: 0.01 }
    const context = { consumerId: 'apikey:key', paymentMethod: 'apikey' as const, keyInfo: null, requestId: 'request', messages: [], apiKeyReservation: { cents: 1, executionBudget: budget } }
    await expect(prepareApiKeyPrompt(box, 'x', 'apikey:key', undefined, budget, new AbortController().signal, undefined, context)).rejects.toBeInstanceOf(ApiKeyBudgetUnsupportedError)
    expect(started).toBe(0)
    const uncapped = await prepareApiKeyPrompt(box, 'x', 'apikey:key', undefined, budget, new AbortController().signal)
    expect(uncapped.prepared).toBeUndefined()
    for await (const _event of box.streamPrompt('x', uncapped.promptOptions)) { /* Drain the legacy adapter. */ }
    expect(started).toBe(1)
  })

  it('prevents duplicate starts, settlement above the reservation, and revoked execution', async () => {
    const { db, store, key } = await fixture(10)
    try {
      await store.claimRequest(key.id, 'one', undefined, 5)
      const starts = await Promise.allSettled([store.reservations.begin(key.id, 'one'), store.reservations.begin(key.id, 'one')])
      expect(starts.filter(result => result.status === 'fulfilled')).toHaveLength(1)
      await expect(store.recordUsage(key.id, 6, 'one')).rejects.toThrow()
      expect((await store.list('owner'))[0].spentCents).toBe(0)
      await store.claimRequest(key.id, 'two', undefined, 5)
      await store.delete('owner', key.id)
      await expect(store.reservations.begin(key.id, 'two')).rejects.toThrow()
    } finally { db.close() }
  })

  it('admits only one competing full quote across independent stores', async () => {
    const { db, store, adapter, key } = await fixture()
    try {
      const other = new SqlApiKeyStore(adapter)
      const claims = await Promise.all([store.claimRequest(key.id, 'one', undefined, 1), other.claimRequest(key.id, 'two', undefined, 1)])
      expect(claims.filter(claim => claim.allowed)).toHaveLength(1)
      expect(claims.find(claim => !claim.allowed)?.reason).toBe('spending')
    } finally { db.close() }
  })

  it('reconciles once, rejects altered replay, and releases only unused pre-execution holds', async () => {
    const { db, store, key } = await fixture(10)
    try {
      await store.claimRequest(key.id, 'one', undefined, 10)
      await store.reservations.begin(key.id, 'one')
      await store.reservations.release(key.id, 'one')
      expect((await store.claimRequest(key.id, 'two', undefined, 1)).allowed).toBe(false)
      await store.recordUsage(key.id, 3, 'one')
      await store.recordUsage(key.id, 3, 'one')
      await expect(store.recordUsage(key.id, 4, 'one')).rejects.toThrow('different usage')
      expect((await store.claimRequest(key.id, 'two', undefined, 7)).allowed).toBe(true)
      await store.reservations.release(key.id, 'two')
      await store.reservations.release(key.id, 'two')
      expect((await store.claimRequest(key.id, 'three', undefined, 7)).allowed).toBe(true)
      await expect(store.reservations.begin(key.id, 'two')).rejects.toThrow()
      expect((await store.list('owner'))[0].spentCents).toBe(3)
    } finally { db.close() }
  })

  it('retains holds after rate-counter pruning and rechecks expiry before execution', async () => {
    const { db, store, key } = await fixture()
    try {
      await store.claimRequest(key.id, 'one', undefined, 1)
      db.exec('DELETE FROM agent_api_key_request')
      expect((await store.claimRequest(key.id, 'two', undefined, 1)).allowed).toBe(false)
      db.prepare('UPDATE agent_api_key SET expires_at = 1 WHERE id = ?').run(key.id)
      await expect(store.reservations.begin(key.id, 'one')).rejects.toThrow()
      await store.reservations.release(key.id, 'one')
      expect(db.prepare('SELECT state FROM agent_api_key_reservation').get()).toMatchObject({ state: 'released' })
    } finally { db.close() }
  })

  it('runs an enforcing prepared adapter once and rejects competing provider work', async () => {
    const { db, store, token } = await fixture()
    let started = 0
    let release!: () => void
    const gate = new Promise<void>(resolve => { release = resolve })
    let entered!: () => void
    const firstStarted = new Promise<void>(resolve => { entered = resolve })
    const box: SandboxBox = {
      async *streamPrompt() { throw new Error('Unbounded path must never execute') },
      async prepareBudgetedPrompt(_message, options) {
        const cost = 0.01
        if (cost > options.executionBudget.maxProviderCostUsd) return { status: 'unsupported', reason: 'Provider minimum exceeds budget' }
        return { status: 'prepared', start: async function* (): AsyncIterable<SandboxStreamEvent> {
          started++; entered(); await gate
          // This synthetic provider checks the full call price before it executes.
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid' } }
          yield { type: 'sandbox.usage', data: { usage: { inputTokens: 0, outputTokens: 1, reasoningTokens: 0, toolTokens: 0, toolCallCount: 0, providerCostUsd: cost, budgetEnforced: true } } }
        } }
      },
    }
    const app = gateway(store, box)
    try {
      const first = await request(app, token)
      const body = first.text()
      await firstStarted
      const second = await request(app, token)
      expect(second.status).toBe(429)
      expect(started).toBe(1)
      release()
      expect(await body).toContain('paid')
      expect((await store.list('owner'))[0].spentCents).toBe(1)
    } finally { release(); db.close() }
  })
})
