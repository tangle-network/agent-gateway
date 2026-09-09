import { describe, expect, it } from 'vitest'
import { createAgentGateway } from '../src/middleware'
import { MemoryPaymentOperations } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore, serializePaymentOperation } from '../src/payment-recovery'
import { recoverPayment } from '../src/payment-recovery-worker'
import type { GatewayConfig, GatewayUsageEvent, SandboxExecutionBudget, SandboxUsageReceipt } from '../src/types'

const agent = { id: 'inclusive', ownerId: 'owner', slug: 'inclusive', enabled: true,
  pricePerTokenUsd: 0.001, platformFeePercent: 0, sandboxEndpoint: null,
  remoteSandboxId: null, remoteBearerToken: null }
const commitment = `0x${'ab'.repeat(32)}`
const totals: SandboxUsageReceipt = { inputTokens: 10, outputTokens: 6,
  reasoningTokens: 2, toolTokens: 4, toolCallCount: 1, providerCostUsd: 0.001, budgetEnforced: true }

function fixture(details?: GatewayConfig['executionBudget']) {
  let received: SandboxExecutionBudget | undefined
  const settlements: bigint[] = []
  const usageEvents: GatewayUsageEvent[] = []
  const recovery = new MemoryPaymentRecoveryStore()
  const operations = new MemoryPaymentOperations({ onSettle: async (_op, input) => { settlements.push(input.amount) }, onReclaim: async () => {} })
  const config: GatewayConfig = {
    resolveAgent: async () => agent, authorizeConsumer: async () => ({ allow: true }),
    getSandbox: async () => ({ async *streamPrompt(_message, options) {
      received = options?.executionBudget
      yield { type: 'usage', data: { usage: totals } }
    } }), recordUsage: async event => { usageEvents.push(event) },
    maxOutputTokens: 20, defaultOutputTokens: 20, unauthenticatedInputTokenBound: 100,
    executionBudget: details,
    x402: { operatorAddress: '0x1', chainId: 1, demoMode: true,
      paymentProtocolVersion: 2, paymentOperations: operations },
    paymentRecovery: { store: recovery, retryDelayMs: 1 },
  }
  return { config, recovery, operations, settlements, usageEvents, received: () => received }
}

async function request(config: GatewayConfig) {
  const response = await createAgentGateway(config).request('/inclusive/chat/completions', {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': JSON.stringify({
      commitment, signature: '0xsig', operator: '0x1', amount: '1000000', nonce: '42',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }) }, body: JSON.stringify({ stream: false, messages: [{ role: 'user', content: 'test' }] }),
  })
  return { status: response.status, text: await response.text() }
}

describe('inclusive token accounting', () => {
  it('omits invented detail caps and bills provider totals once through a real payment operation', async () => {
    const f = fixture()
    expect((await request(f.config)).status).toBe(200)
    expect(f.received()).not.toHaveProperty('maxReasoningTokens')
    expect(f.received()).not.toHaveProperty('maxToolTokens')
    expect(f.received()?.maxProviderCostUsd).toBe(0.12)
    expect(f.settlements).toEqual([16000n])
    expect(f.usageEvents[0]?.totalCostUsd).toBe(0.016)
    const record = await f.recovery.get(`x402:${commitment}:42`)
    expect(record?.attribution.tokenAccounting).toBe('inclusive')
    expect(record?.attribution.requiredAmount).toBe('120000')
  })

  it('preserves and enforces explicitly supplied detail caps, including zero', async () => {
    for (const details of [{ maxReasoningTokens: 0 }, { maxToolTokens: 0 }]) {
      const f = fixture(details)
      const response = await request(f.config)
      expect(response.status).toBe(500)
      expect(response.text).toMatch(/max reasoning|max tool/)
      expect(f.received()).toMatchObject(details)
      expect(f.settlements).toEqual([])
    }
  })

  it('accepts an inclusive receipt without unmeasured detail fields and rejects it under explicit caps', async () => {
    for (const explicit of [false, true]) {
      const f = fixture(explicit ? { maxReasoningTokens: 10 } : undefined)
      f.config.getSandbox = async () => ({ async *streamPrompt() {
        yield { type: 'usage', data: { usage: { inputTokens: 10, outputTokens: 6,
          toolCallCount: 1, providerCostUsd: 0.001, budgetEnforced: true } } }
      } })
      const response = await request(f.config)
      expect(response.status, response.text).toBe(explicit ? 500 : 200)
      if (explicit) expect(response.text).toContain('explicit token cap')
    }
  })

  it.each([{ tokenAccounting: undefined, complete: true }, { tokenAccounting: 'inclusive', complete: true },
    { tokenAccounting: undefined, complete: false }] as const)('keeps persisted $tokenAccounting recovery accounting stable (complete=$complete)', async ({ tokenAccounting, complete }) => {
    const f = fixture()
    const amounts: bigint[] = []
    const operations = new MemoryPaymentOperations({ onSettle: async (_operation, input) => {
      amounts.push(input.amount)
      throw new Error('acknowledgement lost')
    }, onReclaim: async () => {} })
    f.config.x402.paymentOperations = operations
    const budget = { maxInputTokens: 100, maxOutputTokens: 20, maxToolCalls: 8, maxProviderCostUsd: 1 }
    const claimed = await operations.claimPayment({ commitment, nonce: '43', amount: '1000000',
      expiry: String(Math.floor(Date.now() / 1000) + 600) },
    { requestId: 'historical', agentId: agent.id, requiredAmount: 100000n, maxOutputTokens: 20, executionBudget: budget })
    const executing = await operations.beginPaymentExecution(claimed)
    const now = Date.now()
    await f.recovery.createIfAbsent({ version: 1, id: executing.operationId, revision: 0, state: 'settling',
      payment: { kind: 'x402', operationId: executing.operationId, operation: serializePaymentOperation(executing) },
      attribution: { ...(tokenAccounting ? { tokenAccounting } : {}), requestId: 'historical', agentId: agent.id,
        agentSlug: agent.slug, consumerId: commitment, paymentMethod: 'x402', startMs: now,
        pricePerTokenUsd: agent.pricePerTokenUsd, platformFeePercent: 0, requiredAmount: '100000',
        currencyDecimals: 6, maxOutputTokens: 20, executionBudget: budget },
      workStarted: true, usage: complete ? totals : { inputTokens: 10, outputTokens: 6, toolCallCount: 1, providerCostUsd: 0.001, budgetEnforced: true }, usageRecorded: false, attempts: 0, nextAttemptAt: now, createdAt: now, updatedAt: now })
    if (!complete) {
      await expect(recoverPayment(executing.operationId, f.config, { force: true })).rejects.toThrow('historical additive accounting requires complete token details')
      expect(amounts).toEqual([])
      expect(operations.get(executing.operationId)?.state).toBe('executing')
      return
    }
    await expect(recoverPayment(executing.operationId, f.config, { force: true })).rejects.toThrow('acknowledgement lost')
    const expected = tokenAccounting === 'inclusive' ? 16000n : 22000n
    expect(amounts).toEqual([expected])
    expect((await recoverPayment(executing.operationId, f.config, { force: true }))?.state).toBe('reconciled')
    expect(operations.get(executing.operationId)?.settledAmount).toBe(expected)
    await recoverPayment(executing.operationId, f.config, { force: true })
    expect(amounts).toEqual([expected])
  })
})
