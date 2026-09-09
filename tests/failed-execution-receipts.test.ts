import { InMemoryTaskStore } from '../src/a2a/task-store'
import { ServerAssignedTaskStore } from './server-assigned-task-store'
import { describe, expect, it } from 'vitest'
import { createAgentGateway } from '../src/middleware'
import { MemoryPaymentOperations } from '../src/payment-operations'
import { MemoryPaymentRecoveryStore } from '../src/payment-recovery'
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

async function request(config: GatewayConfig, stream = false) {
  const response = await createAgentGateway(config).request('/inclusive/chat/completions', {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': JSON.stringify({
      commitment, signature: '0xsig', operator: '0x1', amount: '1000000', nonce: '42',
      expiry: String(Math.floor(Date.now() / 1000) + 600),
    }) }, body: JSON.stringify({ stream, messages: [{ role: 'user', content: 'test' }] }),
  })
  return { status: response.status, text: await response.text() }
}

describe('failed execution receipts', () => {
  it.each([false, true])('settles measured usage while returning an error (stream=%s)', async stream => {
    const f = fixture()
    f.config.getSandbox = async () => ({ async *streamPrompt() {
      yield { type: 'usage', data: { usage: totals } }
      yield { type: 'error', data: { message: 'Tool failed after paid inference' } }
    } })
    const response = await request(f.config, stream)
    expect(response.text).toContain('Tool failed after paid inference')
    if (!stream) expect(response.status).toBe(500)
    expect(f.settlements).toEqual([16000n])
    expect(f.usageEvents).toHaveLength(1)
    const record = await f.recovery.get(`x402:${commitment}:42`)
    expect(record?.state).toBe('reconciled')
    expect(record?.usage).toEqual(totals)
  })

  it('recovers a lost settlement acknowledgement from the failed run receipt exactly once', async () => {
    const f = fixture()
    const amounts: bigint[] = []
    const operations = new MemoryPaymentOperations({ onSettle: async (_op, input) => {
      amounts.push(input.amount)
      throw new Error('settlement acknowledgement lost')
    }, onReclaim: async () => {} })
    f.config.x402.paymentOperations = operations
    f.config.getSandbox = async () => ({ async *streamPrompt() {
      yield { type: 'usage', data: { usage: totals } }
      yield { type: 'session.run.failed', data: { message: 'Paid tool failure' } }
    } })
    expect((await request(f.config)).status).toBe(500)
    const id = `x402:${commitment}:42`
    expect((await f.recovery.get(id))?.usage).toEqual(totals)
    expect((await recoverPayment(id, f.config, { force: true }))?.state).toBe('reconciled')
    await recoverPayment(id, f.config, { force: true })
    expect(amounts).toEqual([16000n])
    expect(operations.get(id)?.settledAmount).toBe(16000n)
    expect(f.usageEvents).toHaveLength(1)
  })

  it.each(['message/send', 'message/stream'])('keeps an A2A task failed after reconciling its measured payment (%s)', async method => {
    const f = fixture()
    const tasks = new ServerAssignedTaskStore(new InMemoryTaskStore(), 'failed-paid-task')
    f.config.a2a = { taskStore: tasks, authorizeTaskAccess: async () => true }
    f.config.getSandbox = async () => ({ async *streamPrompt() {
      yield { type: 'usage', data: { usage: totals } }
      yield { type: 'error', data: { message: 'Paid tool failure' } }
    } })
    const response = await createAgentGateway(f.config).request('/inclusive', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': JSON.stringify({
        commitment, signature: '0xsig', operator: '0x1', amount: '1000000', nonce: '42',
        expiry: String(Math.floor(Date.now() / 1000) + 600),
      }) }, body: JSON.stringify({ jsonrpc: '2.0', id: 'failed', method, params: {
        message: { kind: 'message', role: 'user', messageId: 'one', parts: [{ kind: 'text', text: 'test' }] },
      } }),
    })
    expect(await response.text()).toContain(method === 'message/send' ? 'Paid tool failure' : '"state":"failed"')
    expect((await tasks.get('failed-paid-task'))?.status.state).toBe('failed')
    expect(f.settlements).toEqual([16000n])
    expect((await f.recovery.get(`x402:${commitment}:42`))?.state).toBe('reconciled')
  })
})
