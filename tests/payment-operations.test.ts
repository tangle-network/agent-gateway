import { describe, expect, it } from 'vitest'
import { Hono } from 'hono'
import {
  dispatchSandboxStreamRich,
  maximumBillableInputTokens,
  requiredX402Amount,
} from '../src/dispatch'
import { createAgentGateway } from '../src/middleware'
import {
  MemoryPaymentOperations,
  type PaymentAuthorizationContext,
} from '../src/payment-operations'
import { MemoryNonceStore } from '../src/nonce-store'
import type {
  AgentMeta,
  GatewayConfig,
  SandboxBox,
  SandboxStreamEvent,
} from '../src/types'

const agent: AgentMeta = {
  id: 'agent-payment-tests',
  ownerId: 'owner',
  slug: 'payment-tests',
  systemPrompt: '',
  pricePerTokenUsd: 0.00002,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

function payload(amount = '1000', nonce = '1', expiry = String(Math.floor(Date.now() / 1000) + 300)) {
  return {
    commitment: `0x${'ab'.repeat(32)}`,
    signature: '0xsig',
    operator: '0x1',
    amount,
    nonce,
    expiry,
  }
}

function context(requiredAmount = 500n): PaymentAuthorizationContext {
  return {
    requestId: 'request-1',
    agentId: agent.id,
    requiredAmount,
    maxOutputTokens: 4,
    executionBudget: {
      maxInputTokens: 10,
      maxOutputTokens: 4,
      maxReasoningTokens: 4,
      maxToolTokens: 4,
      maxToolCalls: 2,
      maxProviderCostUsd: 1,
    },
  }
}

describe('version 2 payment operations', () => {
  it('has one atomic owner and refunds the unused reservation', async () => {
    const operations = new MemoryPaymentOperations()
    const results = await Promise.allSettled([
      operations.claimPayment(payload(), context()),
      operations.claimPayment(payload(), { ...context(), requestId: 'request-2' }),
    ])
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)

    const owner = results.find((result): result is PromiseFulfilledResult<Awaited<ReturnType<typeof operations.claimPayment>>> => result.status === 'fulfilled')!.value
    const settled = await operations.settlePayment(owner, {
      amount: 200n,
      totalCostUsd: 0.2,
      usage: {
        inputTokens: 1,
        outputTokens: 1,
        reasoningTokens: 1,
        toolTokens: 1,
        toolCallCount: 1,
        providerCostUsd: 0.2,
        budgetEnforced: true,
      },
    })
    expect(settled.state).toBe('settled')
    expect(settled.settledAmount).toBe(200n)
    expect(settled.refundAmount).toBe(800n)
    await expect(operations.settlePayment(owner, {
      amount: 1001n,
      totalCostUsd: 1,
      usage: { ...settledUsage(), budgetEnforced: true },
    })).rejects.toThrow()
  })

  it('reclaims a claimed operation after expiry', async () => {
    let now = 100
    let recoveredCalls = 0
    const operations = new MemoryPaymentOperations({
      now: () => now,
      onReclaim: async () => { recoveredCalls += 1 },
    })
    const owner = await operations.claimPayment(payload('1000', '2', '200'), context())
    now = 201
    const reclaimed = await operations.reclaimPayment(owner.operationId)
    expect(reclaimed.state).toBe('reclaimed')
    expect(reclaimed.refundAmount).toBe(1000n)
    expect(recoveredCalls).toBe(1)
  })

  it('recovers a release after a worker crash between state and side effect', async () => {
    let fail = true
    const operations = new MemoryPaymentOperations({
      onRelease: async () => {
        if (fail) throw new Error('worker crashed during release')
      },
      onReclaim: async () => undefined,
    })
    const owner = await operations.claimPayment(payload('1000', '11'), context())

    await expect(operations.releasePayment(owner, 'sandbox failed')).rejects.toThrow('worker crashed')
    expect(operations.get(owner.operationId)?.state).toBe('releasing')
    fail = false
    const recovered = await operations.reclaimPayment(owner.operationId)
    expect(recovered.state).toBe('released')
  })

  it('runs one settlement side effect for concurrent retries', async () => {
    let effects = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => {
        effects += 1
        await new Promise((resolve) => setTimeout(resolve, 1))
      },
    })
    const owner = await operations.claimPayment(payload('1000', '13'), context())
    const input = {
      amount: 200n,
      totalCostUsd: 0.2,
      usage: settledUsage(),
    }
    const settled = await Promise.all([
      operations.settlePayment(owner, input),
      operations.settlePayment(owner, input),
    ])
    expect(effects).toBe(1)
    expect(settled[0].state).toBe('settled')
    expect(settled[1].state).toBe('settled')
  })

  it('does not close a release when recovery has no refund proof', async () => {
    const operations = new MemoryPaymentOperations({
      onRelease: async () => { throw new Error('acknowledgement lost') },
    })
    const owner = await operations.claimPayment(payload('1000', '14'), context())
    await expect(operations.releasePayment(owner, 'sandbox failed')).rejects.toThrow()
    await expect(operations.reclaimPayment(owner.operationId)).rejects.toThrow('recovery is not configured')
    expect(operations.get(owner.operationId)?.state).toBe('releasing')
  })

  it('reclaims a claim that crashed after durable ownership but before completion', async () => {
    let now = 100
    let recoveries = 0
    const operations = new MemoryPaymentOperations({
      now: () => now,
      onClaim: async () => { throw new Error('worker crashed during claim') },
      onReclaim: async () => { recoveries += 1 },
    })

    await expect(operations.claimPayment(payload('1000', '12', '200'), context())).rejects.toThrow('worker crashed')
    expect(operations.get('x402:0x' + 'ab'.repeat(32) + ':12')?.state).toBe('claiming')
    now = 201
    const recovered = await operations.reclaimPayment('x402:0x' + 'ab'.repeat(32) + ':12')
    expect(recovered.state).toBe('reclaimed')
    expect(recoveries).toBe(1)
  })
})

describe('bounded request pricing and sandbox receipts', () => {
  it('quotes conservative UTF-8 input and hidden provider costs', () => {
    expect(maximumBillableInputTokens({ ...agent, systemPrompt: '' }, '😀')).toBe(4)
    expect(requiredX402Amount(0.000001, 4, 2, 6, 3, 5, 0.00002)).toBe(20n)
  })

  it.each([
    ['honors max output', false],
    ['ignores max output', true],
  ])('passes and enforces maxOutputTokens when a Sandbox implementation %s', async (_name, ignoresLimit) => {
    let received: Record<string, unknown> | undefined
    const box: SandboxBox = {
      async *streamPrompt(_message, opts) {
        received = opts as unknown as Record<string, unknown>
        yield {
          type: 'message.part.updated',
          data: { part: { type: 'text' }, delta: ignoresLimit ? '01234567890123456789' : 'abcd' },
        }
        yield {
          type: 'sandbox.usage',
          data: {
            usage: {
              inputTokens: 1,
              outputTokens: ignoresLimit ? 6 : 1,
              reasoningTokens: 0,
              toolTokens: 0,
              toolCallCount: 0,
              providerCostUsd: 0.00002,
              budgetEnforced: !ignoresLimit,
            },
          },
        }
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      x402: { operatorAddress: '0x1', chainId: 1, demoMode: true },
      executionBudget: {
        maxReasoningTokens: 2,
        maxToolTokens: 2,
        maxToolCalls: 1,
        maxProviderCostUsd: 1,
      },
    }
    const run = async () => {
      const events = []
      for await (const event of dispatchSandboxStreamRich(agent, 'hi', 'consumer', config, undefined, undefined, 4)) {
        events.push(event)
      }
      return events
    }
    if (ignoresLimit) await expect(run()).rejects.toThrow(/max output|budget/)
    else expect(await run()).toHaveLength(3)
    expect(received?.maxOutputTokens).toBe(4)
  })

  it('rejects hidden reasoning, tool, and provider spend beyond the receipt budget', async () => {
    const box: SandboxBox = {
      async *streamPrompt() {
        yield {
          type: 'sandbox.usage',
          data: {
            usage: {
              inputTokens: 1,
              outputTokens: 1,
              reasoningTokens: 3,
              toolTokens: 3,
              toolCallCount: 2,
              providerCostUsd: 2,
              budgetEnforced: true,
            },
          },
        }
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      x402: { operatorAddress: '0x1', chainId: 1, demoMode: true },
      executionBudget: {
        maxReasoningTokens: 2,
        maxToolTokens: 2,
        maxToolCalls: 1,
        maxProviderCostUsd: 1,
      },
    }
    const run = async () => {
      for await (const _event of dispatchSandboxStreamRich(agent, 'hi', 'consumer', config, undefined, undefined, 4)) {
        // The receipt is intentionally consumed only to exercise the generator.
      }
    }
    await expect(run()).rejects.toThrow(/reasoning|tool|provider/)
  })

  it('bounds output before delivery and retains payment ownership after an over-limit stream', async () => {
    let releases = 0
    const operations = new MemoryPaymentOperations({ onRelease: async () => { releases += 1 } })
    const box: SandboxBox = {
      async *streamPrompt() {
        yield {
          type: 'message.part.updated',
          data: { part: { type: 'text' }, delta: '0123456789abcdefghijklmnop' },
        }
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      maxOutputTokens: 4,
      defaultOutputTokens: 4,
      x402: {
        operatorAddress: '0x1',
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
    }
    const app = new Hono()
    app.route('/v1/agents', createAgentGateway(config))
    const response = await app.request('/v1/agents/payment-tests/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': JSON.stringify({
          ...payload('1000000000', '15'),
          operator: '0x1',
        }),
      },
      body: JSON.stringify({ max_tokens: 4, messages: [{ role: 'user', content: 'hi' }] }),
    })
    const wire = await response.text()
    expect(wire).toContain('sandbox exceeded max output tokens')
    expect(wire).not.toContain('0123456789abcdefghijklmnop')
    expect(releases).toBe(0)
    expect(operations.get(`x402:${'0x' + 'ab'.repeat(32)}:15`)?.state).toBe('claimed')
  })

  it('retains hidden usage when a final provider receipt omits it', async () => {
    const box: SandboxBox = {
      async *streamPrompt() {
        yield { type: 'task.reasoning', data: { reasoning: { tokens: 1 } } }
        yield { type: 'task.tool.updated', data: { tool: { inputTokens: 1, outputTokens: 0 } } }
        yield {
          type: 'sandbox.usage',
          data: {
            usage: {
              inputTokens: 1,
              outputTokens: 1,
              reasoningTokens: 0,
              toolTokens: 0,
              toolCallCount: 0,
              providerCostUsd: 0.1,
              budgetEnforced: true,
            },
          },
        }
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      x402: { operatorAddress: '0x1', chainId: 1, demoMode: true },
      executionBudget: {
        maxReasoningTokens: 2,
        maxToolTokens: 2,
        maxToolCalls: 1,
        maxProviderCostUsd: 1,
      },
    }
    const run = async () => {
      const events = []
      for await (const _event of dispatchSandboxStreamRich(agent, 'hi', 'consumer', config, undefined, undefined, 4)) {
        events.push(_event)
      }
      return events.find((event) => event.kind === 'usage')
    }
    await expect(run()).resolves.toMatchObject({
      kind: 'usage',
      usage: { reasoningTokens: 1, toolTokens: 1, toolCallCount: 1 },
    })
  })

  it('does not let a lower final receipt erase earlier hidden spend or a failed budget flag', async () => {
    const box: SandboxBox = {
      async *streamPrompt() {
        yield {
          type: 'sandbox.usage',
          data: {
            usage: {
              inputTokens: 1,
              outputTokens: 1,
              reasoningTokens: 5,
              toolTokens: 4,
              toolCallCount: 2,
              providerCostUsd: 0.9,
              budgetEnforced: false,
            },
          },
        }
        yield {
          type: 'sandbox.usage',
          data: {
            usage: {
              inputTokens: 1,
              outputTokens: 1,
              reasoningTokens: 0,
              toolTokens: 0,
              toolCallCount: 0,
              providerCostUsd: 0.1,
              budgetEnforced: true,
            },
          },
        }
      },
    }
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => box,
      recordUsage: async () => undefined,
      x402: { operatorAddress: '0x1', chainId: 1, demoMode: true },
      executionBudget: {
        maxReasoningTokens: 10,
        maxToolTokens: 10,
        maxToolCalls: 10,
        maxProviderCostUsd: 1,
      },
    }
    const run = async () => {
      const events = []
      for await (const event of dispatchSandboxStreamRich(agent, 'hi', 'consumer', config, undefined, undefined, 4)) {
        events.push(event)
      }
      return events.find((event) => event.kind === 'usage')
    }
    await expect(run()).rejects.toThrow(/budget|provider|reasoning|tool/)
  })
})

function settledUsage() {
  return {
    inputTokens: 1,
    outputTokens: 1,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd: 0,
    budgetEnforced: true,
  }
}
