import { Hono } from 'hono'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { InMemoryTaskStore } from '../src/a2a/task-store'
import { createAgentGateway } from '../src/middleware'
import type {
  MppChargeLifecycle,
  MppChargeOperation,
} from '../src/mpp-payment'
import { MemoryNonceStore } from '../src/nonce-store'
import {
  MemoryPaymentOperations,
  type PaymentOperation,
  type PaymentSettlementInput,
} from '../src/payment-operations'
import {
  MemoryPaymentRecoveryStore,
  type PaymentRecoveryRecord,
} from '../src/payment-recovery'
import { recoverPayment, recoverPayments } from '../src/payment-recovery-worker'
import type {
  AgentMeta,
  GatewayConfig,
  GatewayUsageEvent,
  SandboxBox,
  SandboxUsageReceipt,
} from '../src/types'

const operatorAddress = '0x1111111111111111111111111111111111111111'
const commitment = `0x${'ab'.repeat(32)}`

const agent: AgentMeta = {
  id: 'agent-recovery',
  ownerId: 'owner-recovery',
  slug: 'recovery',
  systemPrompt: '',
  pricePerTokenUsd: 0.000001,
  platformFeePercent: 0.2,
  sandboxEndpoint: null,
  remoteSandboxId: null,
  remoteBearerToken: null,
  enabled: true,
}

afterEach(() => {
  vi.useRealTimers()
})

function spendAuth(nonce: string, amount = '1000000000'): string {
  return JSON.stringify({
    commitment,
    signature: '0xsig',
    operator: operatorAddress,
    amount,
    nonce,
    expiry: String(Math.floor(Date.now() / 1000) + 600),
  })
}

function mppCredential(value: Record<string, unknown>): string {
  return Buffer.from(JSON.stringify(value)).toString('base64url')
}

function rawMppCredential(value: string): string {
  return Buffer.from(value).toString('base64url')
}

function receipt(): SandboxUsageReceipt {
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

function mount(config: GatewayConfig): Hono {
  const app = new Hono()
  app.route('/v1/agents', createAgentGateway(config))
  return app
}

function requestBody(message = 'run') {
  return JSON.stringify({ max_tokens: 4, messages: [{ role: 'user', content: message }] })
}

function pendingMppRecovery(
  id: string,
  now: number,
  nextAttemptAt = now,
): PaymentRecoveryRecord {
  return {
    version: 1,
    id,
    revision: 0,
    state: 'claiming',
    payment: { kind: 'mpp-charge', method: 'stripe', operationId: id },
    attribution: {
      requestId: `request-${id}`,
      agentId: agent.id,
      agentSlug: agent.slug,
      consumerId: 'stripe:customer',
      paymentMethod: 'mpp',
      startMs: now,
      pricePerTokenUsd: agent.pricePerTokenUsd,
      platformFeePercent: agent.platformFeePercent,
      requiredAmount: '1',
      currencyDecimals: 6,
      maxOutputTokens: 1,
      executionBudget: {
        maxInputTokens: 1,
        maxOutputTokens: 1,
        maxReasoningTokens: 0,
        maxToolTokens: 0,
        maxToolCalls: 0,
        maxProviderCostUsd: 0.000001,
      },
    },
    workStarted: false,
    usageRecorded: false,
    attempts: 0,
    nextAttemptAt,
    createdAt: now,
    updatedAt: now,
  }
}

function mppRecoveryConfig(
  store: MemoryPaymentRecoveryStore,
  charge: MppChargeLifecycle,
  timing: { leaseMs?: number; retryDelayMs?: number } = {},
): GatewayConfig {
  return {
    resolveAgent: async () => agent,
    getSandbox: async () => ({ async *streamPrompt() {} }),
    recordUsage: async () => undefined,
    x402: { operatorAddress, chainId: 1, demoMode: true },
    mpp: {
      realm: 'gateway.test',
      method: 'stripe',
      authenticateCredential: async () => ({ consumerId: 'unused', paymentIdentity: 'unused' }),
      charge,
    },
    paymentRecovery: { store, ...timing },
  }
}

class HostileChargeLifecycle implements MppChargeLifecycle {
  readonly protocolVersion = 1 as const
  readonly operations = new Map<string, MppChargeOperation>()
  confirmations = 0
  refunds = 0
  recovered = 0
  confirmGate?: Promise<void>
  confirmStarted?: () => void
  loseConfirmAcknowledgement = false
  loseReleaseAcknowledgement = false
  receivedCredential?: string
  receiptValue?: string

  async confirmPayment(request: Parameters<MppChargeLifecycle['confirmPayment']>[0]) {
    this.confirmations += 1
    this.receivedCredential = request.credential
    this.confirmStarted?.()
    await this.confirmGate
    const operation: MppChargeOperation = {
      protocolVersion: 1,
      operationId: request.operationId,
      acquiredByRequestId: request.requestId,
      method: request.method,
      receipt: this.receiptValue ?? `stripe-receipt=${request.operationId}`,
      state: 'confirmed',
    }
    this.operations.set(operation.operationId, operation)
    if (this.loseConfirmAcknowledgement) throw new Error('Stripe confirmation acknowledgement lost')
    return operation
  }

  async releasePayment(operation: MppChargeOperation) {
    const existing = this.operations.get(operation.operationId)
    if (existing?.state === 'released') return existing
    this.refunds += 1
    const released: MppChargeOperation = { ...operation, state: 'released' }
    this.operations.set(operation.operationId, released)
    if (this.loseReleaseAcknowledgement) {
      this.loseReleaseAcknowledgement = false
      throw new Error('Stripe refund acknowledgement lost')
    }
    return released
  }

  async recoverPayment(operationId: string) {
    this.recovered += 1
    return this.operations.get(operationId) ?? { operationId, state: 'not-found' as const }
  }
}

class FailOnceRecoveryStore extends MemoryPaymentRecoveryStore {
  private failed = false

  override async compareAndSet(
    expected: PaymentRecoveryRecord,
    next: PaymentRecoveryRecord,
  ): Promise<boolean> {
    if (!this.failed && next.state === 'releasing') {
      this.failed = true
      throw new Error('payment metadata unavailable')
    }
    return super.compareAndSet(expected, next)
  }
}

describe('generic MPP charge lifecycle', () => {
  it('confirms only after all denials and before any response, body, or sandbox work', async () => {
    const deniedLifecycle = new HostileChargeLifecycle()
    let deniedRuns = 0
    const denied = mount({
      resolveAgent: async () => agent,
      getSandbox: async () => ({ async *streamPrompt() { deniedRuns += 1 } }),
      recordUsage: async () => undefined,
      authorizeConsumer: async () => ({ allow: false, reason: 'not a member', code: 'denied' }),
      x402: { operatorAddress, chainId: 1, demoMode: true },
      mpp: {
        realm: 'gateway.test',
        method: 'stripe',
        authenticateCredential: async (payload) => ({
          consumerId: 'stripe:customer',
          paymentIdentity: String(payload.sharedPaymentToken),
        }),
        charge: deniedLifecycle,
      },
      paymentRecovery: { store: new MemoryPaymentRecoveryStore() },
    })
    const credential = mppCredential({ sharedPaymentToken: 'spt_secret' })
    const deniedResponse = await denied.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${credential}`,
      },
      body: requestBody(),
    })
    expect(deniedResponse.status).toBe(403)
    expect(deniedLifecycle.confirmations).toBe(0)
    expect(deniedRuns).toBe(0)

    const lifecycle = new HostileChargeLifecycle()
    let confirmStarted!: () => void
    const confirmationStarted = new Promise<void>((resolve) => { confirmStarted = resolve })
    lifecycle.confirmStarted = confirmStarted
    let allowConfirmation!: () => void
    lifecycle.confirmGate = new Promise<void>((resolve) => { allowConfirmation = resolve })
    let runs = 0
    let legacySettlements = 0
    const app = mount({
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'paid' } }
          yield { type: 'sandbox.usage', data: { usage: receipt() } }
        },
      }),
      recordUsage: async () => undefined,
      settlePayment: async () => { legacySettlements += 1 },
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
    })
    let responseResolved = false
    const responsePromise = app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${credential}`,
      },
      body: requestBody(),
    }).then((response) => {
      responseResolved = true
      return response
    })
    await confirmationStarted
    await Promise.resolve()
    expect(responseResolved).toBe(false)
    expect(runs).toBe(0)
    allowConfirmation()
    const response = await responsePromise
    expect(response.status).toBe(200)
    expect(response.headers.get('Payment-Receipt')).toContain('stripe-receipt=')
    expect(lifecycle.receivedCredential).toContain('spt_secret')
    expect(await response.text()).toContain('paid')
    expect(runs).toBe(1)
    expect(lifecycle.confirmations).toBe(1)
    expect(legacySettlements).toBe(0)
  })

  it('recovers lost confirmation and refund acknowledgements without work or a second charge', async () => {
    const lifecycle = new HostileChargeLifecycle()
    lifecycle.loseConfirmAcknowledgement = true
    lifecycle.loseReleaseAcknowledgement = true
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let runs = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({ async *streamPrompt() { runs += 1 } }),
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
      paymentRecovery: { store: recoveryStore, retryDelayMs: 1 },
    }
    const app = mount(config)
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${mppCredential({ sharedPaymentToken: 'spt_ack_loss' })}`,
      },
      body: requestBody(),
    })
    expect(response.status).toBe(402)
    expect(runs).toBe(0)
    expect(lifecycle.confirmations).toBe(1)

    const [record] = await recoveryStore.listDue(Number.MAX_SAFE_INTEGER, 10)
    expect(record?.state).toBe('claiming')
    await expect(recoverPayment(record.id, config, { force: true })).rejects.toThrow(
      'Stripe refund acknowledgement lost',
    )
    expect((await recoveryStore.get(record.id))?.state).toBe('releasing')
    const recovered = await recoverPayment(record.id, config, { force: true })
    expect(recovered?.state).toBe('reconciled')
    expect(lifecycle.confirmations).toBe(1)
    expect(lifecycle.refunds).toBe(1)
    expect(runs).toBe(0)
  })

  it('starts each batch lease from the time that row begins recovery', async () => {
    vi.useFakeTimers()
    const startedAt = new Date('2026-08-14T00:00:00.000Z').getTime()
    vi.setSystemTime(startedAt)
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const leaseRemaining: number[] = []
    const lifecycle: MppChargeLifecycle = {
      protocolVersion: 1,
      async confirmPayment() {
        throw new Error('confirmation is not used during recovery')
      },
      async releasePayment(operation) {
        return operation
      },
      async recoverPayment(operationId) {
        if (operationId === 'slow') vi.setSystemTime(startedAt + 11)
        if (operationId === 'next') {
          const current = await recoveryStore.get(operationId)
          leaseRemaining.push((current?.lease?.expiresAt ?? 0) - Date.now())
        }
        return { operationId, state: 'not-found' }
      },
    }
    const config = mppRecoveryConfig(recoveryStore, lifecycle, { leaseMs: 10 })
    await recoveryStore.createIfAbsent(pendingMppRecovery('slow', startedAt, startedAt - 2))
    await recoveryStore.createIfAbsent(pendingMppRecovery('next', startedAt, startedAt - 1))

    expect(await recoverPayments(config)).toMatchObject({ scanned: 2, reconciled: 2 })
    expect(leaseRemaining).toEqual([10])
  })

  it('starts the retry delay after a failed provider recovery call', async () => {
    vi.useFakeTimers()
    const startedAt = new Date('2026-08-14T00:00:00.000Z').getTime()
    vi.setSystemTime(startedAt)
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const lifecycle: MppChargeLifecycle = {
      protocolVersion: 1,
      async confirmPayment() {
        throw new Error('confirmation is not used during recovery')
      },
      async releasePayment(operation) {
        return operation
      },
      async recoverPayment() {
        vi.setSystemTime(startedAt + 11)
        throw new Error('provider unavailable')
      },
    }
    const config = mppRecoveryConfig(recoveryStore, lifecycle, { retryDelayMs: 10 })
    await recoveryStore.createIfAbsent(pendingMppRecovery('failed', startedAt))

    await expect(recoverPayment('failed', config)).rejects.toThrow('provider unavailable')
    expect((await recoveryStore.get('failed'))?.nextAttemptAt).toBe(startedAt + 21)
  })

  it('uses a fresh row clock after a supplied batch scan time', async () => {
    const suppliedNow = 1_000
    let current = suppliedNow
    const clock = () => current
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const lifecycle: MppChargeLifecycle = {
      protocolVersion: 1,
      async confirmPayment() {
        throw new Error('confirmation is not used during recovery')
      },
      async releasePayment(operation) {
        return operation
      },
      async recoverPayment() {
        current += 11
        throw new Error('provider unavailable')
      },
    }
    const config = mppRecoveryConfig(recoveryStore, lifecycle, { retryDelayMs: 10 })
    await recoveryStore.createIfAbsent(pendingMppRecovery('supplied-now', suppliedNow))

    await expect(recoverPayments(config, { now: suppliedNow, clock })).resolves.toMatchObject({
      scanned: 1,
      failed: 1,
    })
    expect((await recoveryStore.get('supplied-now'))?.nextAttemptAt).toBe(1_021)
  })

  it('fences a live request when recovery releases its expired claim lease', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-08-14T00:00:00.000Z'))
    const lifecycle = new HostileChargeLifecycle()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let observerEntered!: () => void
    const observerReady = new Promise<void>((resolve) => { observerEntered = resolve })
    let releaseObserver!: () => void
    const observerGate = new Promise<void>((resolve) => { releaseObserver = resolve })
    let runs = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          yield { type: 'sandbox.usage', data: { usage: receipt() } }
        },
      }),
      recordUsage: async () => undefined,
      observer: {
        async onPaymentVerified() {
          observerEntered()
          await observerGate
        },
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
      paymentRecovery: { store: recoveryStore, staleRequestMs: 5 },
    }
    const app = mount(config)
    const pending = app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${mppCredential({ sharedPaymentToken: 'spt_lease' })}`,
      },
      body: requestBody(),
    })
    await observerReady
    vi.advanceTimersByTime(6)

    expect(await recoverPayments(config, { now: Date.now() })).toMatchObject({
      scanned: 1,
      reconciled: 1,
    })
    releaseObserver()
    const response = await pending

    // The streaming response is committed before a second worker fences the row.
    expect(response.status).toBe(200)
    expect(runs).toBe(0)
    expect(lifecycle.confirmations).toBe(1)
    expect(lifecycle.refunds).toBe(1)
    expect([...lifecycle.operations.values()][0]?.state).toBe('released')
  })

  it('uses the adapter payment identity for replay and never persists the Stripe credential', async () => {
    const lifecycle = new HostileChargeLifecycle()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const claimedKeys = new Set<string>()
    let runs = 0
    const secret = 'spt_same_secret'
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          runs += 1
          yield { type: 'sandbox.usage', data: { usage: receipt() } }
        },
      }),
      recordUsage: async () => undefined,
      nonceStore: {
        hasSeen: async (key) => claimedKeys.has(key),
        claim: async (key) => {
          if (claimedKeys.has(key)) return false
          claimedKeys.add(key)
          return true
        },
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
    }
    const app = mount(config)
    const request = (json: string) => app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${rawMppCredential(json)}`,
      },
      body: requestBody(),
    })

    const first = await request(`{"sharedPaymentToken":"${secret}"}`)
    await first.text()
    const replay = await request(`{ "sharedPaymentToken": "${secret}" }`)
    await replay.text()

    expect(first.status).toBe(200)
    expect(replay.status).toBe(401)
    expect(lifecycle.confirmations).toBe(1)
    expect(runs).toBe(1)
    const [key] = [...claimedKeys]
    const record = await recoveryStore.get(key)
    expect(key).toMatch(/^mpp:stripe:[a-f0-9]{64}$/)
    expect(key).not.toContain(secret)
    expect(JSON.stringify(record)).not.toContain(secret)
  })

  it('retains a synchronous sandbox start failure and settles it once at the bounded ceiling', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-08-14T00:00:00.000Z'))
    const lifecycle = new HostileChargeLifecycle()
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const usage: GatewayUsageEvent[] = []
    let starts = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        streamPrompt() {
          starts += 1
          throw new Error('provider started then lost the stream')
        },
      }),
      recordUsage: async (event) => { usage.push(event) },
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
      paymentRecovery: { store: recoveryStore, receiptTimeoutMs: 10 },
    }
    const app = mount(config)
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${mppCredential({ sharedPaymentToken: 'spt_sync_start' })}`,
      },
      body: requestBody(),
    })
    expect(await response.text()).toContain('provider started then lost the stream')
    const [pending] = await recoveryStore.listDue(Number.MAX_SAFE_INTEGER, 10)

    expect(starts).toBe(1)
    expect(lifecycle.refunds).toBe(0)
    expect(pending.state).toBe('retained')
    vi.advanceTimersByTime(11)
    const recovered = await recoverPayment(pending.id, config)
    expect(recovered?.state).toBe('reconciled')
    expect(lifecycle.confirmations).toBe(1)
    expect(lifecycle.refunds).toBe(0)
    expect(usage).toHaveLength(1)
    expect(usage[0]?.settlementBasis).toBe('quoted-ceiling')
  })

  it('refunds a confirmed charge when its receipt is unsafe for an HTTP header', async () => {
    const lifecycle = new HostileChargeLifecycle()
    lifecycle.receiptValue = 'stripe-receipt=\u2603'
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let runs = 0
    const app = mount({
      resolveAgent: async () => agent,
      getSandbox: async () => ({ async *streamPrompt() { runs += 1 } }),
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
    })
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Payment stripe ${mppCredential({ sharedPaymentToken: 'spt_bad_receipt' })}`,
      },
      body: requestBody(),
    })
    const [operation] = [...lifecycle.operations.values()]

    expect(response.status).toBe(402)
    expect(runs).toBe(0)
    expect(lifecycle.confirmations).toBe(1)
    expect(lifecycle.refunds).toBe(1)
    expect(operation?.state).toBe('released')
    expect((await recoveryStore.get(operation!.operationId))?.state).toBe('reconciled')
  })
})

describe('durable OpenAI recovery', () => {
  it('rejects a v2 adapter operation with a mismatched identity and clears its claim lease', async () => {
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let sandboxCalls = 0
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          sandboxCalls += 1
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: new MemoryPaymentOperations(),
        authorizePayment: async (_payload, context): Promise<PaymentOperation> => ({
          protocolVersion: 2,
          operationId: 'adapter-operation-id',
          acquiredByRequestId: context.requestId,
          nonceKey: 'adapter-nonce',
          authorizationId: 'adapter-authorization',
          reservedAmount: 1_000_000_000n,
          settledAmount: 0n,
          refundAmount: 1_000_000_000n,
          expiresAt: Math.floor(Date.now() / 1000) + 600,
          state: 'claimed',
        }),
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore },
    }
    const app = mount(config)
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': spendAuth('906') },
      body: requestBody(),
    })
    await response.text()

    const record = await recoveryStore.get(`x402:${commitment}:906`)
    expect(response.status).toBe(402)
    expect(record?.state).toBe('claiming')
    expect(record?.lease).toBeUndefined()
    expect(record?.payment.operationId).toBe(`x402:${commitment}:906`)
    expect(sandboxCalls).toBe(0)
  })

  it('clears the active recovery lease when release metadata fails', async () => {
    const recoveryStore = new FailOnceRecoveryStore()
    const operationId = `x402:${commitment}:907`
    class FailingExecutionOperations extends MemoryPaymentOperations {
      override async beginPaymentExecution(_operation: PaymentOperation): Promise<PaymentOperation> {
        throw new Error('execution unavailable')
      }
    }
    const operations = new FailingExecutionOperations()
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({ async *streamPrompt() {} }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore },
    }
    const app = mount(config)
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Payment-Signature': spendAuth('907'),
      },
      body: requestBody(),
    })
    await response.text()

    const pending = await recoveryStore.get(operationId)
    expect(pending?.state).toBe('claimed')
    expect(pending?.lease).toBeUndefined()
    expect(operations.get(operationId)?.state).toBe('claimed')
  })

  it('persists operation and attribution before output, then heals settlement acknowledgement loss', async () => {
    const recoveryStore = new MemoryPaymentRecoveryStore()
    let settleCalls = 0
    let recoveryCalls = 0
    const operations = new MemoryPaymentOperations({
      onSettle: async () => {
        settleCalls += 1
        throw new Error('settlement acknowledgement lost')
      },
      onReclaim: async () => { recoveryCalls += 1 },
    })
    const usage = new Map<string, GatewayUsageEvent>()
    let recordAtSandboxStart: Awaited<ReturnType<typeof recoveryStore.get>>
    const operationId = `x402:${commitment}:501`
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          recordAtSandboxStart = await recoveryStore.get(operationId)
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'valuable' } }
          yield { type: 'sandbox.usage', data: { usage: receipt() } }
        },
      }),
      recordUsage: async (event) => { usage.set(event.requestId, event) },
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore, retryDelayMs: 1 },
      maxOutputTokens: 4,
      defaultOutputTokens: 4,
    }
    const app = mount(config)
    const payment = spendAuth('501')
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': payment },
      body: requestBody(),
    })
    const wire = await response.text()
    expect(response.status).toBe(200)
    expect(wire).toContain('valuable')
    expect(wire).not.toContain('[DONE]')
    expect(recordAtSandboxStart?.state).toBe('executing')
    expect(recordAtSandboxStart?.workStarted).toBe(true)
    expect(recordAtSandboxStart?.attribution.agentId).toBe(agent.id)
    expect(recordAtSandboxStart?.attribution.consumerId).toBe(commitment)
    expect(recordAtSandboxStart?.usage).toBeUndefined()

    const pending = await recoveryStore.get(operationId)
    expect(pending?.state).toBe('settling')
    expect(pending?.usage).toEqual(receipt())
    expect(pending?.usageRecorded).toBe(false)
    expect(operations.get(operationId)?.state).toBe('settling')
    expect(settleCalls).toBe(1)
    expect(usage.size).toBe(0)

    const recovered = await recoverPayment(operationId, config, { force: true })
    expect(recovered?.state).toBe('reconciled')
    expect(recovered?.usageRecorded).toBe(true)
    expect(recoveryCalls).toBe(1)
    expect(usage.size).toBe(1)
    expect(operations.get(operationId)?.state).toBe('settled')
    await recoverPayment(operationId, config, { force: true })
    expect(recoveryCalls).toBe(1)
    expect(usage.size).toBe(1)
    expect(await recoveryStore.get(operationId)).toBeDefined()

    const replay = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': payment },
      body: requestBody('replay'),
    })
    expect(replay.status).toBe(402)
  })

  it('settles a canceled no-receipt run once at the quoted ceiling after the bounded timeout', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-08-14T00:00:00.000Z'))
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const settlements: PaymentSettlementInput[] = []
    let settlementStarted!: () => void
    const settlementReady = new Promise<void>((resolve) => { settlementStarted = resolve })
    let finishSettlement!: () => void
    const settlementReleased = new Promise<void>((resolve) => { finishSettlement = resolve })
    const operations = new MemoryPaymentOperations({
      onSettle: async (_operation, input) => {
        settlements.push(input)
        settlementStarted()
        await settlementReleased
      },
      onReclaim: async () => undefined,
    })
    const usage = new Map<string, GatewayUsageEvent>()
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt(_message, options) {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'partial' } }
          await new Promise<void>((resolve) => {
            if (options?.signal?.aborted) resolve()
            else options?.signal?.addEventListener('abort', () => resolve(), { once: true })
          })
        },
      }),
      recordUsage: async (event) => { usage.set(event.requestId, event) },
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore, receiptTimeoutMs: 10, retryDelayMs: 1 },
      maxOutputTokens: 4,
      defaultOutputTokens: 4,
    }
    const app = mount(config)
    const payment = spendAuth('502')
    const response = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': payment },
      body: requestBody(),
    })
    const reader = response.body!.getReader()
    expect(new TextDecoder().decode((await reader.read()).value)).toContain('partial')
    await reader.cancel()
    for (let attempt = 0; attempt < 20; attempt += 1) await Promise.resolve()

    const operationId = `x402:${commitment}:502`
    const retained = await recoveryStore.get(operationId)
    expect(retained?.state).toBe('retained')
    expect(operations.get(operationId)?.state).toBe('retained')
    expect(settlements).toHaveLength(0)
    expect((await recoverPayments(config, { now: Date.now() })).scanned).toBe(0)

    vi.setSystemTime(Date.now() + 11)
    const firstRecovery = recoverPayment(operationId, config)
    await settlementReady
    const secondRecovery = recoverPayment(operationId, config)
    finishSettlement()
    await Promise.all([firstRecovery, secondRecovery])
    expect((await recoveryStore.get(operationId))?.state).toBe('reconciled')
    expect(settlements).toHaveLength(1)
    expect(settlements[0]?.basis).toBe('quoted-ceiling')
    expect(settlements[0]?.amount).toBe(BigInt(retained!.attribution.requiredAmount))
    expect(settlements[0]?.amount).toBeLessThan(1000000000n)
    expect(operations.get(operationId)?.state).toBe('settled')
    expect(usage.size).toBe(1)
    expect([...usage.values()][0]?.settlementBasis).toBe('quoted-ceiling')
    expect((await recoverPayments(config, { now: Date.now() })).scanned).toBe(0)
    expect(settlements).toHaveLength(1)

    const replay = await app.request('/v1/agents/recovery/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': payment },
      body: requestBody('replay'),
    })
    expect(replay.status).toBe(402)
  })

  it('never broadcasts one recovered receipt across the batch worker', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-08-14T00:00:00.000Z'))
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const settlements: PaymentSettlementInput[] = []
    const operations = new MemoryPaymentOperations({
      onSettle: async (_operation, input) => { settlements.push(input) },
      onReclaim: async () => undefined,
    })
    const usage: GatewayUsageEvent[] = []
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'partial' } }
        },
      }),
      recordUsage: async (event) => { usage.push(event) },
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore, receiptTimeoutMs: 10 },
      maxOutputTokens: 4,
      defaultOutputTokens: 4,
    }
    const app = mount(config)
    for (const nonce of ['504', '505']) {
      const response = await app.request('/v1/agents/recovery/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': spendAuth(nonce) },
        body: requestBody(),
      })
      await response.text()
      expect((await recoveryStore.get(`x402:${commitment}:${nonce}`))?.state).toBe('retained')
    }

    vi.advanceTimersByTime(11)
    const hostileOptions = {
      now: Date.now(),
      usage: receipt(),
    } as unknown as Parameters<typeof recoverPayments>[1]
    expect(await recoverPayments(config, hostileOptions)).toMatchObject({
      scanned: 2,
      reconciled: 2,
      failed: 0,
    })

    expect(settlements).toHaveLength(2)
    expect(settlements.every((settlement) => settlement.basis === 'quoted-ceiling')).toBe(true)
    expect(usage).toHaveLength(2)
    expect(usage.every((event) => event.settlementBasis === 'quoted-ceiling')).toBe(true)
    expect(usage.every((event) => event.inputTokens === 0 && event.outputTokens === 0)).toBe(true)
  })
})

describe('A2A recovery retention', () => {
  it('keeps an expired task until its no-receipt payment reconciles, then restores normal TTL', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-08-14T00:00:00.000Z'))
    const taskStore = new InMemoryTaskStore(20)
    const recoveryStore = new MemoryPaymentRecoveryStore()
    const settlements: PaymentSettlementInput[] = []
    const operations = new MemoryPaymentOperations({
      onSettle: async (_operation, input) => { settlements.push(input) },
      onReclaim: async () => undefined,
    })
    const config: GatewayConfig = {
      resolveAgent: async () => agent,
      getSandbox: async () => ({
        async *streamPrompt() {
          yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta: 'partial task' } }
        },
      }),
      recordUsage: async () => undefined,
      x402: {
        operatorAddress,
        chainId: 1,
        demoMode: true,
        paymentProtocolVersion: 2,
        paymentOperations: operations,
      },
      nonceStore: new MemoryNonceStore(),
      paymentRecovery: { store: recoveryStore, receiptTimeoutMs: 10 },
      a2a: { taskStore, authorizeTaskAccess: async () => true },
      maxOutputTokens: 4,
      defaultOutputTokens: 4,
    }
    const app = mount(config)
    const send = await app.request('/v1/agents/recovery', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': spendAuth('503') },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'message/send',
        params: {
          message: {
            kind: 'message',
            role: 'user',
            messageId: 'message-503',
            taskId: 'task-503',
            parts: [{ kind: 'text', text: 'run' }],
          },
        },
      }),
    })
    expect(send.status).toBe(200)
    expect((await taskStore.get('task-503'))?.metadata?.gatewayPaymentRecovery).toBeDefined()
    await taskStore.delete('task-503')
    expect(await taskStore.get('task-503')).toBeDefined()
    expect(operations.get(`x402:${commitment}:503`)?.state).toBe('retained')

    vi.setSystemTime(Date.now() + 21)
    expect(await taskStore.get('task-503')).toBeDefined()
    const recovered = await app.request('/v1/agents/recovery', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tasks/get', params: { id: 'task-503' } }),
    })
    const recoveredBody = await recovered.json() as { result?: { metadata?: Record<string, unknown> } }
    expect(recoveredBody.result?.metadata?.gatewayPaymentRecovery).toBeUndefined()
    expect(settlements).toHaveLength(1)
    expect(settlements[0]?.basis).toBe('quoted-ceiling')
    expect(operations.get(`x402:${commitment}:503`)?.state).toBe('settled')

    vi.setSystemTime(Date.now() + 21)
    expect(await taskStore.get('task-503')).toBeUndefined()
  })
})
