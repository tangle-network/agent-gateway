import { describe, expect, it, vi } from 'vitest'

import type { AuthorizedRequest } from '../src/dispatch'
import {
  attachPaymentRecoveryMarker,
  clearPaymentRecoveryMarker,
  readPaymentRecoveryMarker,
  retainPaymentRecoveryMarker,
} from '../src/a2a/payment-recovery'
import {
  buildFinalizationRecord,
  clearFinalizationMarker,
  markUsageRecorded,
  readFinalizationRecord,
  withFinalizationRecord,
} from '../src/a2a/task-finalization'
import {
  InMemoryPushNotificationStore,
  type PushNotificationConfig,
} from '../src/a2a/push-notifications'
import { deliverTaskPush } from '../src/a2a/task-push-delivery'
import {
  clearTaskSubmission,
  readTaskOrigin,
  readTaskSubmission,
  recoverSubmissionIfNeeded,
  withTaskOrigin,
  withTaskSubmission,
} from '../src/a2a/task-submission-recovery'
import { InMemoryTaskStore } from '../src/a2a/task-store'
import type { Task } from '../src/a2a/types'

function makeTask(id: string, state: Task['status']['state'] = 'submitted'): Task {
  return {
    kind: 'task',
    id,
    contextId: `ctx-${id}`,
    status: { state, timestamp: new Date().toISOString() },
    history: [],
  }
}

function makeAuthz(): AuthorizedRequest {
  const agent = {
    id: 'agent-1',
    ownerId: 'owner-1',
    slug: 'agent-1',
    pricePerTokenUsd: 0.00002,
    platformFeePercent: 0.2,
    sandboxEndpoint: null,
    remoteSandboxId: null,
    remoteBearerToken: null,
    enabled: true,
  }
  return {
    agent,
    consumerId: 'consumer-1',
    paymentMethod: 'none',
    keyInfo: null,
    userMessage: 'hello',
    rateLimitRemaining: undefined,
    requestId: 'request-1',
    startMs: Date.now(),
    maxOutputTokens: 16,
    executionBudget: {
      maxInputTokens: 16,
      maxOutputTokens: 16,
      maxReasoningTokens: 16,
      maxToolTokens: 16,
      maxToolCalls: 2,
      maxProviderCostUsd: 1,
    },
    requiredPaymentAmount: 0n,
    paymentPayload: null,
  }
}

const receipt = {
  inputTokens: 1,
  outputTokens: 2,
  reasoningTokens: 0,
  toolTokens: 0,
  toolCallCount: 0,
  providerCostUsd: 0.00006,
  budgetEnforced: true,
}

describe('A2A extracted state machines', () => {
  it('preserves task origin while clearing only the submission lease', () => {
    const task = makeTask('submission')
    const withOrigin = withTaskOrigin(task.metadata, { id: 'agent-1', slug: 'agent-1' })
    const submitted = {
      ...task,
      metadata: withTaskSubmission(withOrigin, {
        agent: { id: 'agent-1', slug: 'agent-1' },
        requestId: 'request-1',
        consumerId: 'consumer-1',
      }),
    }

    expect(readTaskOrigin(submitted)).toMatchObject({ agentId: 'agent-1', agentSlug: 'agent-1' })
    expect(readTaskSubmission(submitted)).toMatchObject({ requestId: 'request-1' })
    expect(readTaskSubmission(clearTaskSubmission(submitted))).toBeUndefined()
    expect(readTaskOrigin(clearTaskSubmission(submitted))).toMatchObject({ agentId: 'agent-1' })
  })

  it('fails an expired submission and delivers its terminal transition once', async () => {
    const store = new InMemoryTaskStore()
    const task = makeTask('expired-submission')
    const metadata = withTaskSubmission(task.metadata, {
      agent: { id: 'agent-1', slug: 'agent-1' },
      requestId: 'request-1',
      consumerId: 'consumer-1',
    })
    const expired = {
      ...task,
      metadata: {
        ...metadata,
        gatewaySubmission: {
          ...(metadata.gatewaySubmission as Record<string, unknown>),
          lease: { id: 'expired', expiresAt: 0 },
        },
      },
    }
    await store.put(expired)
    const delivered: Task[] = []

    const recovered = await recoverSubmissionIfNeeded(expired, {
      taskStore: store,
      deliverPush: async (deliveredTask) => {
        delivered.push(deliveredTask)
      },
    })

    expect(recovered.status.state).toBe('failed')
    expect(recovered.metadata?.gatewaySubmission).toBeUndefined()
    expect(recovered.metadata?.gatewaySubmissionRecovery).toBeDefined()
    expect(delivered).toHaveLength(1)
  })

  it('claims a successful push delivery once across repeated terminal reads', async () => {
    const taskStore = new InMemoryTaskStore()
    const pushStore = new InMemoryPushNotificationStore()
    const fetcher = vi.fn(async () => new Response(null, { status: 200 }))
    const config: PushNotificationConfig = { id: 'webhook-1', url: 'https://example.test/done' }
    const task = makeTask('push-once', 'completed')
    await taskStore.put(task)
    await pushStore.set(task.id, config)

    const deps = { taskStore, pushStore, demoMode: true, fetcher }
    await deliverTaskPush((await taskStore.get(task.id))!, deps)
    await deliverTaskPush((await taskStore.get(task.id))!, deps)

    expect(fetcher).toHaveBeenCalledTimes(1)
    expect((await taskStore.get(task.id))?.metadata?.gatewayPushDelivery).toMatchObject({
      claims: { 'webhook-1': 'completed' },
    })
  })

  it('does not claim a push rejected by URL policy, then retries it', async () => {
    const taskStore = new InMemoryTaskStore()
    const pushStore = new InMemoryPushNotificationStore()
    const fetcher = vi.fn(async () => new Response(null, { status: 200 }))
    const urlValidator = vi.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const task = makeTask('push-retry', 'failed')
    await taskStore.put(task)
    await pushStore.set(task.id, { id: 'webhook-1', url: 'https://example.test/retry' })

    const deps = { taskStore, pushStore, demoMode: true, fetcher, urlValidator }
    await deliverTaskPush((await taskStore.get(task.id))!, deps)
    expect((await taskStore.get(task.id))?.metadata?.gatewayPushDelivery).toBeUndefined()
    await deliverTaskPush((await taskStore.get(task.id))!, deps)

    expect(urlValidator).toHaveBeenCalledTimes(2)
    expect(fetcher).toHaveBeenCalledTimes(1)
  })

  it('records and clears finalization usage without changing the task contract', async () => {
    const store = new InMemoryTaskStore()
    const authz = makeAuthz()
    const task = makeTask('finalization', 'working')
    const record = buildFinalizationRecord(authz, receipt, null, false, undefined)
    const marked = withFinalizationRecord(task, record)
    await store.put(marked)

    const recorded = await markUsageRecorded(store, marked)
    expect(readFinalizationRecord(recorded)?.usageRecorded).toBe(true)
    expect(clearFinalizationMarker(recorded).metadata?.gatewayFinalizing).toBeUndefined()
  })

  it('attaches, retains, and clears a payment recovery identity atomically', async () => {
    const store = new InMemoryTaskStore()
    const task = makeTask('payment-marker')
    await store.put(task)

    const attached = await attachPaymentRecoveryMarker(store, task, 'recovery-1')
    const retained = await retainPaymentRecoveryMarker(store, attached, 'recovery-1')
    expect(readPaymentRecoveryMarker(retained)).toEqual({ version: 1, id: 'recovery-1' })
    expect(readPaymentRecoveryMarker(clearPaymentRecoveryMarker(retained))).toBeUndefined()
  })
})
