import { describe, expect, it, vi } from 'vitest'
import { createAgentGateway } from '../src/middleware'

// Deliberately use Reflect.apply to exercise JavaScript callers that bypass
// the required TypeScript configuration field.
describe('explicit consumer authorization policy', () => {
  it.each([undefined, null, true, 'allow'])('rejects invalid policy %s before any host work', (policy) => {
    const resolveAgent = vi.fn(async () => null)
    const getSandbox = vi.fn(async () => ({
      async* streamPrompt() { yield { type: 'finish' } },
    }))
    const verifyApiKey = vi.fn(async () => null)
    const recordUsage = vi.fn(async () => {})
    expect(() => Reflect.apply(createAgentGateway, undefined, [{
      resolveAgent, getSandbox, verifyApiKey, recordUsage,
      ...(policy === undefined ? {} : { authorizeConsumer: policy }),
    }])).toThrow('authorizeConsumer must be an explicit authorization function')
    expect(resolveAgent).not.toHaveBeenCalled()
    expect(getSandbox).not.toHaveBeenCalled()
    expect(verifyApiKey).not.toHaveBeenCalled()
    expect(recordUsage).not.toHaveBeenCalled()
  })

  it.each([undefined, null, { allow: 'false' }, { allow: 1 }, {}, 'allow'])(
    'rejects malformed decision %j before request claims and sandbox access', async (decision) => {
      const getSandbox = vi.fn(async () => ({ async* streamPrompt() { yield { type: 'finish' } } }))
      const claimApiKeyRequest = vi.fn(async () => ({
        allowed: true, minuteRemaining: 1, dailyRemaining: 1,
        minuteResetAt: Date.now() + 60_000, dailyResetAt: Date.now() + 86_400_000,
      }))
      const recordUsage = vi.fn(async () => {})
      const gateway = Reflect.apply(createAgentGateway, undefined, [{
        resolveAgent: async () => ({
          id: 'agent', ownerId: 'owner', slug: 'agent', enabled: true,
          pricePerTokenUsd: 0.00002, platformFeePercent: 0.2,
          sandboxEndpoint: null, remoteSandboxId: null, remoteBearerToken: null,
        }),
        authorizeConsumer: async () => decision,
        verifyApiKey: async () => ({ keyId: 'key', consumerId: 'apikey:key', scopes: ['chat'] }),
        getSandbox, claimApiKeyRequest, recordUsage,
        a2a: false,
      }])
      const response = await gateway.request('/agent/chat/completions', {
        method: 'POST',
        headers: { Authorization: 'Bearer sk_agent_synthetic', 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: [{ role: 'user', content: 'Read private data' }] }),
      })
      expect(response.status).toBe(403)
      expect(await response.json()).toMatchObject({ error: { code: 'invalid_authorization_decision' } })
      expect(claimApiKeyRequest).not.toHaveBeenCalled()
      expect(getSandbox).not.toHaveBeenCalled()
      expect(recordUsage).not.toHaveBeenCalled()
    },
  )

})
