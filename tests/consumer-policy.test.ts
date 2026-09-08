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
})
