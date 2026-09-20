/**
 * Fixtures shared by the end-to-end A2A suites. a2a.test.ts and
 * a2a-long-horizon.test.ts drive the same gateway over the same wire shapes
 * and only their scenarios differ, so the agent, the stub sandbox, the demo
 * key verifier and the SSE parser belong in one place.
 */
import type { JSONRPCSuccessResponse, StreamingEvent } from '../src/a2a/types'
import type { AgentMeta, ApiKeyInfo, SandboxBox, SandboxStreamEvent } from '../src/types'

export const operatorAddress = '0x1111111111111111111111111111111111111111'

export class StubSandbox implements SandboxBox {
  constructor(
    private chunks: string[],
    private opts: { delayMs?: number } = {},
  ) {}
  async *streamPrompt(): AsyncIterable<SandboxStreamEvent> {
    let output = ''
    for (const delta of this.chunks) {
      if (this.opts.delayMs) await new Promise((resolve) => setTimeout(resolve, this.opts.delayMs))
      output += delta
      yield { type: 'message.part.updated', data: { part: { type: 'text' }, delta } }
    }
    yield { type: 'sandbox.usage', data: { usage: stubUsage(output) } }
  }
}

/** Usage receipt matching `StubSandbox`'s four-characters-per-token estimate. */
export function stubUsage(output: string) {
  const outputTokens = Math.ceil(output.length / 4)
  return {
    inputTokens: 1,
    outputTokens,
    reasoningTokens: 0,
    toolTokens: 0,
    toolCallCount: 0,
    providerCostUsd: (1 + outputTokens) * 0.00002,
    budgetEnforced: true,
  }
}

export function makeAgent(overrides: Partial<AgentMeta> = {}): AgentMeta {
  return {
    id: 'agent_1',
    ownerId: 'user_owner',
    slug: 'test-agent',
    systemPrompt: 'You are a test assistant.',
    pricePerTokenUsd: 0.00002,
    platformFeePercent: 0.2,
    sandboxEndpoint: null,
    remoteSandboxId: null,
    remoteBearerToken: null,
    enabled: true,
    ...overrides,
  }
}

export async function verifyDemoApiKey(header: string): Promise<ApiKeyInfo | null> {
  const token = header.replace(/^Bearer\s+/, '')
  if (!token.startsWith('sk_agent_')) return null
  return { consumerId: `consumer_${token}`, keyId: token, scopes: ['chat'] } as ApiKeyInfo
}

export function apiKeyHeader(): Record<string, string> {
  return { Authorization: 'Bearer sk_agent_test_key_1' }
}

export function textMessage(text: string, opts: { taskId?: string; contextId?: string } = {}) {
  return {
    kind: 'message' as const,
    role: 'user' as const,
    parts: [{ kind: 'text' as const, text }],
    messageId: `msg_${Math.random().toString(36).slice(2)}`,
    ...(opts.taskId ? { taskId: opts.taskId } : {}),
    ...(opts.contextId ? { contextId: opts.contextId } : {}),
  }
}

export async function parseSseEvents(res: Response): Promise<StreamingEvent[]> {
  const body = await res.text()
  return body
    .split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => (JSON.parse(line.slice(6)) as JSONRPCSuccessResponse<StreamingEvent>).result)
}
