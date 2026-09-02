import type { ChatMessage } from './types'

const CHAT_ROLES = new Set<ChatMessage['role']>([
  'system',
  'user',
  'assistant',
  'tool',
])

export function isChatMessageArray(value: unknown): value is ChatMessage[] {
  return Array.isArray(value) && value.length > 0 && value.every((message) => {
    if (!message || typeof message !== 'object' || Array.isArray(message)) return false
    const candidate = message as { role?: unknown; content?: unknown }
    return typeof candidate.role === 'string' &&
      CHAT_ROLES.has(candidate.role as ChatMessage['role']) &&
      typeof candidate.content === 'string'
  })
}
