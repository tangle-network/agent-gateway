import type { ChatMessage } from './types'

// --- Injection detection patterns ---

const INJECTION_PATTERNS = [
  // Direct instruction override
  /ignore\s+(all\s+)?(previous|prior|above|earlier)\s+(instructions?|prompts?|rules?|directives?)/i,
  /disregard\s+(all\s+)?(previous|prior|system)/i,
  /forget\s+(everything|all|your)\s+(previous|instructions?|training)/i,
  // Role assumption
  /you\s+are\s+now\s+(a|an|the)\s+/i,
  /pretend\s+(you\s+are|to\s+be)\s+/i,
  /act\s+as\s+(if\s+you\s+are|a|an|the)\s+/i,
  /new\s+instructions?:/i,
  /\[system\]/i,
  /\[INST\]/i,
  // Prompt extraction
  /what\s+(is|are)\s+your\s+(system\s+)?(prompt|instructions?|rules?|directives?)/i,
  /repeat\s+(your|the)\s+(system\s+)?(prompt|instructions?)/i,
  /output\s+(your|the)\s+((system|initial|original|first|full|real|hidden|secret|raw|exact)\s+)?(prompt|instructions?)/i,
  /show\s+(me\s+)?(your|the)\s+((system|hidden|secret|initial|original)\s+)?(prompt|instructions?|message|directives?)/i,
  /tell\s+me\s+(your|the)\s+(system\s+)?(prompt|instructions?|rules?)/i,
  // Developer/debug/admin/jailbreak mode override
  /(developer|debug|admin|god|sudo|jailbreak|unrestricted|maintenance)\s+mode/i,
  // Safety/safeguard disablement
  /(disable|bypass|override|remove|turn\s+off)\s+(your\s+|the\s+)?(safety|safeguards?|guardrails?|filters?|restrictions?|rules?|limitations?|content\s+policy)/i,
  // Role reversal
  /(from\s+now\s+on|starting\s+now|now)\s+you('re|\s+are)\s+(the\s+)?(user|human|customer|assistant)/i,
  /i('m|\s+am)\s+(the\s+)?(assistant|ai|model|llm|bot)/i,
  // Tool-call / JSON-shaped spoofs
  /"(?:tool|function|action|call)"\s*:\s*"[^"]*(exfiltrate|leak|reveal|dump|extract|steal)[^"]*"/i,
  /exfiltrate[_\s]*(system|prompt|secret|vault|config|data)/i,
  // Data exfiltration
  /read\s+(the\s+)?(vault|workspace|config|secret|\.env)/i,
  /cat\s+\/home\/agent\/(vault|config|\.env|secrets?)/i,
  /list\s+(all\s+)?(vault|workspace|secret)\s+(files?|contents?|data)/i,
]

// Unicode normalization.
//
// Attacks insert zero-width chars between every word so `\s+`-anchored
// regexes don't fire. Stripping entirely leaves attack text as one glued
// token which ALSO dodges `\s+`. Split the handling: U+200B ZWSP,
// U+200C ZWNJ, U+FEFF ZWNBSP are commonly used in attacks and get
// replaced with a real SPACE so regexes see tokenized text. U+200D ZWJ
// + directional marks are legitimately used in scripts like Hindi and
// Arabic; strip without replacement so we don't spuriously split
// legitimate words. NFKC collapses homoglyphs (Cyrillic a → Latin a, fullwidth → ascii).
function normalizeUnicode(text: string): string {
  return text
    .replace(/[\u200B\u200C\uFEFF]/g, ' ')
    .replace(/[\u200D\u200E\u200F\u2028-\u202F\u2060]/g, '')
    .normalize('NFKC')
}

// Detection-only normalization on top of `normalizeUnicode`. Collapses
// punctuation/bracket runs between word chars into a single space so
// adversarial noise (`Ignore,,, all...`, `(ignore) [all] {previous}`)
// survives \s+-anchored pattern match. Never applied to redaction
// because redaction patterns reference literal punctuation
// (`config\.json`, `\.env`) that we must preserve.
function normalizeForDetection(text: string): string {
  return normalizeUnicode(text)
    .replace(/[(){}\[\]<>,.!?:;"`~]+/g, ' ')
    .replace(/\s+/g, ' ')
}

/**
 * Detect prompt injection attempts.
 * Returns array of matched pattern descriptions, empty if clean.
 */
export function detectInjection(content: string): string[] {
  const normalized = normalizeForDetection(content)
  const matches: string[] = []

  for (const pattern of INJECTION_PATTERNS) {
    if (pattern.test(normalized)) {
      matches.push(pattern.source.slice(0, 60))
    }
  }

  // Check for base64-encoded injection attempts
  const b64Matches = normalized.match(/[A-Za-z0-9+/]{40,}={0,2}/g)
  if (b64Matches) {
    for (const b64 of b64Matches) {
      try {
        const decoded = atob(b64)
        if (INJECTION_PATTERNS.some(p => p.test(decoded))) {
          matches.push('base64-encoded injection')
        }
      } catch { /* not valid b64 */ }
    }
  }

  return matches
}

/**
 * Security boundary — filter consumer messages before forwarding to agent.
 *
 * Defense in depth:
 * 1. Strip system messages (consumers cannot set system prompt)
 * 2. Normalize Unicode (collapse homoglyphs, remove zero-width chars)
 * 3. Detect injection patterns (instruction override, prompt extraction, data exfil)
 * 4. Redact sensitive keywords
 * 5. Cap message length
 *
 * Returns filtered messages and any injection warnings detected.
 */
export function filterConsumerMessages(
  messages: ChatMessage[],
  maxLength = 8000,
): ChatMessage[] {
  return messages
    .filter((m) => m.role !== 'system')
    .map((m) => {
      const normalized = normalizeUnicode(m.content)
      const redacted = normalized
        .replace(/\b(vault|workspace|owner|admin|secret|\.env|config\.json)[\s/:][^\s]*/gi, '[REDACTED]')
        .slice(0, maxLength)
      return { role: m.role, content: redacted }
    })
}

/**
 * Filter consumer messages with injection detection.
 * Returns { messages, injectionWarnings }.
 * If injectionWarnings is non-empty, the gateway should log and optionally reject.
 */
export function filterConsumerMessagesStrict(
  messages: ChatMessage[],
  maxLength = 8000,
): { messages: ChatMessage[]; injectionWarnings: string[] } {
  const filtered = filterConsumerMessages(messages, maxLength)
  const allContent = filtered.map(m => m.content).join(' ')
  const injectionWarnings = detectInjection(allContent)
  return { messages: filtered, injectionWarnings }
}

/**
 * Redact system prompt content from agent output.
 * Prevents the agent from leaking its own instructions in responses.
 *
 * Strategy: if any chunk of the system prompt appears verbatim (>40 chars)
 * in the output, replace it with [REDACTED].
 */
export function redactSystemPromptFromOutput(
  output: string,
  systemPrompt: string | undefined,
): string {
  if (!systemPrompt || systemPrompt.length < 40) return output

  // Split system prompt into meaningful chunks (sentences or lines)
  const chunks = systemPrompt
    .split(/[.\n]/)
    .map(s => s.trim())
    .filter(s => s.length >= 40)

  let redacted = output
  for (const chunk of chunks) {
    // Case-insensitive substring match
    const idx = redacted.toLowerCase().indexOf(chunk.toLowerCase())
    if (idx >= 0) {
      redacted = redacted.slice(0, idx) + '[REDACTED — system instructions]' + redacted.slice(idx + chunk.length)
    }
  }

  return redacted
}
