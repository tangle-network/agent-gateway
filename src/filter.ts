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
  // i18n — Spanish
  /ignor[ae]\s+(todas?\s+)?(las\s+)?(instrucciones?|indicaciones?|órdenes?)\s+(previas?|anteriores?)/i,
  /olvid[ae]\s+(todas?\s+|tus\s+)?(instrucciones?|indicaciones?)/i,
  /(ahora\s+)?(tú\s+)?eres\s+(ahora\s+)?(un|una|el|la)\s+/i,
  /finge\s+(que\s+eres|ser)\s+/i,
  // i18n — French
  /ignor(ez|e|es)\s+(toutes?\s+)?(les\s+)?(instructions?|consignes?|directives?)\s+(précédentes?|antérieures?)/i,
  /oublie(z|s)?\s+(toutes?\s+|vos\s+|tes\s+)?(instructions?|consignes?)/i,
  /(vous\s+êtes|tu\s+es)\s+(maintenant|désormais)\s+(un|une)\s+/i,
  /prétend(ez|s)?\s+(être|que\s+vous\s+êtes|que\s+tu\s+es)\s+/i,
  // i18n — German
  /ignorier(e|en|t)\s+(alle\s+)?(vorherigen?|vorigen?|bisherigen?)\s+(anweisungen|anordnungen|vorgaben)/i,
  /vergiss\s+(alle\s+|deine\s+)?(anweisungen|vorgaben|regeln)/i,
  /du\s+bist\s+(jetzt|nun)\s+(ein|eine)\s+/i,
  /tu\s+so\s+als\s+(wärst\s+du|ob\s+du)\s+/i,
]

// Cyrillic + Greek confusables that look identical to Latin letters but
// NFKC does NOT collapse (they are distinct characters in Unicode, not
// compatibility equivalents). A payload like `ignоre аll previоus
// instructiоns` (o's are Cyrillic U+043E) evades the pattern set
// entirely because `ignore` written with Cyrillic chars never matches
// the Latin `[o]` in our regexes even after NFKC.
//
// Mapping derived from Unicode's confusables.txt, intentionally limited
// to letters that routinely appear in English prompt-injection payloads.
// Not a full confusable table — adding every entry would risk mangling
// legitimate Russian/Greek content that happens to contain these
// letters. Our INJECTION_PATTERNS and redaction keywords are English
// ASCII, so collapsing to ASCII only touches payloads impersonating
// those tokens.
const HOMOGLYPHS: Record<string, string> = {
  'а': 'a', 'е': 'e', 'о': 'o', 'р': 'p',
  'с': 'c', 'у': 'y', 'х': 'x', 'і': 'i',
  'ј': 'j', 'ѕ': 's', 'һ': 'h',
  'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K',
  'М': 'M', 'Н': 'H', 'О': 'O', 'Р': 'P',
  'С': 'C', 'Т': 'T', 'Х': 'X', 'І': 'I',
  'Ј': 'J', 'Ѕ': 'S',
  'α': 'a', 'ε': 'e', 'ο': 'o', 'ρ': 'p',
  'υ': 'y', 'ν': 'v', 'ι': 'i', 'τ': 't',
  'κ': 'k', 'χ': 'x', 'η': 'n', 'μ': 'u',
  'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Ζ': 'Z',
  'Η': 'H', 'Ι': 'I', 'Κ': 'K', 'Μ': 'M',
  'Ν': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T',
  'Χ': 'X', 'Υ': 'Y',
}

function collapseHomoglyphs(text: string): string {
  let out = ''
  for (const ch of text) out += HOMOGLYPHS[ch] ?? ch
  return out
}

// Unicode normalization for REDACTION.
//
// Strip zero-widths entirely (no space substitute) so cross-character
// elisions like "v​aul​t/secret" → "vault/secret" are matched
// by the redaction keyword pass. NFKC collapses compatibility chars
// (fullwidth `ｉｇｎｏｒｅ` → `ignore`). collapseHomoglyphs catches the
// Cyrillic/Greek lookalikes NFKC leaves alone.
function normalizeUnicode(text: string): string {
  return collapseHomoglyphs(
    text.replace(/[\u200B-\u200F\u2028-\u202F\u2060\uFEFF]/g, '').normalize('NFKC'),
  )
}

// Unicode normalization for DETECTION.
//
// Two adversarial concerns diverge from redaction:
//
//   1. Between-word zero-widths ("ignore​all​previous") — the
//      attacker separates tokens with ZWSP so \s+-anchored regexes do
//      not fire. Strip-then-detect glues the tokens into one
//      unmatchable word; replace ZWSP with a real SPACE instead so
//      /ignore\s+all\s+previous/ fires.
//   2. Punctuation noise ("Ignore,,, all...previous!!!") — collapse
//      punctuation runs to a single space. Preserve `[` `]` because
//      literal `[system]` / `[INST]` are first-class injection signals
//      we match directly.
function normalizeForDetection(text: string): string {
  const spaced = text
    .replace(/[\u200B\u200C\uFEFF]/g, ' ')
    .replace(/[\u200D\u200E\u200F\u2028-\u202F\u2060]/g, '')
    .normalize('NFKC')
  return collapseHomoglyphs(spaced)
    .replace(/[(){}<>,.!?:;"`~]+/g, ' ')
    .replace(/\s+/g, ' ')
}

// Third pass: also collapse square brackets. Literal `[system]` /
// `[INST]` patterns are matched against the other two passes (which
// preserve brackets); this pass catches the OPPOSITE evasion where
// the attacker wraps tokens in brackets to split them for \s+-anchored
// regexes — e.g. `(ignore) [all] {previous} <instructions>` should
// still match the baseline pattern.
function normalizeForDetectionNoBrackets(text: string): string {
  return normalizeForDetection(text)
    .replace(/[\[\]]+/g, ' ')
    .replace(/\s+/g, ' ')
}

/**
 * Detect prompt injection attempts.
 * Returns array of matched pattern descriptions, empty if clean.
 *
 * Patterns run against TWO normalizations so both within-word and
 * between-word zero-width evasions are caught, and so `[system]` /
 * `[INST]` literals are not lost to punctuation-collapse:
 *   - normalizeUnicode    — strips zero-widths, preserves brackets
 *   - normalizeForDetection — spaces zero-widths, collapses punct
 */
export function detectInjection(content: string): string[] {
  const stripNorm = normalizeUnicode(content)
  const detectNorm = normalizeForDetection(content)
  const noBracketNorm = normalizeForDetectionNoBrackets(content)
  const matches: string[] = []

  for (const pattern of INJECTION_PATTERNS) {
    if (pattern.test(stripNorm) || pattern.test(detectNorm) || pattern.test(noBracketNorm)) {
      matches.push(pattern.source.slice(0, 60))
    }
  }

  // Check for base64-encoded injection attempts in either normalization.
  for (const norm of [stripNorm, detectNorm]) {
    const b64Matches = norm.match(/[A-Za-z0-9+/]{40,}={0,2}/g)
    if (!b64Matches) continue
    for (const b64 of b64Matches) {
      try {
        const decoded = atob(b64)
        if (INJECTION_PATTERNS.some(p => p.test(decoded))) {
          if (!matches.includes('base64-encoded injection')) {
            matches.push('base64-encoded injection')
          }
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
