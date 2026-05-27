/**
 * A2A Message ↔ inner-agent (text-only) translation. Both directions are
 * intentionally narrow: callers send text parts, the inner sandbox produces
 * text. Data + file parts are rejected with `CONTENT_TYPE_NOT_SUPPORTED`
 * rather than silently dropped — the protocol's whole point is letting the
 * caller know exactly what an agent does and doesn't accept.
 */

import { A2A_ERROR_CODES, type Artifact, type Message } from './types'

export interface ExtractedText {
  text: string
}

export interface ExtractError {
  error: { code: number; message: string }
}

/**
 * Pull `text` out of a Message's parts. Concatenates multi-text-part messages
 * with two newlines, mirroring the OpenAI-compat path's join of `user`
 * messages. Returns an A2A error code on any non-text part — text-only is the
 * declared capability of every agent this gateway fronts.
 */
export function extractTextFromMessage(message: Message): ExtractedText | ExtractError {
  if (!message || message.kind !== 'message') {
    return {
      error: {
        code: A2A_ERROR_CODES.INVALID_PARAMS,
        message: 'params.message must be an A2A Message ({ kind: "message", role, parts, messageId })',
      },
    }
  }
  if (!Array.isArray(message.parts) || message.parts.length === 0) {
    return {
      error: {
        code: A2A_ERROR_CODES.INVALID_PARAMS,
        message: 'message.parts must be a non-empty array',
      },
    }
  }
  const texts: string[] = []
  for (const part of message.parts) {
    if (part.kind === 'text') {
      if (typeof part.text !== 'string') {
        return {
          error: {
            code: A2A_ERROR_CODES.INVALID_PARAMS,
            message: 'text part .text must be a string',
          },
        }
      }
      texts.push(part.text)
    } else {
      return {
        error: {
          code: A2A_ERROR_CODES.CONTENT_TYPE_NOT_SUPPORTED,
          message: `part.kind '${(part as { kind: string }).kind}' not supported; this agent accepts text parts only`,
        },
      }
    }
  }
  return { text: texts.join('\n\n') }
}

/**
 * Wrap the agent's final response text as an A2A Artifact for the task's
 * `artifacts` field. `name='response'` is the convention for the primary
 * model output across A2A reference servers.
 */
export function responseTextToArtifact(text: string, artifactId: string): Artifact {
  return {
    artifactId,
    name: 'response',
    parts: [{ kind: 'text', text }],
  }
}
