/**
 * JSON-RPC 2.0 envelope parsing + response builders. Keeps the wire
 * format isolated from the method dispatcher so a fuzz harness can throw
 * malformed bodies at `parseEnvelope` directly.
 */

import {
  A2A_ERROR_CODES,
  type JSONRPCErrorResponse,
  type JSONRPCRequest,
  type JSONRPCSuccessResponse,
} from './types'

export interface EnvelopeError {
  /** id is whatever the caller sent (or null when we can't recover it). */
  id: string | number | null
  code: number
  message: string
}

/**
 * Parse an inbound body. Returns the request envelope on success OR an
 * EnvelopeError that the caller renders as a JSONRPCErrorResponse. We never
 * throw — JSON-RPC says any malformed body becomes a -32700/-32600 response.
 */
export function parseEnvelope(raw: unknown): JSONRPCRequest | EnvelopeError {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    return {
      id: null,
      code: A2A_ERROR_CODES.INVALID_REQUEST,
      message: 'request must be a JSON object',
    }
  }
  const req = raw as Record<string, unknown>
  const id = req.id === undefined ? null : (req.id as string | number | null)
  if (req.jsonrpc !== '2.0') {
    return { id, code: A2A_ERROR_CODES.INVALID_REQUEST, message: 'jsonrpc field must be "2.0"' }
  }
  if (typeof req.method !== 'string' || req.method.length === 0) {
    return { id, code: A2A_ERROR_CODES.INVALID_REQUEST, message: 'method field required' }
  }
  return {
    jsonrpc: '2.0',
    id,
    method: req.method,
    params: req.params,
  }
}

export function ok<T>(id: string | number | null, result: T): JSONRPCSuccessResponse<T> {
  return { jsonrpc: '2.0', id, result }
}

export function fail(
  id: string | number | null,
  code: number,
  message: string,
  data?: unknown,
): JSONRPCErrorResponse {
  return {
    jsonrpc: '2.0',
    id,
    error: { code, message, ...(data !== undefined ? { data } : {}) },
  }
}
