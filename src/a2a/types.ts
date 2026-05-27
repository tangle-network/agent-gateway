/**
 * A2A protocol types (Google Agent-to-Agent, April 2025).
 *
 * Subset shipped by this gateway:
 *   - Discovery: AgentCard via `.well-known/agent.json`
 *   - Messaging: `message/send`, `message/stream`
 *   - Task control: `tasks/get`, `tasks/cancel`, `tasks/resubscribe`
 *   - Push: `tasks/pushNotificationConfig/{set,get,list,delete}` (gated on `pushStore`)
 *   - Multi-turn: `input-required` state + follow-up `message/send` with the same `taskId`
 *   - Capabilities: streaming = true; pushNotifications gated on config; stateTransitionHistory = false
 *   - Parts: text only on input/output (data/file parts rejected with CONTENT_TYPE_NOT_SUPPORTED)
 *
 * Deferred until a real consumer needs them: authenticated extended card,
 * data/file parts, OAuth2/mTLS auth schemes.
 */

// ── JSON-RPC 2.0 envelopes ───────────────────────────────────────────────

export interface JSONRPCRequest {
  jsonrpc: '2.0'
  id: string | number | null
  method: string
  params?: unknown
}

export interface JSONRPCSuccessResponse<T = unknown> {
  jsonrpc: '2.0'
  id: string | number | null
  result: T
}

export interface JSONRPCErrorResponse {
  jsonrpc: '2.0'
  id: string | number | null
  error: { code: number; message: string; data?: unknown }
}

export type JSONRPCResponse<T = unknown> = JSONRPCSuccessResponse<T> | JSONRPCErrorResponse

/** Standard JSON-RPC + A2A-specific codes. Negative ints per JSON-RPC spec. */
export const A2A_ERROR_CODES = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603,
  TASK_NOT_FOUND: -32001,
  TASK_NOT_CANCELABLE: -32002,
  PUSH_NOT_SUPPORTED: -32003,
  UNSUPPORTED_OPERATION: -32004,
  CONTENT_TYPE_NOT_SUPPORTED: -32005,
  INVALID_AGENT_RESPONSE: -32006,
  AUTHENTICATED_EXTENDED_CARD_NOT_CONFIGURED: -32007,
} as const

// ── Message parts ────────────────────────────────────────────────────────

export interface TextPart {
  kind: 'text'
  text: string
  metadata?: Record<string, unknown>
}

export interface DataPart {
  kind: 'data'
  data: Record<string, unknown>
  metadata?: Record<string, unknown>
}

export interface FilePart {
  kind: 'file'
  file: { name?: string; mimeType?: string; bytes?: string; uri?: string }
  metadata?: Record<string, unknown>
}

export type Part = TextPart | DataPart | FilePart

// ── Message + Task + Artifact ────────────────────────────────────────────

export interface Message {
  kind: 'message'
  role: 'user' | 'agent'
  parts: Part[]
  messageId: string
  taskId?: string
  contextId?: string
  metadata?: Record<string, unknown>
}

export type TaskState =
  | 'submitted'
  | 'working'
  | 'input-required'
  | 'completed'
  | 'canceled'
  | 'failed'
  | 'rejected'
  | 'auth-required'

export interface TaskStatus {
  state: TaskState
  message?: Message
  timestamp: string
}

export interface Artifact {
  artifactId: string
  name?: string
  description?: string
  parts: Part[]
  metadata?: Record<string, unknown>
}

export interface Task {
  kind: 'task'
  id: string
  contextId: string
  status: TaskStatus
  history?: Message[]
  artifacts?: Artifact[]
  metadata?: Record<string, unknown>
}

// ── Streaming events (carried as JSON-RPC result over SSE) ──────────────

export interface TaskStatusUpdateEvent {
  kind: 'status-update'
  taskId: string
  contextId: string
  status: TaskStatus
  /** True on the terminal event; clients close the stream after this. */
  final: boolean
  metadata?: Record<string, unknown>
}

export interface TaskArtifactUpdateEvent {
  kind: 'artifact-update'
  taskId: string
  contextId: string
  artifact: Artifact
  /** True when this artifact's parts should be appended to the prior emit (incremental streaming). */
  append?: boolean
  /** True on the artifact's final chunk. */
  lastChunk?: boolean
  metadata?: Record<string, unknown>
}

export type StreamingEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent

// ── Method-specific params ───────────────────────────────────────────────

export interface MessageSendParams {
  message: Message
  configuration?: {
    acceptedOutputModes?: string[]
    blocking?: boolean
    historyLength?: number
  }
}

export interface TaskIdParams {
  id: string
  metadata?: Record<string, unknown>
}

export interface TaskPushNotificationConfigGetParams {
  /** Task id whose configs are being queried. */
  id: string
  /** Specific config id to fetch. Required for `set` and `delete`; omitted for `list`. */
  pushNotificationConfigId?: string
  metadata?: Record<string, unknown>
}

// ── Agent Card ──────────────────────────────────────────────────────────

export interface AgentSkill {
  id: string
  name: string
  description: string
  tags?: string[]
  examples?: string[]
  inputModes?: string[]
  outputModes?: string[]
}

export interface AgentCapabilities {
  streaming?: boolean
  pushNotifications?: boolean
  stateTransitionHistory?: boolean
}

export interface AgentProvider {
  organization: string
  url?: string
}

export interface AgentCardAuthentication {
  /** Auth scheme names the agent accepts (e.g. 'Bearer', 'x402', 'mpp'). */
  schemes: string[]
  /** Optional human-readable hint about obtaining credentials. */
  credentials?: string
}

export interface AgentCard {
  name: string
  description: string
  /** JSON-RPC endpoint URL — clients POST methods here. */
  url: string
  version: string
  documentationUrl?: string
  provider?: AgentProvider
  capabilities: AgentCapabilities
  authentication: AgentCardAuthentication
  defaultInputModes: string[]
  defaultOutputModes: string[]
  skills: AgentSkill[]
}
