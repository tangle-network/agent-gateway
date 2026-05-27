export { createAgentGateway } from './middleware'
export { verifyX402, verifyMpp, defaultVerifyApiKey } from './verify'
export {
  filterConsumerMessages,
  filterConsumerMessagesStrict,
  detectInjection,
  redactSystemPromptFromOutput,
} from './filter'
export {
  checkRateLimit,
  MemoryRateLimitStore,
  KvRateLimitStore,
  type RateLimitConfig,
  type RateLimitResult,
  type RateLimitStore,
} from './rate-limit'
export {
  createApiKeyRoutes,
  verifyApiKeyFromStore,
  type ApiKey,
  type ApiKeyCreateRequest,
  type ApiKeyStore,
  type ApiKeyRoutesConfig,
} from './api-keys'
export {
  MemoryNonceStore,
  KvNonceStore,
  type NonceStore,
} from './nonce-store'
export {
  ConsoleObserver,
  CompositeObserver,
  generateRequestId,
  type GatewayObserver,
  type RequestContext,
  type AuthFailureReason,
} from './observer'
export {
  createPublishRoutes,
  type PublishedConfig,
  type PublishRequest,
  type PublishStore,
  type PublishRoutesConfig,
} from './publish'
export type {
  AgentMeta,
  PaymentMethod,
  X402Config,
  MppConfig,
  PaymentResult,
  ApiKeyInfo,
  GatewayUsageEvent,
  SandboxStreamEvent,
  SandboxBox,
  GatewayConfig,
  ChatMessage,
  ChatCompletionRequest,
  ChatCompletionChunk,
} from './types'

// --- A2A protocol surface (Google Agent-to-Agent) ---
// Types + task-store adapter. Handlers are wired automatically by
// createAgentGateway when `GatewayConfig.a2a` (or its default) is honored;
// consumers only import these to BYO a durable TaskStore (D1, postgres, DO)
// or to declare richer AgentMeta.skills for the Agent Card.
export { InMemoryTaskStore, type TaskStore } from './a2a/task-store'
export type {
  AgentCard,
  AgentCapabilities,
  AgentCardAuthentication,
  AgentProvider,
  AgentSkill,
  Artifact,
  DataPart,
  FilePart,
  JSONRPCErrorResponse,
  JSONRPCRequest,
  JSONRPCResponse,
  JSONRPCSuccessResponse,
  Message,
  MessageSendParams,
  Part,
  StreamingEvent,
  Task,
  TaskArtifactUpdateEvent,
  TaskIdParams,
  TaskState,
  TaskStatus,
  TaskStatusUpdateEvent,
  TextPart,
} from './a2a/types'
export { A2A_ERROR_CODES } from './a2a/types'
