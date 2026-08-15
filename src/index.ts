export { createAgentGateway } from './middleware'
export { reclaimPayment } from './dispatch'
export {
  recoverPayment,
  recoverPayments,
  type RecoverPaymentOptions,
  type RecoverPaymentsOptions,
  type PaymentRecoveryRun,
} from './payment-recovery-worker'
export {
  verifyX402,
  verifyMpp,
  defaultVerifyApiKey,
  isApiKeyAuthEnabled,
  isMppAuthEnabled,
  mppReplayNonceKey,
  mppPaymentCredential,
  type VerifiedMppCredential,
} from './verify'
export {
  MPP_CHARGE_PROTOCOL_VERSION,
  mppPaymentOperationId,
  type MppAuthenticatedCredential,
  type MppChargeLifecycle,
  type MppChargeOperation,
  type MppChargeOperationState,
  type MppChargeRecoveryResult,
  type MppChargeRequest,
} from './mpp-payment'
export {
  PAYMENT_RECOVERY_VERSION,
  MemoryPaymentRecoveryStore,
  PaymentRecoveryFenceError,
  type PaymentRecoveryAttribution,
  type PaymentRecoveryConfig,
  type PaymentRecoveryRecord,
  type PaymentRecoveryState,
  type PaymentRecoveryStore,
  type PaymentRecoveryTarget,
  type PaymentSettlementBasis,
} from './payment-recovery'
export { SqlPaymentRecoveryStore } from './payment-recovery-sql'
export {
  PAYMENT_PROTOCOL_VERSION,
  MemoryPaymentOperations,
  type MemoryPaymentOperationsOptions,
  type PaymentAuthorizationContext,
  type PaymentOperation,
  type PaymentOperationState,
  type PaymentOperations,
  type PaymentSettlementInput,
} from './payment-operations'
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
  isAtomicNonceStore,
  type AtomicNonceStore,
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
  SandboxExecutionBudget,
  SandboxUsageReceipt,
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
export {
  type D1DatabaseLike,
  type D1StmtLike,
  d1ToSqlAdapter,
  type SqlAdapter,
  SqlTaskStore,
} from './a2a/task-store-sql'
export {
  deliverPushNotifications,
  InMemoryPushNotificationStore,
  type PushDeliveryResult,
  type PushNotificationAuthentication,
  type PushNotificationConfig,
  type PushNotificationStore,
  SqlPushNotificationStore,
  type TaskPushNotificationConfig,
} from './a2a/push-notifications'
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
  TaskPushNotificationConfigGetParams,
  TaskState,
  TaskStatus,
  TaskStatusUpdateEvent,
  TextPart,
} from './a2a/types'
export { A2A_ERROR_CODES } from './a2a/types'
