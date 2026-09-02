# @tangle-network/agent-gateway

Hono middleware that turns any Tangle agent app into a paid API.
It exposes one shared request pipeline for API keys, x402 SpendAuth, and MPP credentials, with scope enforcement, per-key rate limits, nonce replay protection, prompt-injection detection, and publish routes for the marketplace.

## Install

```bash
npm install @tangle-network/agent-gateway
```

## Usage

```ts
import {
  createAgentGateway,
  createApiKeyRequestClaim,
  createApiKeyUsageSettlement,
  recoverPayments,
  SqlApiKeyStore,
  SqlGatewayUsageStore,
  SqlPaymentRecoveryStore,
  verifyApiKeyFromStore,
} from '@tangle-network/agent-gateway'
import { Hono } from 'hono'

const app = new Hono()
const paymentRecoveryStore = new SqlPaymentRecoveryStore(sqlAdapter)
await paymentRecoveryStore.migrate()
const apiKeyStore = new SqlApiKeyStore(sqlAdapter)
await apiKeyStore.migrate()
const usageStore = new SqlGatewayUsageStore(sqlAdapter)
await usageStore.migrate()
app.route('/v1/agents', createAgentGateway({
  resolveAgent: loadPublishedAgent,
  getSandbox: openAgentSandbox,
  recordUsage: usageStore.recordUsage,
  claimApiKeyRequest: createApiKeyRequestClaim(apiKeyStore),
  settlePayment: createApiKeyUsageSettlement(apiKeyStore),
  x402: {
    operatorAddress: '0x…',
    chainId: 3799,
    currencyDecimals: 6,
    verifySigner: verifySpendAuthSignature,
    paymentProtocolVersion: 2,
    paymentOperations,
    authorizePayment: reserveSpendAuthorization,
  },
  paymentRecovery: { store: paymentRecoveryStore },
  defaultOutputTokens: 1024,
  maxOutputTokens: 4096,
  verifyApiKey: (authHeader) => verifyApiKeyFromStore(authHeader, apiKeyStore),
}))
```

Use `sqlApiKeyStoreSchemaStatements()` in deploy-time SQL migrations.
Use `sqlGatewayUsageStoreSchemaStatements()` for retry-safe usage attribution.
Use `sqlTaskStoreSchemaStatements()` for the durable A2A task table.
The store defaults match the existing `agent_api_key` table used by Tangle agent apps.
The API-key store claims each request before compute starts.
It enforces the key's rolling-minute and UTC-day request limits with durable database slots.
Concurrent workers cannot claim the same slot, and a retry with the same request ID does not consume another slot.
When `verifyApiKey` returns a minute or daily limit, configure `claimApiKeyRequest` or the request fails closed with `503`.
The SQL store retains the current and previous UTC day, then prunes older claim rows every 256 accepted requests.
The API-key store also records each usage settlement once and refuses a settlement that would exceed the spending limit.
This check runs after work completes, so it does not reserve funds before an in-flight request.
Use a payment authorization flow when the product requires a strict pre-run budget.
The usage store writes USD values as integer nanodollars instead of SQL floating-point values.

Production requires either `x402.verifySigner` or `verifyApiKey`.
API-key-only apps can omit `x402` entirely.
Set `x402.demoMode: true` only for local development and tests; that explicit mode also enables the built-in `sk_agent_*` demo key verifier.
Keep `verifySigner` free of side effects.
Use version 2's `authorizePayment` to reserve or claim funds after rate limits, content checks, and product authorization succeed.
For production version 2, set `x402.paymentProtocolVersion: 2`, provide `paymentOperations`, and return its operation from `authorizePayment`.
Production version 2 also requires a durable `paymentRecovery.store`.
Production version 1 is read-only and must not configure `authorizePayment`.
Production x402 version 1 also rejects the legacy `settlePayment` callback before it consumes a nonce.
Use version 2 whenever authorization can reserve, charge, or otherwise mutate external funds.
Run `recoverPayments(config)` from a private scheduled worker.
Every live request and worker uses a unique durable fence token.
A stale request or worker cannot update a row after another worker takes its lease.
Provider settlement, recovery, and release methods must still use the operation ID idempotently.
`paymentOperations.getPaymentOperation` must read the authoritative provider state by operation ID without changing it.
This read is required for recovery of older A2A finalization records that predate the shared payment outbox.
The operation store owns claim, execution start, receipt retention, partial settle, release, and expiry reclaim.
An executing or retained operation cannot expire into a refund.
A retained operation settles from its receipt when one exists.
If the receipt does not arrive before `receiptTimeoutMs`, recovery settles the original quoted ceiling.
The fallback never settles the payer's larger authorization amount.
Keep version 1 explicitly configured while old and new gateways coexist; shared nonce storage must reject a version 1 claim owned by a version 2 operation.
Before it calls the verifier, the gateway requires the signed amount to cover the complete filtered conversation plus the requested output limit.
The default bound includes system text, message roles, and JSON framing.
Set `inputTokenBound` when the provider adds harness, tool, workspace, or other hidden context.
The gateway rejects `max_tokens` above `maxOutputTokens` and stops the sandbox stream at the accepted limit.
An unpaid request receives `required_amount`, `currency_decimals`, and `max_output_tokens` in the 402 response.
Sandbox adapters should emit a complete `sandbox.usage` receipt.
Requests with a version 2 operation or generic MPP charge reject missing receipts.
API-key requests keep the legacy visible-token estimate path.
recordUsage must atomically upsert by event.requestId; recovery may retry an event after its acknowledgement is lost.
Explicit demo mode exposes A2A with an in-memory task store.
Production must configure an atomic durable task store; otherwise A2A returns `503` while the OpenAI surface remains available.
Older custom A2A task stores remain source-compatible at the type boundary.
The OpenAI surface stays available when such a store is configured, while A2A returns `503` until its owner supplies atomic methods.
Use an atomic task store for multi-worker production deployments.

MPP is method-specific.
Configure `mpp.authenticateCredential` for production MPP credentials.
This callback receives the decoded payload and live credential.
It returns `{ consumerId, paymentIdentity }` or `null`.
`paymentIdentity` must be a stable, non-secret processor identity.
Equivalent encodings of one credential must return the same payment identity.
It must not reserve, confirm, or consume payment.
The default `blueprintevm` method may reuse `x402.verifySigner` when its credential has the compatible x402 payload shape.
Every other method requires an `mpp.charge` lifecycle.
The lifecycle confirms payment after all request denials.
The gateway then acquires its execution fence before it returns a response or starts sandbox work.
`confirmPayment` must bind the provider operation to the supplied `operationId` before confirmation.
It must return only after it verifies final payment success.
`recoverPayment` must inspect that operation ID and must never create another charge.
An authoritative `not-found` result must fence the operation ID against a later charge.
`releasePayment` must perform an idempotent refund or release.
The live credential is passed to `confirmPayment` only on the original request.
The nonce and recovery stores persist only the SHA-256 digest of `paymentIdentity`.
`Payment-Receipt` values must contain visible ASCII only.

`NonceStore` remains source-compatible with 0.7.1 `hasSeen`/`markSeen` stores.
Payment requests now require its atomic `claim` method, including version 1.
This is a deliberate safety boundary: a check followed by a write can accept two concurrent payments.
`KvNonceStore` with plain Cloudflare KV is not atomic and is rejected by `createAgentGateway`.
Provide `KvNonceStore` an `atomicClaim` callback backed by D1, a Durable Object, or another linearizable store.
Payment paths fail closed unless the store also provides one atomic `claim` method.
The 0.7.1 `mpp.verifySigner` callback is also supported; the gateway derives a stable identity until the integration moves to `authenticateCredential`.

The same authentication, authorization, rate-limit, filtering, sandbox, settlement, and usage-recording pipeline is used by the OpenAI-compatible and A2A endpoints.
Wire protocol handlers only translate their request and response shapes.

Set `conversationMode: 'thread'` when API calls must use the app's visible conversations.
The gateway accepts an optional `X-Tangle-Thread-Id` and returns the resolved ID in the same response header.
Its authenticated `getSandbox` context contains that thread ID, the API-key identity, and the filtered messages.
An `agent-app` host can use this context to drive its normal persisted chat route instead of opening a second sandbox session.

## A2A protocol

The gateway speaks Google's A2A protocol alongside its OpenAI-compatible surface: discovery via `.well-known/agent.json`, JSON-RPC 2.0 dispatch for `message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and the four `tasks/pushNotificationConfig/*` methods. Long-horizon agents — durable tasks across worker restarts, webhook delivery on terminal state, `input-required` pauses with multi-turn continuation — are documented in [`docs/a2a-long-horizon.md`](./docs/a2a-long-horizon.md).
Production A2A task control requires `a2a.authorizeTaskAccess`; explicit demo mode is the local-test exception.
Custom production task stores must implement atomic `createIfAbsent`, `compareAndSet`, and `compareAndSetExecution` methods.
`compareAndSetExecution` must reject a renewal when the stored owner lease has expired.
Task stores must retain payment recovery metadata until reconciliation clears it.
The short-lived `gatewaySubmission` marker is not a payment recovery record and may expire with its task.
The bundled memory and SQL stores enforce this rule even after the normal task TTL.
Push destinations must use HTTPS without URL credentials.
Push delivery does not follow redirects.
Production push delivery also requires `a2a.pushUrlValidator` to reject private DNS destinations.
Production push delivery requires `a2a.webhookSecret` so every webhook has an HMAC signature.
The exported `deliverPushNotifications` function also requires a non-empty secret.
Use `deliverDemoPushNotifications` only for explicit local demo mode.
Tasks created before this release have no recorded origin and fail closed; migrate them with a verified owner binding or let them expire.
The payment claim keeps its submission lease until the atomic submitted-to-working transition.
An expired execution lease fails the working task and preserves its payment recovery markers.

## Tier

Marketplace tier of the [agent-builder](https://github.com/drewstone/tangle-agent-builder) three-tier architecture (Forge / Workbench / Marketplace). Used by every `*.tangle.tools` agent app that publishes a paid API.

## Related

- [`@tangle-network/agent-client`](https://github.com/tangle-network/agent-client) — consumer SDK for calling endpoints this gateway fronts
- [`@tangle-network/agent-eval`](https://github.com/tangle-network/agent-eval) — evaluation framework for agents published behind this gateway
- [`@tangle-network/tcloud`](https://github.com/tangle-network/tcloud) — consumer SDK for Tangle platform services (router, sandbox, browser)

## License

MIT
