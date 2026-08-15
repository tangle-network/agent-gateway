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
  recoverPayments,
  SqlPaymentRecoveryStore,
  verifyApiKeyFromStore,
} from '@tangle-network/agent-gateway'
import { Hono } from 'hono'

const app = new Hono()
const paymentRecoveryStore = new SqlPaymentRecoveryStore(sqlAdapter)
await paymentRecoveryStore.migrate()
app.route('/v1/agents', createAgentGateway({
  resolveAgent: loadPublishedAgent,
  getSandbox: openAgentSandbox,
  recordUsage: recordUsageEvent,
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

`x402.verifySigner` is required for production.
Set `x402.demoMode: true` only for local development and tests; that explicit mode also enables the built-in `sk_agent_*` demo key verifier.
Keep `verifySigner` free of side effects.
Use `authorizePayment` to reserve or claim funds after rate limits, content checks, and product authorization succeed.
For production version 2, set `x402.paymentProtocolVersion: 2`, provide `paymentOperations`, and return its operation from `authorizePayment`.
Production version 2 also requires a durable `paymentRecovery.store`.
Run `recoverPayments(config)` from a private scheduled worker.
Every live request and worker uses a unique durable fence token.
A stale request or worker cannot update a row after another worker takes its lease.
Provider settlement, recovery, and release methods must still use the operation ID idempotently.
The operation store owns claim, execution start, receipt retention, partial settle, release, and expiry reclaim.
An executing or retained operation cannot expire into a refund.
A retained operation settles from its receipt when one exists.
If the receipt does not arrive before `receiptTimeoutMs`, recovery settles the original quoted ceiling.
The fallback never settles the payer's larger authorization amount.
Keep version 1 explicitly configured while old and new gateways coexist; shared nonce storage must reject a version 1 claim owned by a version 2 operation.
Before it calls the verifier, the gateway requires the signed amount to cover filtered input plus the requested output limit.
The gateway rejects `max_tokens` above `maxOutputTokens` and stops the sandbox stream at the accepted limit.
An unpaid request receives `required_amount`, `currency_decimals`, and `max_output_tokens` in the 402 response.
Sandbox adapters should emit a complete `sandbox.usage` receipt.
Requests with a version 2 operation or generic MPP charge reject missing receipts.
API-key requests keep the legacy visible-token estimate path.
recordUsage must atomically upsert by event.requestId; recovery may retry an event after its acknowledgement is lost.
Older custom A2A task stores remain source-compatible through a process-safe fallback.
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
That legacy path is limited to non-owner payment claims; version 2 and MPP charge lifecycles require an atomic `claim` method.
The 0.7.1 `mpp.verifySigner` callback is also supported; the gateway derives a stable identity until the integration moves to `authenticateCredential`.

The same authentication, authorization, rate-limit, filtering, sandbox, settlement, and usage-recording pipeline is used by the OpenAI-compatible and A2A endpoints.
Wire protocol handlers only translate their request and response shapes.

## A2A protocol

The gateway speaks Google's A2A protocol alongside its OpenAI-compatible surface: discovery via `.well-known/agent.json`, JSON-RPC 2.0 dispatch for `message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and the four `tasks/pushNotificationConfig/*` methods. Long-horizon agents — durable tasks across worker restarts, webhook delivery on terminal state, `input-required` pauses with multi-turn continuation — are documented in [`docs/a2a-long-horizon.md`](./docs/a2a-long-horizon.md).
Production A2A task control requires `a2a.authorizeTaskAccess`; explicit demo mode is the local-test exception.
Custom production task stores must implement atomic `createIfAbsent` and `compareAndSet` methods.
Task stores must retain gateway recovery metadata until reconciliation clears it.
The bundled memory and SQL stores enforce this rule even after the normal task TTL.
Push destinations must use HTTPS without URL credentials.

## Tier

Marketplace tier of the [agent-builder](https://github.com/drewstone/tangle-agent-builder) three-tier architecture (Forge / Workbench / Marketplace). Used by every `*.tangle.tools` agent app that publishes a paid API.

## Related

- [`@tangle-network/agent-client`](https://github.com/tangle-network/agent-client) — consumer SDK for calling endpoints this gateway fronts
- [`@tangle-network/agent-eval`](https://github.com/tangle-network/agent-eval) — evaluation framework for agents published behind this gateway
- [`@tangle-network/tcloud`](https://github.com/tangle-network/tcloud) — consumer SDK for Tangle platform services (router, sandbox, browser)

## License

MIT
