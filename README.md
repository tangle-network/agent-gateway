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
  verifyApiKeyFromStore,
} from '@tangle-network/agent-gateway'
import { Hono } from 'hono'

const app = new Hono()
app.route('/v1/agents', createAgentGateway({
  resolveAgent: loadPublishedAgent,
  getSandbox: openAgentSandbox,
  recordUsage: recordUsageEvent,
  x402: {
    operatorAddress: '0x…',
    chainId: 3799,
    currencyDecimals: 6,
    verifySigner: verifySpendAuthSignature,
    authorizePayment: reserveSpendAuthorization,
  },
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
The operation store owns claim, execution start, receipt retention, partial settle, release, and expiry reclaim.
An executing or retained operation cannot expire into a refund.
A retained operation can settle later when recovery obtains its usage receipt.
Keep version 1 explicitly configured while old and new gateways coexist; shared nonce storage must reject a version 1 claim owned by a version 2 operation.
Before it calls the verifier, the gateway requires the signed amount to cover filtered input plus the requested output limit.
The gateway rejects `max_tokens` above `maxOutputTokens` and stops the sandbox stream at the accepted limit.
An unpaid request receives `required_amount`, `currency_decimals`, and `max_output_tokens` in the 402 response.
Sandbox adapters should emit a complete `sandbox.usage` receipt.
Version 2 payment operations reject missing receipts; legacy adapters use visible-token estimates only.

MPP is method-specific.
Configure `mpp.verifySigner` for production MPP credentials; it receives the decoded JSON payload when available plus the original decoded credential, and returns the authenticated consumer ID or `null`.
The default `blueprintevm` method may reuse `x402.verifySigner` when its credential has the compatible x402 payload shape.
Other methods are not accepted until they have their own verifier.

The same authentication, authorization, rate-limit, filtering, sandbox, settlement, and usage-recording pipeline is used by the OpenAI-compatible and A2A endpoints.
Wire protocol handlers only translate their request and response shapes.

## A2A protocol

The gateway speaks Google's A2A protocol alongside its OpenAI-compatible surface: discovery via `.well-known/agent.json`, JSON-RPC 2.0 dispatch for `message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and the four `tasks/pushNotificationConfig/*` methods. Long-horizon agents — durable tasks across worker restarts, webhook delivery on terminal state, `input-required` pauses with multi-turn continuation — are documented in [`docs/a2a-long-horizon.md`](./docs/a2a-long-horizon.md).
Production A2A task control requires `a2a.authorizeTaskAccess`; explicit demo mode is the local-test exception.
Custom production task stores must implement atomic `createIfAbsent` and `compareAndSet` methods.
Push destinations must use HTTPS without URL credentials.

## Tier

Marketplace tier of the [agent-builder](https://github.com/drewstone/tangle-agent-builder) three-tier architecture (Forge / Workbench / Marketplace). Used by every `*.tangle.tools` agent app that publishes a paid API.

## Related

- [`@tangle-network/agent-client`](https://github.com/tangle-network/agent-client) — consumer SDK for calling endpoints this gateway fronts
- [`@tangle-network/agent-eval`](https://github.com/tangle-network/agent-eval) — evaluation framework for agents published behind this gateway
- [`@tangle-network/tcloud`](https://github.com/tangle-network/tcloud) — consumer SDK for Tangle platform services (router, sandbox, browser)

## License

MIT
