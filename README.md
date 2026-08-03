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
    verifySigner: verifySpendAuthSignature,
  },
  verifyApiKey: (authHeader) => verifyApiKeyFromStore(authHeader, apiKeyStore),
}))
```

`x402.verifySigner` is required for production.
Set `x402.demoMode: true` only for local development and tests; that explicit mode also enables the built-in `sk_agent_*` demo key verifier.

MPP is method-specific.
Configure `mpp.verifySigner` for production MPP credentials; it receives the decoded JSON payload when available plus the original decoded credential, and returns the authenticated consumer ID or `null`.
The default `blueprintevm` method may reuse `x402.verifySigner` when its credential has the compatible x402 payload shape.
Other methods are not accepted until they have their own verifier.

The same authentication, authorization, rate-limit, filtering, sandbox, settlement, and usage-recording pipeline is used by the OpenAI-compatible and A2A endpoints.
Wire protocol handlers only translate their request and response shapes.

## A2A protocol

The gateway speaks Google's A2A protocol alongside its OpenAI-compatible surface: discovery via `.well-known/agent.json`, JSON-RPC 2.0 dispatch for `message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and the four `tasks/pushNotificationConfig/*` methods. Long-horizon agents — durable tasks across worker restarts, webhook delivery on terminal state, `input-required` pauses with multi-turn continuation — are documented in [`docs/a2a-long-horizon.md`](./docs/a2a-long-horizon.md).

## Tier

Marketplace tier of the [agent-builder](https://github.com/drewstone/tangle-agent-builder) three-tier architecture (Forge / Workbench / Marketplace). Used by every `*.tangle.tools` agent app that publishes a paid API.

## Related

- [`@tangle-network/agent-client`](https://github.com/tangle-network/agent-client) — consumer SDK for calling endpoints this gateway fronts
- [`@tangle-network/agent-eval`](https://github.com/tangle-network/agent-eval) — evaluation framework for agents published behind this gateway
- [`@tangle-network/tcloud`](https://github.com/tangle-network/tcloud) — consumer SDK for Tangle platform services (router, sandbox, browser)

## License

MIT
