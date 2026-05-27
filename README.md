# @tangle-network/agent-gateway

Hono middleware that turns any Tangle agent app into a paid API. Wrap your chat endpoint to accept API keys, x402 SpendAuth, or MPP credentials — with scope enforcement, per-key rate limits, nonce replay protection, prompt-injection detection, and publish routes for the marketplace.

## Install

```bash
npm install @tangle-network/agent-gateway
```

## Usage

```ts
import { createAgentGateway } from '@tangle-network/agent-gateway'
import { Hono } from 'hono'

const app = new Hono()
app.use('/chat/*', createAgentGateway({
  apiKeyStore: myKeyStore,
  x402: { verifierUrl: 'https://router.tangle.tools/x402/verify' },
  rateLimits: { perKey: { rpm: 60 } },
}))
```

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
