# @tangle-network/agent-gateway

Hono middleware that turns any Tangle agent app into a paid API.

One shared request pipeline for API keys, x402 SpendAuth, and MPP credentials — with scope enforcement, per-key rate limits, nonce replay protection, prompt-injection detection, usage billing, and Google A2A protocol support.

## Install

```bash
npm install @tangle-network/agent-gateway hono
```

Hono is a required peer dependency. Use the application's existing compatible Hono installation so the application and gateway share one router type identity. The supported range is declared in `package.json`.

## Contents

- [Quickstart](#quickstart) — the minimal gateway in 30 seconds
- [Authentication methods](#authentication-methods) — API keys, x402, MPP
- [A2A protocol](#a2a-protocol) — Google's agent-to-agent surface
- [Payment recovery](./docs/payment-recovery.md) — the durable outbox
- [API key details](./docs/api-keys.md) — scopes, reservations, rate limits
- [A2A details](./docs/a2a-protocol.md) — task stores, push notifications

## Quickstart

The minimal gateway: an agent behind API-key authentication.

```ts
import {
  createAgentGateway,
  SqlApiKeyStore,
  SqlGatewayUsageStore,
  verifyApiKeyFromStore,
} from '@tangle-network/agent-gateway'
import { Hono } from 'hono'

const app = new Hono()
const apiKeyStore = new SqlApiKeyStore(sqlAdapter)
const usageStore = new SqlGatewayUsageStore(sqlAdapter)

app.route('/v1/agents', createAgentGateway({
  resolveAgent: loadPublishedAgent,
  getSandbox: openAgentSandbox,
  authorizeConsumer: authorizeAgentAccess,   // required: your access policy
  recordUsage: usageStore.recordUsage,
  verifyApiKey: (authHeader) => verifyApiKeyFromStore(authHeader, apiKeyStore),
}))
```

Every gateway requires an explicit `authorizeConsumer` policy. Authentication or payment does not grant access to a private workspace.

## Authentication methods

| Method | How | Who uses it |
|---|---|---|
| **API key** | `Authorization: Bearer ak_...` | paying customers with a Tangle account |
| **x402 SpendAuth** | signed EIP-712 payment in the request | anonymous per-call payment (no account) |
| **MPP credential** | method-specific credential + charge lifecycle | marketplace payments |

All three share the same downstream pipeline: rate limits → content checks → authorization → sandbox → settlement → usage recording.

Production requires at least one of `x402.verifySigner` or `verifyApiKey`. API-key-only apps can omit `x402` entirely.

## A2A protocol

The gateway speaks Google's A2A protocol alongside its OpenAI-compatible surface: discovery via `.well-known/agent.json`, JSON-RPC 2.0 for `message/send`, `message/stream`, task management, and push notifications.

Set `a2a: false` when the application does not expose A2A routes. See [docs/a2a-protocol.md](./docs/a2a-protocol.md) for task stores, push delivery, and long-horizon agents.

## Consumers call this with agent-client

[`@tangle-network/agent-client`](https://github.com/tangle-network/agent-client) is the consumer SDK for the endpoints this gateway fronts:

```ts
import { AgentClient } from '@tangle-network/agent-client'
const client = new AgentClient('https://your-agent.tangle.tools')
const response = await client.chat('my-agent', messages, { apiKey: 'ak_...' })
```

## Architecture

```
agent-gateway (this package — server middleware)
  ├── createAgentGateway() — the request pipeline
  ├── SqlApiKeyStore — API keys + rate limits + reservations
  ├── SqlGatewayUsageStore — retry-safe usage attribution
  ├── SqlPaymentRecoveryStore — durable payment outbox
  └── A2A routes — Google agent-to-agent protocol

agent-client (consumer SDK)
  ├── AgentClient — discover, chat, stream
  └── Auth: API key, x402 SpendAuth, MPP
```

## Documentation

| Topic | Document |
|---|---|
| Payment recovery, reservations, nonce stores | [docs/payment-recovery.md](./docs/payment-recovery.md) |
| API key scopes, dependencies, rate limits | [docs/api-keys.md](./docs/api-keys.md) |
| A2A task stores, push notifications, long-horizon | [docs/a2a-protocol.md](./docs/a2a-protocol.md) |
| Long-horizon agents | [docs/a2a-long-horizon.md](./docs/a2a-long-horizon.md) |

## Related

- [`agent-client`](https://github.com/tangle-network/agent-client) — consumer SDK for calling this gateway
- [`agent-eval`](https://github.com/tangle-network/agent-eval) — evaluation framework
- [`tcloud`](https://github.com/tangle-network/tcloud) — consumer SDK for Tangle platform services

## License

MIT
