# Long-Horizon A2A Agents — Durability, Push, Resubscribe, Multi-Turn

The A2A protocol works well for short request-response calls out of the box. For agents that run for minutes, pause for user input, or finish after the calling client has disconnected, you need four additional features — all of which agent-gateway ships in this surface:

| Feature | Method(s) | Why it matters |
|---|---|---|
| Durable tasks | (transparent) | Worker / process recycle in the middle of a task doesn't lose the task's state |
| Push notifications | `tasks/pushNotificationConfig/{set,get,list,delete}` | Webhook fires when a task reaches a terminal state — the client doesn't have to hold a connection open |
| Resubscribe | `tasks/resubscribe` | A client that lost its SSE stream can re-attach to find out where the task ended up |
| Input-required + multi-turn | `input-required` state + follow-up `message/send` with the same `taskId` | The agent can pause and ask the user a question without ending the task |

The A2A surface is available with an in-memory task store by default.
Durable storage and push notifications are enabled through `GatewayConfig.a2a`.

## Durable tasks (SqlTaskStore)

By default `GatewayConfig.a2a.taskStore` is in-memory: fast, zero-config, fine for tests and single-machine deployments.
Production deployments swap in `SqlTaskStore` against any SQL store — D1, postgres, sqlite, libSQL, Turso — via a `SqlAdapter` shim.
Custom production task stores must implement `createIfAbsent`, `compareAndSet`, and `compareAndSetExecution`.
`compareAndSetExecution` must reject a renewal when the stored owner lease has expired.
The gateway keeps the OpenAI surface available when an older store lacks either method.
It returns `503` for A2A until the store is upgraded, rather than using a cross-worker unsafe fallback.
Use `SqlTaskStore` or another atomic adapter for multi-worker production deployments.

Payment nonce stores also require an atomic claim operation on every payment protocol version.
Plain Cloudflare KV cannot provide that operation.
Use D1, a Durable Object, or a custom atomic adapter for the claim callback passed to `KvNonceStore`.

Before a payment provider can mutate state, the gateway stores the recovery identity in task metadata.
The record contains the payment operation, usage receipt, output artifact, and a five-minute lease.
The task also points to the shared payment recovery outbox.
This outbox survives task-handler crashes and supports scheduled recovery without a task read.
After a restart, the first task read after lease expiry resumes settlement with the same operation.
If settlement acknowledgement fails, the gateway keeps the operation and retries after the lease expires.
Cancellation stores the recovery record before it completes the canceled task.
The canceled task therefore keeps a retryable lease when settlement acknowledgement fails.
If cancellation must release an unused durable operation, the shared outbox enters `releasing` before the adapter call.
An ambiguous release acknowledgement remains in that outbox and is retried by operation ID.
Usage attribution must atomically upsert by `requestId`.
The finalization record stores whether attribution was acknowledged, so recovery does not repeat an acknowledged usage event.
The payment claim keeps its submission lease until the task changes to `working` in one compare-and-set operation.
An expired execution lease fails the task while leaving payment recovery metadata available for reconciliation.
If a legacy record is malformed or has no recoverable operation, the gateway expires the task as failed.
If work exists without a receipt, the outbox settles the original quoted ceiling after its configured timeout.
The task-store TTL cannot delete a task while any payment recovery marker remains.
The short-lived `gatewaySubmission` marker is not a payment recovery marker.
An abandoned submission without a payment marker may expire after the normal task TTL.

Task control methods (`tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and push configuration methods) require `a2a.authorizeTaskAccess` in production.
The hook receives the task and request headers so the application can enforce task ownership.
Explicit `x402.demoMode` permits these methods without the hook for local tests only.
This is a fail-closed upgrade boundary.
Tasks stored before this release do not have a `gatewayOrigin` binding and cannot pass the default ownership check.
Migrate those records with a verified owner binding, or allow them to expire before enabling production control methods.
Push registration rejects reserved IP literals and private hostname suffixes.
Production push delivery also requires `a2a.pushUrlValidator`, which should apply the deployment's DNS-aware private-network policy.
The exported `deliverPushNotifications` function requires a non-empty HMAC secret.
Use `deliverDemoPushNotifications` only for explicit local demo mode.

### D1 (Cloudflare Workers)

Cloudflare does not invoke arbitrary `migrate` exports on a module Worker.
Create a D1 migration file and apply it before serving traffic.

```sql
-- migrations/0001_a2a.sql
CREATE TABLE IF NOT EXISTS a2a_tasks (
  id TEXT PRIMARY KEY,
  context_id TEXT NOT NULL,
  state TEXT NOT NULL,
  payload TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  execution_request_id TEXT,
  execution_lease_expires_at REAL
);
CREATE INDEX IF NOT EXISTS idx_a2a_tasks_context
  ON a2a_tasks (context_id, updated_at);
CREATE TABLE IF NOT EXISTS a2a_push_configs (
  task_id TEXT NOT NULL,
  config_id TEXT NOT NULL,
  url TEXT NOT NULL,
  token TEXT,
  authentication TEXT,
  PRIMARY KEY (task_id, config_id)
);
```

Apply it with `npx wrangler d1 migrations apply DB_NAME --remote`.
This creates both tables before the Worker uses them.
For an existing `a2a_tasks` table, apply a separate upgrade migration with these two statements before deployment:

```sql
ALTER TABLE a2a_tasks ADD COLUMN execution_request_id TEXT;
ALTER TABLE a2a_tasks ADD COLUMN execution_lease_expires_at REAL;
```

Do not serve traffic until this upgrade is applied.
The upgrade leaves both columns `NULL` for rows created by the old schema.
On the first renewal of a working legacy task, `SqlTaskStore` seeds both columns from the valid `gatewayExecution` payload marker in one atomic compare-and-set.
The compare-and-set requires the old payload, `state = 'working'`, and both columns to remain `NULL`, so only one worker can seed the row.
If the marker is malformed or only one column is populated, renewal fails closed.
Do not replace this contract with an unconditional payload backfill.

```ts
import {
  createAgentGateway,
  d1ToSqlAdapter,
  SqlPushNotificationStore,
  SqlTaskStore,
} from '@tangle-network/agent-gateway'

export default {
  async fetch(req: Request, env: { DB: D1Database }) {
    const db = d1ToSqlAdapter(env.DB)
    const taskStore = new SqlTaskStore(db)
    const pushStore = new SqlPushNotificationStore(db)

    const gw = createAgentGateway({
      // ... your existing config ...
      a2a: {
        taskStore,
        pushStore,
        webhookSecret: env.A2A_WEBHOOK_SECRET,
      },
    })
    // ...
  },
}
```

### node-postgres

```ts
import { Pool } from 'pg'
import { SqlTaskStore, type SqlAdapter } from '@tangle-network/agent-gateway'

const pool = new Pool({ connectionString: process.env.DATABASE_URL })

// pg uses $1, $2 placeholders; the store emits ?. Rewrite at the adapter boundary.
function rewrite(sql: string): string {
  let n = 0
  return sql.replace(/\?/g, () => `$${++n}`)
}

const pg: SqlAdapter = {
  async exec(sql, params = []) {
    const r = await pool.query(rewrite(sql), params as never[])
    return { rowsAffected: r.rowCount ?? 0 }
  },
  async query(sql, params = []) {
    const r = await pool.query(rewrite(sql), params as never[])
    return r.rows
  },
}

const taskStore = new SqlTaskStore(pg)
await taskStore.migrate()
```

### libSQL / Turso

```ts
import { createClient } from '@libsql/client'
import { SqlTaskStore, type SqlAdapter } from '@tangle-network/agent-gateway'

const client = createClient({
  url: process.env.TURSO_URL!,
  authToken: process.env.TURSO_TOKEN!,
})

const libsql: SqlAdapter = {
  async exec(sql, params = []) {
    const r = await client.execute({ sql, args: params as never[] })
    return { rowsAffected: Number(r.rowsAffected ?? 0) }
  },
  async query(sql, params = []) {
    const r = await client.execute({ sql, args: params as never[] })
    return r.rows as unknown as Record<string, unknown>[]
  },
}

const taskStore = new SqlTaskStore(libsql)
await taskStore.migrate()
```

### Schema

```sql
CREATE TABLE IF NOT EXISTS a2a_tasks (
  id TEXT PRIMARY KEY,
  context_id TEXT NOT NULL,
  state TEXT NOT NULL,
  payload TEXT NOT NULL,           -- JSON Task envelope
  updated_at INTEGER NOT NULL,     -- ms since epoch
  execution_request_id TEXT,       -- nullable durable execution owner
  execution_lease_expires_at REAL  -- ms since epoch
);
CREATE INDEX IF NOT EXISTS idx_a2a_tasks_context ON a2a_tasks (context_id, updated_at);
```

The two execution columns are required by the current store schema.
For an existing table, apply the upgrade migration above before traffic.
Rows from the old schema keep `NULL` execution columns until their first valid renewal, which performs the guarded seed described above.

One table stores the JSON payload.
TTL is enforced at read time and defaults to one hour.
Configure it with `new SqlTaskStore(db, { ttlMs })`.
Expired rows are lazily deleted only when they have no payment recovery marker.

`SqlTaskStore` also exposes `listByContext(contextId)` for surfacing all tasks in a conversation when the consumer's UI wants to show a thread view.

## Push notifications

When a task reaches a terminal state (`completed`, `canceled`, `failed`, `rejected`), the gateway POSTs the task envelope to each registered webhook URL with two headers:

- `X-A2A-Notification-Token` — the opaque token the consumer registered. Lets the receiver confirm the call corresponds to a registration they authorized.
- `X-A2A-Signature` — `sha256=<hex(HMAC-SHA256(webhookSecret, body))>`. Proves the body wasn't tampered with by anyone who doesn't know `webhookSecret`.

### Configure

```ts
import {
  createAgentGateway,
  d1ToSqlAdapter,
  SqlPushNotificationStore,
} from '@tangle-network/agent-gateway'

createAgentGateway({
  // ...
  a2a: {
    pushStore: new SqlPushNotificationStore(d1ToSqlAdapter(env.DB)),
    webhookSecret: env.A2A_WEBHOOK_SECRET, // required for HMAC signing
  },
})
```

`webhookSecret` MUST be a high-entropy shared secret — leaking it lets an attacker forge webhook deliveries to your consumer. Rotate it with the same cadence as any other inter-service HMAC.

When `pushStore` is set, the agent card advertises `capabilities.pushNotifications: true`. When it isn't, the four push RPC methods return `PUSH_NOT_SUPPORTED` and the card honestly reports `false`.

### Register from the client

```jsonc
// POST /v1/agents/test-agent
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tasks/pushNotificationConfig/set",
  "params": {
    "taskId": "task_abc",
    "pushNotificationConfig": {
      "id": "cfg_1",
      "url": "https://my-consumer.example.com/agent/done",
      "token": "opaque-shared-secret-from-my-side"
    }
  }
}
```

Get / list / delete mirror standard CRUD via the same method namespace.
Push destinations must use HTTPS and must not include URL credentials.

### Webhook receiver shape

```http
POST /agent/done HTTP/1.1
Host: my-consumer.example.com
Content-Type: application/json
X-A2A-Notification-Token: opaque-shared-secret-from-my-side
X-A2A-Signature: sha256=4f0c2d…(64 hex)

{
  "taskId": "task_abc",
  "state": "completed",
  "task": {
    "kind": "task",
    "id": "task_abc",
    "contextId": "ctx_xyz",
    "status": { "state": "completed", "timestamp": "2026-05-27T18:42:01.000Z" },
    "artifacts": [
      {
        "artifactId": "task_abc-artifact-0",
        "name": "response",
        "parts": [{ "kind": "text", "text": "…final agent output…" }]
      }
    ]
  }
}
```

Verify HMAC in your receiver:

```ts
async function verify(req: Request, secret: string): Promise<boolean> {
  const sig = req.headers.get('x-a2a-signature') ?? ''
  const expected = `sha256=${await hmac(secret, await req.clone().text())}`
  // constant-time compare in real code
  return sig === expected
}
```

### Delivery semantics

- **Fire-once.** The gateway claims each terminal `(task, config)` delivery with the durable task store before it sends the webhook.
  Concurrent workers therefore produce at most one attempt for that terminal state.
  There are no retries after a failed attempt.
  If a webhook returns non-2xx or the request fails, the gateway logs and moves on.
  The consumer's webhook handler SHOULD idempotently re-fetch state via `tasks/get` rather than rely on at-least-once delivery.
- **No partial-state deliveries.** Only terminal transitions fire push. `input-required` is NOT terminal — it's a pause, not an end.
- **Fire even on cancel + fail.** Consumers want to know the task ended for any reason, not just success.

If you need at-least-once or exactly-once delivery, wrap the gateway's fetcher with your own queue:

```ts
createAgentGateway({
  a2a: {
    pushStore: yourStore,
    webhookSecret: secret,
    pushFetcher: async (url, init) => {
      await yourQueue.enqueue({ url, init }) // your queue handles retries
      return new Response(null, { status: 202 })
    },
  },
})
```

## Resubscribe (`tasks/resubscribe`)

A client that disconnected its `message/stream` connection re-attaches with `tasks/resubscribe`. Returns SSE with one `status-update` event reflecting the task's current state:

```
client:  POST /v1/agents/test-agent  { method: "tasks/resubscribe", params: { id: "task_abc" } }

server:  HTTP/1.1 200 OK
         Content-Type: text/event-stream

         data: { "jsonrpc": "2.0", "id": 1, "result": {
                  "kind": "status-update",
                  "taskId": "task_abc",
                  "contextId": "ctx_xyz",
                  "status": { "state": "completed", "timestamp": "…" },
                  "final": true
               }}
```

`final` is `true` when the task is terminal or `input-required`; `false` while still `submitted` or `working`. Clients waiting on an in-flight task should re-call `tasks/resubscribe` periodically, or — preferred — register a push notification and let the webhook fire when state actually changes.

> The current implementation returns one event and closes. Live-rebroadcasting deltas from an in-flight stream to a new subscriber would require per-task pub/sub state — we'll add it when a real consumer needs it.

## Input-required + multi-turn continuation

A multi-turn agent pauses mid-conversation, emits an `input-required` status, and waits for the caller to provide more input. The caller's follow-up `message/send` carries the same `taskId`; the gateway appends the message to the task's history, transitions to `working`, and resumes the sandbox.

### Sandbox-side signal

The sandbox opts in by yielding a `SandboxStreamEvent` with `type: 'input-required'` (or by setting `data.inputRequired` on any event):

```ts
yield {
  type: 'input-required',
  data: { inputRequired: { prompt: 'What name should I use?' } },
}
```

The gateway then:

1. Settles for whatever tokens were produced before the pause (the caller is charged for the partial response).
2. Emits the `input-required` status with the prompt text as an agent-role message.
3. Persists the task with the partial response as an artifact.
4. Does NOT fire push notifications — `input-required` is non-terminal.
5. Returns the task envelope (for `message/send`) or emits a final `status-update` (for `message/stream`).

The gateway records a short-lived execution fence after sandbox acquisition and before the provider call.
Another worker can cancel before that fence is recorded.
After the fence is recorded, a remote cancellation returns `TASK_NOT_CANCELABLE` because it cannot abort the live worker safely.

Sandboxes that never emit input-required see identical behavior to before.

### Client continuation

```jsonc
// Initial call returns input-required.
{ "method": "message/send", "params": { "message": { "kind": "message", "role": "user",
  "parts": [{ "kind": "text", "text": "Set up an account for me" }], "messageId": "msg_1" }}}

// → returns Task { id: "task_abc", status: { state: "input-required", message: { ...
//                  parts: [{ kind: "text", text: "What name shall I use?" }] }}}

// Follow-up uses the same taskId.
{ "method": "message/send", "params": { "message": { "kind": "message", "role": "user",
  "parts": [{ "kind": "text", "text": "Drew" }], "messageId": "msg_2", "taskId": "task_abc" }}}

// → returns Task { id: "task_abc", status: { state: "completed" },
//                  history: [ msg_1, msg_2 ], artifacts: [...] }
```

A follow-up against a task in any state other than `input-required` is rejected with `INVALID_PARAMS` ("only 'input-required' tasks accept follow-up messages"). The gateway never silently re-runs or merges into a terminal task.

## Putting it all together — long-horizon agent loop

```
client                          gateway                          sandbox
  │                                │                                │
  ├──message/stream────────────────▶                                │
  │                                ├──streamPrompt────────────────────▶
  │                                │                                │
  │ ◀──status-update (working)─────┤                                │
  │ ◀──artifact-update ×N──────────┤◀──text deltas──────────────────┤
  │                                │ ◀──input-required───────────────┤
  │ ◀──status-update (input-required, final=true)──┤                │
  │                                │                                │
  ├──pushNotificationConfig/set──▶ │                                │
  │ ◀──{ ok }────────────────────┤ │                                │
  │                                │                                │
  │  ⋮ days later ⋮                │                                │
  │                                │                                │
  ├──message/send (taskId, "go")──▶                                │
  │                                ├──streamPrompt (resume)─────────▶
  │                                │ ◀──text deltas──────────────────┤
  │ ◀──Task (completed)────────────┤                                │
  │                                │                                │
  │                       webhook ─▶ my-consumer.example.com/agent/done
  │                                │   X-A2A-Notification-Token
  │                                │   X-A2A-Signature: sha256=…
  │                                │   { taskId, state: "completed", task }
```

## Operational notes

- **Run migrations once at deploy.** `SqlTaskStore.migrate()` and `SqlPushNotificationStore.migrate()` are idempotent, but running per-request adds round-trips. The schema is stable; if it changes, the package bumps a major.
- **Index by `task_id` and `context_id`.** Already created by the migrations. If you write admin tools that query out-of-band, lean on those indexes.
- **PII in the store.** Task histories contain user-generated content; apply the same retention + encryption controls you use for chat logs elsewhere.
- **Single-writer per task.** The gateway is the only writer to a task row in normal operation. If you build admin tools that mutate tasks directly, gate at the application layer — there's no row-level lock.
- **Custom table prefixes** (`new SqlTaskStore(db, { table: 'agent_a_tasks' })`) let one database host multiple agent surfaces without collision.

## Reference

- `src/a2a/task-store.ts` — `TaskStore` interface + `InMemoryTaskStore`.
- `src/a2a/task-store-sql.ts` — `SqlAdapter`, `SqlTaskStore`, `d1ToSqlAdapter`.
- `src/a2a/push-notifications.ts` — `PushNotificationStore`, `InMemoryPushNotificationStore`, `SqlPushNotificationStore`, `deliverPushNotifications`.
- `src/a2a/handler.ts` — RPC dispatch including the four push methods + `tasks/resubscribe` + multi-turn continuation logic.
- `src/a2a/types.ts` — A2A protocol types including the extended error codes.
- `tests/a2a-long-horizon.test.ts` — end-to-end push + input-required + multi-turn + resubscribe tests.
- `tests/a2a-durability.test.ts` — SqlTaskStore + SqlPushNotificationStore lifecycle tests.
