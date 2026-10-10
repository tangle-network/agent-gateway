# A2A protocol

> Part of the [agent-gateway](../README.md) reference. See the README for the quickstart.

## A2A protocol

The gateway speaks Google's A2A protocol alongside its OpenAI-compatible surface: discovery via `.well-known/agent.json`, JSON-RPC 2.0 dispatch for `message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, and the four `tasks/pushNotificationConfig/*` methods. Long-horizon agents — durable tasks across worker restarts, webhook delivery on terminal state, `input-required` pauses with multi-turn continuation — are documented in [`docs/a2a-long-horizon.md`](./docs/a2a-long-horizon.md).
Production A2A task control requires `a2a.authorizeTaskAccess`; explicit demo mode is the local-test exception.
Set `a2a: false` when the application does not expose A2A routes.
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