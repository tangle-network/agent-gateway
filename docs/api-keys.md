# API key permissions

> Part of the [agent-gateway](../README.md) reference. See the README for the quickstart.

## API key permissions

Use `createApiKeyRoutes` with the app's existing key store and browser-session authenticator.
The authenticator must reject operator keys for credential management.
Declare prerequisites when one permission requires another:

```ts
createApiKeyRoutes({
  store: apiKeyStore,
  getAuthUserId: getBrowserSessionUserId,
  validScopes: ['operator:read', 'operator:write', 'operator:run'],
  scopeDependencies: { 'operator:run': ['operator:read'] },
  requireExpiryForScopes: ['operator:read', 'operator:run'],
})
```

Missing prerequisites return HTTP 400 with `api_key.scope_dependency` before key creation.
The issuer never adds missing prerequisites automatically.
Existing default-scope behavior still applies when scopes are omitted or contain no configured values.
Dependencies must name configured scopes in a plain record.
Use a computed property (`['__proto__']`) if that literal scope needs prerequisites.
`requireExpiryForScopes` rejects missing or nonfuture expiry dates for any matching normalized scope before storage.
The expiry check uses whole seconds to match the bundled SQL store.
Other scopes retain the existing optional-expiry behavior.
Request-time authentication must still enforce all required permissions, expiry, revocation, and workspace access.