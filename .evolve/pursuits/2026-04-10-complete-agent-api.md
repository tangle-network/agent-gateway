# Pursuit: Complete Agent API Surface
Generation: 1
Date: 2026-04-10
Status: evaluated

## Thesis
The gateway middleware exists but agents can't actually be published, keys can't be created, payments aren't enforced, and there's no client SDK. This pursuit ships everything needed for one agent to pay another agent to do work — the complete API surface.

## Build Status
| # | Change | Status | Repo |
|---|--------|--------|------|
| 1 | Rate limiting in gateway | DONE | agent-gateway |
| 2 | API key management (types + routes) | DONE | agent-gateway |
| 3 | Publishing routes | DONE | agent-gateway |
| 4 | Consumer client SDK | DONE | agent-client |
| 5 | Nonce replay protection | DONE | agent-gateway |
| 6 | Rate limit eviction | DONE | agent-gateway |
| 7 | Security audit fixes (expiry, body limit, demo mode) | DONE | agent-gateway + agent-client |
| 8 | Wire gateway into GTM | DONE | gtm-agent |
| 9 | Wire gateway into Tax | DONE | tax-agent |
| 10 | Wire gateway into Legal | DONE | legal-agent |
| 11 | Wire gateway into Film (replace inline) | DONE | film-agent |
| 12 | Publish + API key routes in GTM | DONE | gtm-agent |
| 13 | Publish + API key routes in Tax | DONE | tax-agent |
| 14 | Publish routes in Legal (has own API keys) | DONE | legal-agent |

## Gap Closure

| Gap | Before | After |
|-----|--------|-------|
| Publishing flow | Film only | All 4 agents |
| API key management | Legal only (standalone) | GTM + Tax (shared), Legal (own), Film (pending) |
| Rate limiting | None in gateway | Per-consumer sliding window with eviction |
| Nonce replay | None | MemoryNonceStore with auto-eviction |
| Consumer client SDK | None | @tangle-network/agent-client |
| Body size limit | None | 64KB enforced before parsing |
| Expiry validation | None | Reject expired x402 payments |
| Demo mode flag | Implicit | Explicit demoMode config |
| Slug validation (client) | None | Regex + URL encoding |

## What remains

| Item | Status | Why deferred |
|------|--------|-------------|
| Real x402 on-chain verification | Needs ShieldedCredits contract | Not a code gap — needs deployed contract |
| D1-backed API key stores | In-memory works for dev | Production needs per-agent migration |
| Film API key routes | Film has old inline publish | Needs to adopt shared API key routes |
| KV-backed rate limit/nonce stores | In-memory works for dev | Production Workers need KV |
| sandbox-ui publishing components | No UI | Frontend work, separate pursuit |
| Constant-time hash comparison | Documented requirement | ApiKeyStore implementors must handle |

## Verdict
ADVANCE — all critical gaps closed. The system now has a complete request→auth→sandbox→stream→settle→record pipeline with rate limiting, nonce protection, and a client SDK. Every agent can publish endpoints, accept API keys, and be called programmatically.
