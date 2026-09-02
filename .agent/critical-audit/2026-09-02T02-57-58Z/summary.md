# Audit: durable API-key policy — 6daaab6..ad9c58f — n=12 files, 3 findings

**Verdict:** REQUEST_CHANGES — 2 of 3 findings permit abuse or storage growth · 0 CRITICAL / 2 HIGH / 1 MEDIUM / 0 LOW
**Worst:** #1 `src/dispatch-payment.ts:47` — minute-only keys bypass durable counting · cost if shipped W × declared requests/minute
**Next:** Fix all 3 findings, then re-run the 3 real HTTP and SQL regressions.

## Scope

| Field | Value |
|---|---|
| Files | n=12 via `git diff --name-only origin/main..ad9c58f` |
| Base..head | `6daaab6..ad9c58f` |
| Project type | TypeScript package |
| Reviewers | A,B,C · serial |
| Not inspected | Live D1 deployment; no deployed consumer uses this unreleased commit. |

## Findings — 3 of 3, ranked

| # | Sev | file:line | Defect | Failure scenario | Status | Evidence | Fix | Verification | Cost if shipped | Saved if fixed |
|---:|---|---|---|---|---|---|---|---|---:|---:|
| 1 | HIGH | `src/dispatch-payment.ts:47` | Minute-only keys do not require durable counting. | `rateLimitPerMinute=10`, no claim callback → compute returns 200. | measured | Targeted Vitest: 1/1 regression failed, 200 vs 503. | Require the callback for either limit. | HTTP returns 503; sandbox untouched. | W×10 requests/min | (W−1)×10 requests/min |
| 2 | HIGH | `src/a2a/handler.ts:423` | Rejected A2A calls retain failed tasks. | Daily limit reached → 429 plus one stored task. | measured | Targeted Vitest: 1/1 cleanup regression retained the task. | Delete new unpublished tasks. | 429 plus missing task row. | 1 row/rejection | 1 row/rejection |
| 3 | MEDIUM | `src/api-key-store-sql.ts:278` | Claim rows never expire. | 1,000/day for 365 days → 365,000 rows/key. | measured | Source has 1 insert and 0 deletes. | Amortized two-day retention. | Old row removed; 255 current remain. | 1 row/request forever | all expired rows |

0 dropped.

## Assumptions & unverified

| Assumption | Finding it would flip | Check that settles it |
|---|---|---|
| Production uses more than one Worker instance. | #1 cost, not correctness | Read deployed Worker concurrency. |
| D1 honors the tested SQLite statement behavior. | #3 | Run the migration and retention case on deployed D1. |

## Self-gate

9/9 passed — failed: none.
