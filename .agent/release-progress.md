# Release Progress

## Target
- Environment: npm registry
- Live URL: https://www.npmjs.com/package/@tangle-network/agent-gateway
- Live service/process: npm package `@tangle-network/agent-gateway`
- Artifact path: tag-triggered GitHub Actions publish workflow
- Rollback artifact/path: retain v0.8.8; publish a follow-up patch if needed
- Credential files: GitHub trusted publishing and npm provenance workflow

## Local State
- Branch: chore/release-agent-gateway-0.8.9
- Commit: b39a117 (merged failure-contract fix)
- Dirty files: clean at start
- Gates planned: focused failure tests, full typecheck/test/build, PR review, npm version proof

## Remote State
- Host/provider: GitHub Actions + npm trusted publishing
- Current live artifact: @tangle-network/agent-gateway@0.8.8
- Current service status: package registry state to verify before release
- Last smoke result: pending

## Decision
- Build path: tag-triggered GitHub Actions publish
- Reason: repository workflow owns verification, provenance, and npm publication
- Expected duration: minutes after merge and tag push
- Fallback/rollback: retain v0.8.8 and diagnose failed workflow before retrying

## Timeline
- 2026-09-01: confirmed branch starts at origin/main v0.8.8; implementation and release verification in progress
- 2026-09-01: PR #24 merged as b39a117 after verify passed; release target is 0.8.9
