# Release Progress

## Target
- Environment: npm registry
- Live URL: https://www.npmjs.com/package/@tangle-network/agent-gateway
- Live service/process: npm package `@tangle-network/agent-gateway`
- Artifact path: tag-triggered GitHub Actions publish workflow
- Rollback artifact/path: retain v0.8.8; publish a follow-up patch if needed
- Credential files: GitHub trusted publishing and npm provenance workflow

## Local State
- Branch: origin/main (tag source)
- Commit: 2896b2e (merged 0.8.10 release)
- Dirty files: clean at release tag
- Gates planned: focused failure tests, full typecheck/test/build, PR review, npm version proof

## Remote State
- Host/provider: GitHub Actions + npm trusted publishing
- Current live artifact: @tangle-network/agent-gateway@0.8.10
- Current service status: tag workflow 33542264053 completed successfully; verify and publish-npm passed
- Last smoke result: `npm view @tangle-network/agent-gateway version dist-tags --json` returned version/latest `0.8.10`

## Decision
- Build path: tag-triggered GitHub Actions publish
- Reason: repository workflow owns verification, provenance, and npm publication
- Expected duration: minutes after merge and tag push; completed in under two minutes
- Fallback/rollback: retain v0.8.8; diagnose and retry only after a failed v0.8.10 workflow

## Timeline
- 2026-09-01: confirmed branch starts at origin/main v0.8.8; implementation and release verification in progress
- 2026-09-01: PR #24 merged as b39a117 after verify passed; release target is 0.8.9
- 2026-09-01: v0.8.9 tag run 33541628022 failed because PR #25 server-owned IDs invalidated the new preallocated-id test; npm publication was skipped
- 2026-09-01: PR #27 merged as c008808; corrected test passes 389/389 and release target is 0.8.10
- 2026-09-01: PR #28 merged as 2896b2e; tag v0.8.10 points to that SHA
- 2026-09-01: workflow 33542264053 passed verification and trusted npm publication
- 2026-09-01: npm registry returned version/latest 0.8.10 and tarball `https://registry.npmjs.org/@tangle-network/agent-gateway/-/agent-gateway-0.8.10.tgz`
