# Release Progress

## Target
- Environment: npm registry
- Live URL: https://www.npmjs.com/package/@tangle-network/agent-gateway
- Live service/process: npm package `@tangle-network/agent-gateway`
- Artifact path: tag-triggered GitHub Actions publish workflow
- Rollback artifact/path: retain v0.8.15; publish a follow-up patch if needed
- Credential files: GitHub trusted publishing and npm provenance workflow

## Local State
- Branch: chore/release-0.8.16-20260902
- Commit: 408dac6 (merged background usage settlement)
- Dirty files: clean at release start
- Gates planned: version-only diff, full typecheck/test/build, PR review, npm version proof

## Remote State
- Host/provider: GitHub Actions + npm trusted publishing
- Current live artifact: @tangle-network/agent-gateway@0.8.15
- Current service status: merged source is 408dac6; release workflow runs from an exact v0.8.16 tag
- Last smoke result: pending this release

## Decision
- Build path: tag-triggered GitHub Actions publish
- Reason: repository workflow owns verification, provenance, and npm publication
- Expected duration: minutes after merge and tag push
- Fallback/rollback: retain v0.8.15; diagnose and retry only after a failed v0.8.16 workflow

## Timeline
- 2026-09-01: confirmed release branch starts at origin/main 5095f58, version 0.8.13
- 2026-09-01: local typecheck, 26 test files / 439 tests, and build passed on Node 24.13.0
- 2026-09-01: PR #24 merged as b39a117 after verify passed; release target is 0.8.9
- 2026-09-01: v0.8.9 tag run 33541628022 failed because PR #25 server-owned IDs invalidated the new preallocated-id test; npm publication was skipped
- 2026-09-01: PR #27 merged as c008808; corrected test passes 389/389 and release target is 0.8.10
- 2026-09-01: PR #28 merged as 2896b2e; tag v0.8.10 points to that SHA
- 2026-09-01: workflow 33542264053 passed verification and trusted npm publication
- 2026-09-01: npm registry returned version/latest 0.8.10 and tarball `https://registry.npmjs.org/@tangle-network/agent-gateway/-/agent-gateway-0.8.10.tgz`
- 2026-09-02: PR #39 merged as 23d9f8a; async input bounds and the root pricing export landed
- 2026-09-02: PR #40 merged as 90ed0fd; dynamic bounds now run after payment and consumer authorization, and `a2a: false` disables A2A routes
- 2026-09-02: npm registry returned version/latest 0.8.14 before the 0.8.15 release
- 2026-09-02: PR #43 merged as 408dac6; canceled clients no longer prevent final usage settlement when the host opts in
- 2026-09-02: local Node 22.13 and Node 24.13 test runs each passed 447/447 tests
