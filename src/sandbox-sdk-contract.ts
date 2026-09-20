/**
 * Compile-time conformance between `SandboxBox` and the Tangle Sandbox SDK.
 *
 * The A2A detached-execution path stores an exact run reference and later
 * reattaches to it through `dispatchPrompt` and `session().events/result/
 * interrupt/runs`. `SandboxBox` states that surface structurally so a host can
 * supply its own sandbox, which means an SDK that renamed or reshaped any of it
 * would otherwise fail first in production.
 *
 * These assertions check the direction the gateway actually depends on: what
 * the SDK returns must still be readable through the gateway's own types.
 * Event payloads are deliberately excluded — `SandboxStreamEvent` is a narrow
 * read of a deliberately open event bag, not a mirror of it.
 *
 * Nothing imports this module; it exists so `pnpm typecheck` fails when the
 * declared `@tangle-network/sandbox` version stops matching what is consumed.
 */
import type { SandboxInstance } from '@tangle-network/sandbox'
import type { SandboxBox, SandboxPromptResult } from './types'

type SdkSession = ReturnType<SandboxInstance['session']>
type GatewaySession = ReturnType<NonNullable<SandboxBox['session']>>
type GatewayDispatchResult = Awaited<ReturnType<NonNullable<SandboxBox['dispatchPrompt']>>>
type GatewayRunInfo = Awaited<ReturnType<GatewaySession['runs']>>[number]

declare const sandbox: SandboxInstance
declare const session: SdkSession
declare const dispatched: Awaited<ReturnType<SandboxInstance['dispatchPrompt']>>
declare const runs: Awaited<ReturnType<SdkSession['runs']>>
declare const result: Awaited<ReturnType<SdkSession['result']>>
declare const interrupted: Awaited<ReturnType<SdkSession['interrupt']>>

function assertSandboxSdkSurface(): void {
  sandbox.id satisfies string
  sandbox.dispatchPrompt satisfies Function
  sandbox.session satisfies Function
  session.events satisfies Function
  dispatched satisfies GatewayDispatchResult
  runs[0] satisfies GatewayRunInfo | undefined
  result satisfies SandboxPromptResult
  interrupted satisfies { cancelled: boolean }
}

void assertSandboxSdkSurface
