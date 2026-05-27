/**
 * Build an A2A AgentCard from the gateway's existing AgentMeta + GatewayConfig.
 * The card's `url` MUST be the JSON-RPC endpoint a client POSTs to; clients
 * fetch the card from `<url>/.well-known/agent.json` per A2A convention.
 *
 * Auth schemes advertised reflect the gateway's actually-accepted payment
 * methods (x402 always; mpp when configured; Bearer for API keys). Skills
 * fall back to a single synthesized "chat" skill when the AgentMeta doesn't
 * declare its own — most consumers will override.
 */

import type { AgentMeta, GatewayConfig } from '../types'
import type { AgentCard, AgentSkill } from './types'

export function buildAgentCard(
  agent: AgentMeta,
  config: GatewayConfig,
  agentUrl: string,
): AgentCard {
  const schemes: string[] = ['x402']
  if (config.mpp) schemes.push('mpp')
  schemes.push('Bearer')

  const skills: AgentSkill[] =
    agent.skills && agent.skills.length > 0
      ? agent.skills
      : [
          {
            id: 'chat',
            name: agent.slug,
            description: agent.description ?? `${agent.slug} agent`,
            tags: ['chat'],
            inputModes: ['text'],
            outputModes: ['text'],
          },
        ]

  return {
    name: agent.slug,
    description: agent.description ?? `${agent.slug} agent`,
    url: agentUrl,
    version: '1.0.0',
    provider: { organization: 'Tangle', url: 'https://tangle.tools' },
    capabilities: {
      streaming: true,
      pushNotifications: !!config.a2a?.pushStore,
      stateTransitionHistory: false,
    },
    authentication: { schemes },
    defaultInputModes: ['text'],
    defaultOutputModes: ['text'],
    skills,
  }
}
