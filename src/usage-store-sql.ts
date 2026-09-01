import type { GatewayUsageEvent } from './payment-types'
import type { SqlAdapter } from './a2a/task-store-sql'
import { requireSqlIdentifier } from './sql'

export interface SqlGatewayUsageStoreOptions {
  table?: string
}

export function sqlGatewayUsageStoreSchemaStatements(
  options: SqlGatewayUsageStoreOptions = {},
): readonly [string, string] {
  const table = requireSqlIdentifier(options.table ?? 'agent_gateway_usage')
  return [
    `CREATE TABLE IF NOT EXISTS ${table} (
      request_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      agent_slug TEXT NOT NULL,
      consumer_id TEXT NOT NULL,
      payment_method TEXT NOT NULL,
      input_tokens INTEGER NOT NULL,
      output_tokens INTEGER NOT NULL,
      reasoning_tokens INTEGER,
      tool_tokens INTEGER,
      tool_call_count INTEGER,
      provider_cost_usd REAL,
      total_cost_usd REAL NOT NULL,
      owner_earned_usd REAL NOT NULL,
      platform_fee_usd REAL NOT NULL,
      duration_ms INTEGER NOT NULL,
      settlement_basis TEXT,
      created_at INTEGER NOT NULL
    )`,
    `CREATE INDEX IF NOT EXISTS idx_${table}_agent ON ${table} (agent_id, created_at)`,
  ]
}

/** Durable, retry-safe storage for gateway usage attribution. */
export class SqlGatewayUsageStore {
  private readonly table: string

  constructor(
    private readonly db: SqlAdapter,
    options: SqlGatewayUsageStoreOptions = {},
  ) {
    this.table = requireSqlIdentifier(options.table ?? 'agent_gateway_usage')
  }

  /** Idempotent. Call once during deployment. */
  async migrate(): Promise<void> {
    for (const statement of sqlGatewayUsageStoreSchemaStatements({ table: this.table })) {
      await this.db.exec(statement)
    }
  }

  /** Safe to pass directly as createAgentGateway's recordUsage callback. */
  readonly recordUsage = async (event: GatewayUsageEvent): Promise<void> => {
    await this.db.exec(
      `INSERT INTO ${this.table} (
        request_id, agent_id, agent_slug, consumer_id, payment_method,
        input_tokens, output_tokens, reasoning_tokens, tool_tokens, tool_call_count,
        provider_cost_usd, total_cost_usd, owner_earned_usd, platform_fee_usd,
        duration_ms, settlement_basis, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(request_id) DO UPDATE SET
        agent_id = excluded.agent_id,
        agent_slug = excluded.agent_slug,
        consumer_id = excluded.consumer_id,
        payment_method = excluded.payment_method,
        input_tokens = excluded.input_tokens,
        output_tokens = excluded.output_tokens,
        reasoning_tokens = excluded.reasoning_tokens,
        tool_tokens = excluded.tool_tokens,
        tool_call_count = excluded.tool_call_count,
        provider_cost_usd = excluded.provider_cost_usd,
        total_cost_usd = excluded.total_cost_usd,
        owner_earned_usd = excluded.owner_earned_usd,
        platform_fee_usd = excluded.platform_fee_usd,
        duration_ms = excluded.duration_ms,
        settlement_basis = excluded.settlement_basis`,
      [
        event.requestId,
        event.agentId,
        event.agentSlug,
        event.consumerId,
        event.paymentMethod,
        event.inputTokens,
        event.outputTokens,
        event.reasoningTokens ?? null,
        event.toolTokens ?? null,
        event.toolCallCount ?? null,
        event.providerCostUsd ?? null,
        event.totalCostUsd,
        event.ownerEarnedUsd,
        event.platformFeeUsd,
        event.durationMs,
        event.settlementBasis ?? null,
        Math.floor(Date.now() / 1_000),
      ],
    )
  }
}
