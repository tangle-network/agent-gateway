import type {
  SandboxBox,
  SandboxPromptResult,
  SandboxStreamEvent,
  SandboxUsageReceipt,
} from '../src/types'

interface Run {
  sessionId: string
  executionId: string
  events: SandboxStreamEvent[]
  done: boolean
  interrupted: boolean
  result?: SandboxPromptResult
  controller: AbortController
  finished: Promise<void>
  waiters: Set<() => void>
}

export interface DurableTestSandbox extends SandboxBox {
  readonly id: string
}

const adapters = new WeakMap<object, DurableTestSandbox>()

export function durableSandbox(source: SandboxBox, id = 'test-sandbox'): DurableTestSandbox {
  if (source.id && source.dispatchPrompt && source.session) return source as DurableTestSandbox
  const cached = adapters.get(source)
  if (cached) return cached
  const byTurn = new Map<string, Run>()
  const byExecution = new Map<string, Run>()
  let sequence = 0

  const start = (
    sessionId: string,
    executionId: string,
    message: string,
    options: Parameters<NonNullable<SandboxBox['streamPrompt']>>[1],
  ): Run => {
    const run = {
      sessionId,
      executionId,
      events: [],
      done: false,
      interrupted: false,
      controller: new AbortController(),
      waiters: new Set(),
    } as Run
    run.finished = (async () => {
      let response = ''
      let usage: Partial<SandboxUsageReceipt> | undefined
      let errorMessage: string | undefined
      let inputRequired = false
      try {
        for await (const original of source.streamPrompt(message, {
          ...options,
          signal: run.controller.signal,
        })) {
          const event = original.id
            ? original
            : { ...original, id: `${executionId}:${run.events.length}` }
          run.events.push(event)
          wake(run)
          if (event.type === 'message.part.updated' && event.data?.delta) response += event.data.delta
          if (event.data?.usage) usage = event.data.usage
          if (event.type === 'input-required' || event.data?.inputRequired) inputRequired = true
          if (event.type === 'error' || event.type === 'session.run.failed') {
            errorMessage = event.data?.message ?? 'sandbox stream failed'
          }
        }
      } catch (error) {
        errorMessage = error instanceof Error ? error.message : String(error)
        run.events.push({
          id: `${executionId}:${run.events.length}`,
          type: 'error',
          data: { message: errorMessage },
        })
        wake(run)
      }
      const hasUsage = Number.isSafeInteger(usage?.inputTokens) && Number.isSafeInteger(usage?.outputTokens)
      run.result = {
        success: !run.interrupted && !errorMessage,
        status: run.interrupted ? 'canceled' : inputRequired ? 'awaiting_question' : errorMessage ? 'failed' : 'completed',
        executionId,
        ...(response ? { response } : {}),
        ...(errorMessage ? { error: errorMessage } : {}),
        ...(hasUsage ? { usage: { inputTokens: usage!.inputTokens!, outputTokens: usage!.outputTokens! } } : {}),
        ...(typeof usage?.providerCostUsd === 'number' ? { costUsd: usage.providerCostUsd } : {}),
      }
      run.done = true
      wake(run)
    })()
    return run
  }

  const events = (run: Run, options?: { since?: string; signal?: AbortSignal }): AsyncIterable<SandboxStreamEvent> => ({
    async *[Symbol.asyncIterator]() {
      let index = options?.since
        ? Math.max(0, run.events.findIndex((event) => event.id === options.since) + 1)
        : 0
      while (!options?.signal?.aborted) {
        while (index < run.events.length) yield run.events[index++]!
        if (run.done) return
        await waitForEvent(run, options?.signal)
      }
    },
  })

  const find = (sessionId: string, executionId?: string): Run => {
    const run = executionId
      ? byExecution.get(executionId)
      : [...byExecution.values()].reverse().find((candidate) => candidate.sessionId === sessionId)
    if (!run || run.sessionId !== sessionId) throw new Error('test sandbox execution not found')
    return run
  }
  const adapted: DurableTestSandbox = {
    id,
    streamPrompt: source.streamPrompt.bind(source),
    async dispatchPrompt(message, options = {}) {
      const sessionId = options.sessionId ?? `session:${id}`
      const turnId = options.turnId ?? `turn:${sequence}`
      const key = `${sessionId}:${turnId}`
      const existing = byTurn.get(key)
      if (existing) return {
        sessionId,
        executionId: existing.executionId,
        runControlRef: { environmentId: id, sessionId, executionId: existing.executionId },
        dispatched: false,
      }
      const executionId = `${id}:execution:${sequence++}`
      const run = start(sessionId, executionId, message, options)
      byTurn.set(key, run)
      byExecution.set(executionId, run)
      return {
        sessionId,
        executionId,
        runControlRef: { environmentId: id, sessionId, executionId },
        dispatched: true,
      }
    },
    session(sessionId) {
      return {
        events: (options) => events(find(sessionId, options?.executionId), options),
        result: async (options) => {
          const run = find(sessionId, options?.executionId)
          await run.finished
          return run.result!
        },
        interrupt: async (options) => {
          const run = find(sessionId, options?.executionId)
          if (run.done) return { cancelled: false }
          run.interrupted = true
          run.controller.abort()
          return { cancelled: true }
        },
      }
    },
  }
  adapters.set(source, adapted)
  return adapted
}

function wake(run: Run): void {
  for (const resolve of run.waiters) resolve()
  run.waiters.clear()
}

function waitForEvent(run: Run, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const wakeWaiter = () => {
      run.waiters.delete(wakeWaiter)
      signal?.removeEventListener('abort', wakeWaiter)
      resolve()
    }
    run.waiters.add(wakeWaiter)
    signal?.addEventListener('abort', wakeWaiter, { once: true })
    if (run.done || signal?.aborted) wakeWaiter()
  })
}
