import { readdirSync, readFileSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const sourceRoot = join(dirname(fileURLToPath(import.meta.url)), '../src/a2a')
const handlerPath = join(sourceRoot, 'handler.ts')
const extractedModulePaths = readdirSync(sourceRoot)
  .filter((name) => name.endsWith('.ts') && name !== 'handler.ts')
  .sort()
  .map((name) => join(sourceRoot, name))

function countLines(path: string): number {
  const source = readFileSync(path, 'utf8')
  return source.length === 0 ? 0 : source.split(/\r?\n/).length - (source.endsWith('\n') ? 1 : 0)
}

describe('A2A module size boundaries', () => {
  it('keeps the handler below 1,000 lines and every A2A module below 500', () => {
    expect(countLines(handlerPath), relative(process.cwd(), handlerPath)).toBeLessThan(1_000)
    expect(extractedModulePaths.length).toBeGreaterThan(0)
    for (const path of extractedModulePaths) {
      expect(countLines(path), relative(process.cwd(), path)).toBeLessThan(500)
    }
  })
})
