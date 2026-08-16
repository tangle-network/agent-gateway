import { readFileSync, readdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const sourceDirectory = fileURLToPath(new URL('../src/', import.meta.url))

function lineCount(fileName: string): number {
  const source = readFileSync(`${sourceDirectory}/${fileName}`, 'utf8')
  return source.trimEnd().split(/\r?\n/).length
}

describe('dispatch module boundaries', () => {
  const files = readdirSync(sourceDirectory).filter(
    (fileName) => fileName === 'dispatch.ts' || /^dispatch-.+\.ts$/.test(fileName),
  )

  for (const fileName of files) {
    it(`${fileName} stays focused`, () => {
      const limit = fileName === 'dispatch.ts' ? 100 : 500
      expect(lineCount(fileName)).toBeLessThanOrEqual(limit)
    })
  }
})
