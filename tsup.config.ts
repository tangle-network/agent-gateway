import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/middleware.ts',
    'src/types.ts',
    'src/api-keys.ts',
    'src/publish.ts',
    'src/rate-limit.ts',
    'src/nonce-store.ts',
  ],
  format: ['esm'],
  dts: true,
  clean: true,
  sourcemap: true,
})
