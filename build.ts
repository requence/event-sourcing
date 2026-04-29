import { mkdir, readdir, rm } from 'node:fs/promises'
import { join } from 'node:path'

const SRC = './src'
const OUT = './dist'

// Collect all .ts source files (exclude tests)
async function collectEntrypoints(dir: string): Promise<string[]> {
  const entries: string[] = []
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const fullPath = join(dir, entry.name)
    if (entry.isDirectory()) {
      if (entry.name === 'tests') {
        continue
      }
      entries.push(...(await collectEntrypoints(fullPath)))
    } else if (entry.name.endsWith('.ts') && !entry.name.endsWith('.spec.ts')) {
      entries.push(fullPath)
    }
  }
  return entries
}

// Clean dist
await rm(OUT, { recursive: true, force: true })
await mkdir(OUT, { recursive: true })

const entrypoints = await collectEntrypoints(SRC)

const result = await Bun.build({
  entrypoints,
  outdir: OUT,
  root: SRC,
  target: 'node',
  format: 'esm',
  splitting: true,
  sourcemap: 'external',
  external: [
    'zod',
    'drizzle-orm',
    'drizzle-orm/*',
    'ioredis',
    'async-mutex',
    'fast-equals',
    'superjson',
  ],
})

if (!result.success) {
  console.error('Build failed:')
  for (const log of result.logs) {
    console.error(log)
  }
  process.exit(1)
}

// Generate .d.ts files via tsc
const tsc = Bun.spawn(['bunx', 'tsc'], {
  stdout: 'inherit',
  stderr: 'inherit',
})
const tscExit = await tsc.exited

if (tscExit !== 0) {
  console.error('TypeScript declaration generation failed')
  process.exit(1)
}

console.log(`✓ Built ${result.outputs.length} files to ${OUT}/`)
