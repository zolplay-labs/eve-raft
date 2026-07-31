#!/usr/bin/env node

import { readFile } from 'node:fs/promises'
import { createInterface } from 'node:readline/promises'

import { parseCli } from './cli-options.js'
import { connectRaft } from './connect.js'
import { startSupervisedRuntime } from './runtime.js'

const HELP = `Usage:
  eve-raft connect [--server-url URL] [--agent-id ID] [--data-dir PATH] [--replace]
  eve-raft start [options] -- eve start

Start options:
  --data-dir PATH       Persistent state directory (default: .eve-raft)
  --health-host HOST    Health interface (default: 0.0.0.0)
  --health-port PORT    Health port (default: $PORT or 3000)
  --eve-host HOST       Internal Eve interface (default: 127.0.0.1)
  --eve-port PORT       Internal Eve port (default: 3100)`

async function prompt(label: string, defaultValue?: string): Promise<string> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) throw new Error(`${label} is required in non-interactive mode`)
  const input = createInterface({ input: process.stdin, output: process.stdout })
  try {
    const answer = (await input.question(`${label}${defaultValue ? ` (${defaultValue})` : ''}: `)).trim()
    if (answer) return answer
    if (defaultValue) return defaultValue
    throw new Error(`${label} is required`)
  } finally {
    input.close()
  }
}

async function version(): Promise<string> {
  const value = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8')) as { version?: unknown }
  if (typeof value.version !== 'string') throw new Error('Package version is missing')
  return value.version
}

async function main(): Promise<void> {
  const command = parseCli(process.argv.slice(2))
  if (command.command === 'help') {
    console.log(HELP)
    return
  }
  if (command.command === 'version') {
    console.log(await version())
    return
  }
  if (command.command === 'connect') {
    const serverUrl = command.serverUrl ?? (await prompt('Raft server URL', 'https://api.raft.build'))
    const agentId = command.agentId ?? (await prompt('Raft external-agent ID'))
    await connectRaft({
      serverUrl,
      agentId,
      stateDirectory: command.stateDirectory,
      replace: command.replace,
    })
    return
  }

  const runtime = await startSupervisedRuntime(command)
  const shutdown = () => void runtime.stop()
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
  try {
    console.log(`[eve-raft] health server listening at ${runtime.healthOrigin}`)
    await runtime.ready
    console.log(`[eve-raft] Eve is ready on the internal port ${command.evePort}`)
    await runtime.done
  } finally {
    process.off('SIGINT', shutdown)
    process.off('SIGTERM', shutdown)
    await runtime.stop()
  }
}

main().catch((error: unknown) => {
  console.error(`[eve-raft] ${error instanceof Error ? error.message : 'Unexpected failure'}`)
  process.exitCode = 1
})
