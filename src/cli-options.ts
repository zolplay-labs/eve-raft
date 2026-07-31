import path from 'node:path'

interface ConnectCommand {
  command: 'connect'
  stateDirectory: string
  serverUrl?: string
  agentId?: string
  replace: boolean
}

interface StartCommand {
  command: 'start'
  stateDirectory: string
  healthHost: string
  healthPort: number
  eveHost: string
  evePort: number
  childCommand: string[]
}

export type CliCommand = ConnectCommand | StartCommand | { command: 'help' } | { command: 'version' }

function port(value: string | undefined, name: string): number {
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > 65_535) throw new Error(`${name} must be a valid port`)
  return parsed
}

function optionValues(argv: string[], allowed: ReadonlySet<string>): Map<string, string | true> {
  const values = new Map<string, string | true>()
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index]!
    if (!allowed.has(name)) throw new Error(`Unknown option: ${name}`)
    if (name === '--replace') {
      values.set(name, true)
      continue
    }
    const value = argv[++index]
    if (!value || value.startsWith('--')) throw new Error(`${name} requires a value`)
    values.set(name, value)
  }
  return values
}

function stringValue(values: Map<string, string | true>, name: string): string | undefined {
  const value = values.get(name)
  return typeof value === 'string' ? value : undefined
}

export function parseCli(argv: string[], environment: NodeJS.ProcessEnv = process.env): CliCommand {
  const [command, ...rest] = argv
  if (!command || command === 'help' || command === '--help' || command === '-h') return { command: 'help' }
  if (command === '--version' || command === '-v' || command === 'version') return { command: 'version' }
  if (command === 'connect') {
    const values = optionValues(rest, new Set(['--server-url', '--agent-id', '--data-dir', '--replace']))
    return {
      command,
      stateDirectory: stringValue(values, '--data-dir') ?? path.resolve('.eve-raft'),
      ...(stringValue(values, '--server-url') ? { serverUrl: stringValue(values, '--server-url')! } : {}),
      ...(stringValue(values, '--agent-id') ? { agentId: stringValue(values, '--agent-id')! } : {}),
      replace: values.get('--replace') === true,
    }
  }
  if (command === 'start') {
    const separator = rest.indexOf('--')
    if (separator < 0 || separator === rest.length - 1) throw new Error('start requires a child command after --')
    const values = optionValues(
      rest.slice(0, separator),
      new Set(['--data-dir', '--health-host', '--health-port', '--eve-host', '--eve-port']),
    )
    return {
      command,
      stateDirectory: stringValue(values, '--data-dir') ?? path.resolve('.eve-raft'),
      healthHost: stringValue(values, '--health-host') ?? '0.0.0.0',
      healthPort: port(stringValue(values, '--health-port') ?? environment.PORT ?? '3000', 'health port'),
      eveHost: stringValue(values, '--eve-host') ?? '127.0.0.1',
      evePort: port(stringValue(values, '--eve-port') ?? '3100', 'Eve port'),
      childCommand: rest.slice(separator + 1),
    }
  }
  throw new Error(`Unknown command: ${command}`)
}
