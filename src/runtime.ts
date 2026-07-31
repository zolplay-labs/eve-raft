import { spawn, type ChildProcess } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import { createServer, type Server } from 'node:http'
import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { EveRaftService } from './service.js'
import { StateStore } from './state.js'

export interface SupervisedRuntimeOptions {
  stateDirectory: string
  healthHost: string
  healthPort: number
  eveHost: string
  evePort: number
  childCommand: string[]
  cwd?: string
  childShutdownGraceMs?: number
  childKillWaitMs?: number
}

export interface SupervisedRuntime {
  healthOrigin: string
  ready: Promise<void>
  done: Promise<void>
  stop(): Promise<void>
}

interface ChildExit {
  code: number | null
  signal: NodeJS.Signals | null
}

const DEFAULT_CHILD_SHUTDOWN_GRACE_MS = 10_000
const DEFAULT_CHILD_KILL_WAIT_MS = 5_000

export interface RuntimeVersions {
  node: string
  eve: string
  eveRaft: string
}

function numericVersion(value: string): [number, number, number] | null {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/u.exec(value)
  return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null
}

export function assertRuntimeCompatibility(versions: Pick<RuntimeVersions, 'node' | 'eve'>): void {
  const node = numericVersion(versions.node)
  if (!node || node[0] < 24)
    throw new Error(`Unsupported Node.js ${versions.node}; Eve Raft requires Node.js 24 or newer`)
  const eve = numericVersion(versions.eve)
  if (!eve || eve[0] !== 0 || eve[1] !== 29 || eve[2] < 2) {
    throw new Error(`Unsupported Eve ${versions.eve}; Eve Raft requires Eve >=0.29.2 <0.30`)
  }
}

async function packageVersion(packagePath: string): Promise<string> {
  const value = JSON.parse(await readFile(packagePath, 'utf8')) as { version?: unknown }
  if (typeof value.version !== 'string') throw new Error(`Package version is missing from ${packagePath}`)
  return value.version
}

async function runtimeVersions(cwd: string): Promise<RuntimeVersions> {
  const requireFromApp = createRequire(path.join(cwd, 'package.json'))
  let evePackagePath: string
  try {
    evePackagePath = requireFromApp.resolve('eve/package.json')
  } catch {
    throw new Error(`Eve is not installed in ${cwd}`)
  }
  const versions = {
    node: process.versions.node,
    eve: await packageVersion(evePackagePath),
    eveRaft: await packageVersion(fileURLToPath(new URL('../package.json', import.meta.url))),
  }
  assertRuntimeCompatibility(versions)
  return versions
}

function supervisedChildCommand(command: string[], host: string, port: number): string[] {
  const rewritten: string[] = []
  for (let index = 0; index < command.length; index += 1) {
    const argument = command[index]!
    if (argument === '--host' || argument === '--port') {
      index += 1
      continue
    }
    if (argument.startsWith('--host=') || argument.startsWith('--port=')) continue
    rewritten.push(argument)
  }
  return [...rewritten, '--host', host, '--port', String(port)]
}

function closeServer(server: Server): Promise<void> {
  if (!server.listening) return Promise.resolve()
  return new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
}

function childExit(child: ChildProcess): Promise<ChildExit> {
  return new Promise((resolve, reject) => {
    child.once('error', reject)
    child.once('exit', (code, signal) => resolve({ code, signal }))
  })
}

async function waitForChildExit(exited: Promise<ChildExit>, timeoutMs: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      exited.then(
        () => true,
        () => true,
      ),
      new Promise<boolean>((resolve) => {
        timer = setTimeout(() => resolve(false), timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

async function terminateChild(
  child: ChildProcess,
  exited: Promise<ChildExit>,
  shutdownGraceMs: number,
  killWaitMs: number,
): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return
  child.kill('SIGTERM')
  if (await waitForChildExit(exited, shutdownGraceMs)) return
  if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
  await waitForChildExit(exited, killWaitMs)
}

async function waitForEve(origin: string, child: ChildProcess, signal: AbortSignal): Promise<void> {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    if (signal.aborted) throw signal.reason ?? new Error('Eve startup was cancelled')
    if (child.exitCode !== null || child.signalCode !== null) throw new Error('Eve exited before becoming ready')
    try {
      const response = await fetch(new URL('/eve/v1/health', origin), { signal: AbortSignal.timeout(1_000) })
      if (response.ok) return
    } catch {
      // The child is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error('Eve did not become ready within 60 seconds')
}

export async function startSupervisedRuntime(options: SupervisedRuntimeOptions): Promise<SupervisedRuntime> {
  if (options.childCommand.length === 0) throw new Error('An Eve child command is required')
  if (options.eveHost !== '127.0.0.1' && options.eveHost !== '::1') {
    throw new Error('The supervised Eve server must use a loopback host')
  }
  const cwd = options.cwd ?? process.cwd()
  const versions = await runtimeVersions(cwd)
  const store = new StateStore(options.stateDirectory)
  await store.initialize()
  const settings = await store.loadOrCreateSettings()
  const eveOrigin = `http://${options.eveHost.includes(':') ? `[${options.eveHost}]` : options.eveHost}:${options.evePort}`
  const service = new EveRaftService({
    stateDirectory: options.stateDirectory,
    eveOrigin,
    channelToken: settings.channelToken,
  })
  const controller = new AbortController()
  let eveReady = false
  let stopping = false
  let shutdownPromise: Promise<void> | null = null
  let childTerminationPromise: Promise<void> | null = null

  const [command, ...args] = supervisedChildCommand(options.childCommand, options.eveHost, options.evePort)
  const child = spawn(command!, args, {
    cwd,
    env: {
      ...process.env,
      EVE_RAFT_CHANNEL_TOKEN: settings.channelToken,
      PORT: String(options.evePort),
    },
    stdio: 'inherit',
  })
  const exited = childExit(child)
  const shutdownGraceMs = options.childShutdownGraceMs ?? DEFAULT_CHILD_SHUTDOWN_GRACE_MS
  const killWaitMs = options.childKillWaitMs ?? DEFAULT_CHILD_KILL_WAIT_MS

  const stopChild = (): Promise<void> => {
    childTerminationPromise ??= terminateChild(child, exited, shutdownGraceMs, killWaitMs)
    return childTerminationPromise
  }

  const healthServer = createServer((request, response) => {
    if (request.method !== 'GET' || request.url !== '/health') {
      response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' })
      response.end('Not found')
      return
    }
    const live =
      !stopping &&
      service.health.state !== 'error' &&
      service.health.state !== 'disconnected' &&
      child.exitCode === null &&
      child.signalCode === null
    response.writeHead(live ? 200 : 503, {
      'cache-control': 'no-store',
      'content-type': 'application/json',
    })
    response.end(JSON.stringify({ ok: live, eveReady, runtime: versions, ...service.health }))
  })
  await new Promise<void>((resolve, reject) => {
    healthServer.once('error', reject)
    healthServer.listen(options.healthPort, options.healthHost, resolve)
  })
  const address = healthServer.address()
  if (!address || typeof address === 'string') throw new Error('Health server did not bind to a TCP port')
  const healthHost = options.healthHost === '0.0.0.0' || options.healthHost === '::' ? '127.0.0.1' : options.healthHost
  const healthOrigin = `http://${healthHost.includes(':') ? `[${healthHost}]` : healthHost}:${address.port}`

  const ready = waitForEve(eveOrigin, child, controller.signal).then(async () => {
    eveReady = true
    await service.initialize()
  })
  const shutdown = (): Promise<void> => {
    shutdownPromise ??= (async () => {
      stopping = true
      controller.abort()
      try {
        await stopChild()
      } finally {
        await closeServer(healthServer)
      }
    })()
    return shutdownPromise
  }
  const done = (async () => {
    try {
      await ready
      const outcome = await Promise.race([
        service.run(controller.signal).then(() => ({ kind: 'service' as const })),
        exited.then((value) => ({ kind: 'child' as const, value })),
      ])
      if (!stopping && outcome.kind === 'child') {
        throw new Error(`Eve exited unexpectedly with code ${String(outcome.value.code)}`)
      }
    } catch (error) {
      service.setFatalError(error)
      throw error
    } finally {
      await shutdown()
    }
  })()
  void done.catch(() => undefined)

  return {
    healthOrigin,
    ready,
    done,
    async stop() {
      await shutdown()
    },
  }
}
