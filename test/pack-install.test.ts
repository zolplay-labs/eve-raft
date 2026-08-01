import { execFile as execFileCallback, spawn, type ChildProcess } from 'node:child_process'
import { createServer, type Server } from 'node:http'
import { copyFile, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { promisify } from 'node:util'

import { describe, expect, it } from 'vitest'

import { FakeRaftServer } from './fake-raft-server.ts'

const execFile = promisify(execFileCallback)
const root = path.resolve(import.meta.dirname, '..')
const PNG_BYTES = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAAqADAAQAAAABAAAAAgAAAADtGLyqAAAAFklEQVQIHWOW1zT34Z3NqJr8+nXYNAAY3ATCCT8yegAAAABJRU5ErkJggg==',
  'base64',
)

function stopProcess(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve()
  child.kill('SIGTERM')
  return new Promise((resolve) => child.once('exit', () => resolve()))
}

async function waitForHealth(origin: string, path = '/health'): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(new URL(path, origin))
      if (response.ok) {
        const health = (await response.json()) as Record<string, unknown>
        if (health.eveReady === true && health.state !== 'starting') return health
      }
    } catch {
      // The supervised fixture is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error('Packed standalone Eve Raft fixture did not become healthy')
}

async function waitForSentContent(raft: FakeRaftServer, fromIndex: number, expected: string): Promise<void> {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    if (raft.sent.slice(fromIndex).some((message) => message.content.includes(expected))) return
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(
    `Packed standalone Eve Raft fixture did not send: ${expected}; sent: ${JSON.stringify(raft.sent.slice(fromIndex))}`,
  )
}

async function waitForQueueDepth(origin: string, expected: number): Promise<void> {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    const response = await fetch(new URL('/health', origin))
    if (response.ok) {
      const health = (await response.json()) as Record<string, unknown>
      if (health.queueDepth === expected) return
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`Packed standalone Eve Raft fixture queue did not reach depth ${expected}`)
}

describe('packed registry installation', () => {
  it('installs the package through Eve and runs the real root channel', async () => {
    const workspace = await mkdtemp(path.join(tmpdir(), 'eve-raft-pack-'))
    const packDirectory = path.join(workspace, 'pack')
    const fixtureDirectory = path.join(workspace, 'fixture')
    const stateDirectory = path.join(workspace, 'state')
    const standaloneFixtureDirectory = path.join(root, 'fixtures/standalone')
    const standaloneFixtureManifest = JSON.parse(
      await readFile(path.join(standaloneFixtureDirectory, 'package.json'), 'utf8'),
    ) as { dependencies?: Record<string, string> }
    expect(standaloneFixtureManifest.dependencies).toMatchObject({ 'just-bash': '3.1.0' })
    await execFile(process.execPath, ['--input-type=module', '-e', 'await import("just-bash")'], {
      cwd: standaloneFixtureDirectory,
    })

    const raft = new FakeRaftServer()
    await raft.start()
    await mkdir(packDirectory)
    await mkdir(path.join(fixtureDirectory, 'agent'), { recursive: true })

    await execFile('pnpm', ['build'], { cwd: root })
    const packed = await execFile('pnpm', ['pack', '--pack-destination', packDirectory], { cwd: root })
    const tarballName = packed.stdout.trim().split('\n').at(-1)
    if (!tarballName) throw new Error('pnpm pack did not return an artifact path')
    const tarball = path.isAbsolute(tarballName) ? tarballName : path.join(packDirectory, tarballName)
    const packedManifest = JSON.parse((await execFile('tar', ['-xOf', tarball, 'package/package.json'])).stdout) as {
      name?: unknown
      version?: unknown
    }
    expect(packedManifest).toMatchObject({ name: '@zolplay/eve-raft', version: '0.2.0' })

    const fixturePackage = {
      name: 'eve-raft-packed-fixture',
      version: '0.0.0',
      private: true,
      type: 'module',
      dependencies: { ai: '^7.0.38', eve: '0.29.2' },
      engines: { node: '>=24' },
    }
    await writeFile(path.join(fixtureDirectory, 'package.json'), `${JSON.stringify(fixturePackage, null, 2)}\n`)
    await copyFile(path.join(root, 'fixtures/standalone/agent/agent.ts'), path.join(fixtureDirectory, 'agent/agent.ts'))
    await copyFile(
      path.join(root, 'fixtures/standalone/agent/instructions.md'),
      path.join(fixtureDirectory, 'agent/instructions.md'),
    )
    await execFile('pnpm', ['install'], { cwd: fixtureDirectory })

    const registryItem = JSON.parse(await readFile(path.join(root, 'registry/r/raft.json'), 'utf8')) as {
      dependencies: string[]
    }
    expect(registryItem.dependencies).toEqual([`@zolplay/eve-raft@^${packedManifest.version}`])
    registryItem.dependencies = [`@zolplay/eve-raft@file:${tarball}`]
    let registryServer: Server | null = createServer((request, response) => {
      if (request.url !== '/r/raft.json') {
        response.writeHead(404)
        response.end()
        return
      }
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(JSON.stringify(registryItem))
    })
    await new Promise<void>((resolve) => registryServer!.listen(0, '127.0.0.1', resolve))
    const registryAddress = registryServer.address()
    if (!registryAddress || typeof registryAddress === 'string') throw new Error('Registry server did not bind')
    const registryOrigin = `http://127.0.0.1:${registryAddress.port}`

    let eve: ChildProcess | null = null
    try {
      await execFile('pnpm', ['exec', 'eve', 'registry', 'add', `@zolplay=${registryOrigin}/r/{name}.json`], {
        cwd: fixtureDirectory,
      })
      await execFile('pnpm', ['exec', 'eve', 'add', '@zolplay/raft', '-y'], { cwd: fixtureDirectory })
      expect(await readFile(path.join(fixtureDirectory, 'agent/channels/raft.ts'), 'utf8')).toContain(
        "from '@zolplay/eve-raft/channel'",
      )
      const consumerImport = await execFile(
        process.execPath,
        [
          '--input-type=module',
          '-e',
          "const api = await import('@zolplay/eve-raft/consumer'); console.log(typeof api.EveRaftService)",
        ],
        { cwd: fixtureDirectory },
      )
      expect(consumerImport.stdout.trim()).toBe('function')
      await execFile('pnpm', ['exec', 'eve', 'build', '--skip-sandbox-prewarm'], {
        cwd: fixtureDirectory,
        env: { ...process.env, EVE_RAFT_CHANNEL_TOKEN: 'packed-channel-token' },
        maxBuffer: 20 * 1024 * 1024,
      })

      const eveRaftBinary = path.join(fixtureDirectory, 'node_modules/.bin/eve-raft')
      const eveBinary = path.join(fixtureDirectory, 'node_modules/.bin/eve')
      raft.approveDevice()
      await execFile(
        eveRaftBinary,
        ['connect', '--data-dir', stateDirectory, '--server-url', raft.origin, '--agent-id', raft.agentId],
        { cwd: fixtureDirectory },
      )

      const healthPort = 32_000 + Math.floor(Math.random() * 5_000)
      const evePort = healthPort + 5_000
      const origin = `http://127.0.0.1:${healthPort}`
      const startArguments = [
        'start',
        '--data-dir',
        stateDirectory,
        '--health-host',
        '127.0.0.1',
        '--health-port',
        String(healthPort),
        '--eve-port',
        String(evePort),
        '--',
        eveBinary,
        'start',
      ]
      const startFixture = () =>
        spawn(eveRaftBinary, startArguments, {
          cwd: fixtureDirectory,
          env: { ...process.env, NODE_ENV: 'production' },
          stdio: 'ignore',
        })
      eve = startFixture()
      const health = await waitForHealth(origin)
      expect(health).toMatchObject({ ok: true, eveReady: true, state: 'connected', protocolVersion: 1 })
      expect(JSON.stringify(health)).not.toContain(raft.apiKey)
      expect(JSON.stringify(health)).not.toContain(raft.serverId)
      expect(JSON.stringify(health)).not.toContain(raft.agentId)
      expect(JSON.stringify(health)).not.toContain(raft.agentName)

      raft.events.push({
        seq: 1,
        id: 'message-packed',
        message_id: 'message-packed',
        timestamp: '2026-07-31T00:00:00.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: 'hello from packed fixture',
      })
      const replyDeadline = Date.now() + 15_000
      while (raft.sent.length === 0 && Date.now() < replyDeadline) {
        await new Promise((resolve) => setTimeout(resolve, 100))
      }
      expect(raft.sent).toEqual([
        expect.objectContaining({
          target: `dm:@${raft.agentName}:message-`,
          content: expect.stringContaining('hello from packed fixture'),
        }),
      ])
      expect(raft.activity.length).toBeGreaterThan(0)

      raft.attachments.set('packed-attachment', { bytes: PNG_BYTES, mediaType: 'image/png' })
      raft.events.push({
        seq: 2,
        id: 'message-packed-attachment',
        message_id: 'message-packed-attachment',
        timestamp: '2026-07-31T00:00:01.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: 'hello with packed attachment',
        attachments: [{ id: 'packed-attachment', filename: 'fixture.png' }],
      })
      await waitForSentContent(raft, 1, 'Fixture received PNG attachment')
      expect(raft.sent).toHaveLength(2)
      expect(raft.sent[1]).toMatchObject({ content: expect.stringContaining('Fixture received PNG attachment') })

      raft.events.push({
        seq: 3,
        id: 'hitlpack-root',
        message_id: 'hitlpack-root',
        timestamp: '2026-07-31T00:00:02.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: 'ask me before restart',
      })
      await waitForSentContent(raft, 2, '1) Ship')
      await waitForQueueDepth(origin, 0)

      await stopProcess(eve)
      eve = startFixture()
      expect(await waitForHealth(origin)).toMatchObject({ state: 'connected' })
      raft.events.push({
        seq: 4,
        id: 'hitlpack-answer',
        message_id: 'hitlpack-answer',
        timestamp: '2026-07-31T00:00:03.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'thread',
        channel_name: 'thread-hitlpack',
        parent_channel_type: 'dm',
        parent_channel_name: raft.agentName,
        content: '1',
      })
      await waitForSentContent(raft, 3, 'Fixture resumed:')
      expect(raft.sent.filter((message) => message.content.includes('Fixture resumed:'))).toHaveLength(1)

      raft.events.push({
        seq: 5,
        id: 'message-after-restart',
        message_id: 'message-after-restart',
        timestamp: '2026-07-31T00:00:04.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: 'after restart',
      })
      await waitForSentContent(raft, 4, 'after restart')
      expect(raft.sent).toHaveLength(5)
      expect(raft.sent[4]).toMatchObject({ content: expect.stringContaining('after restart') })
    } finally {
      if (eve) await stopProcess(eve)
      await raft.stop()
      await new Promise<void>((resolve, reject) =>
        registryServer!.close((error) => (error ? reject(error) : resolve())),
      )
      registryServer = null
    }
  }, 120_000)
})
