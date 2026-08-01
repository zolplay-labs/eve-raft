import { createServer } from 'node:net'
import { lstat, mkdir, mkdtemp, readFile, realpath, stat, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { describe, expect, it } from 'vitest'

import { assertRuntimeCompatibility, startSupervisedRuntime } from '../src/runtime.ts'
import { StateStore } from '../src/state.ts'

const root = path.resolve(import.meta.dirname, '..')

async function runtimeWorkspace(prefix: string): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), prefix))
  await writeFile(path.join(directory, 'package.json'), '{"private":true}\n')
  await symlink(path.join(root, 'node_modules'), path.join(directory, 'node_modules'), 'dir')
  return directory
}

async function unusedPort(): Promise<number> {
  const server = createServer()
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('Could not allocate a test port')
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
  return address.port
}

describe('co-located Eve supervisor', () => {
  it('injects private channel settings and exposes redacted liveness while unconfigured', async () => {
    const directory = await runtimeWorkspace('eve-raft-runtime-')
    const originalWorkflowDirectory = path.join(directory, '.eve', '.workflow-data')
    await mkdir(originalWorkflowDirectory, { recursive: true })
    await writeFile(path.join(originalWorkflowDirectory, 'existing-session'), 'preserved')
    const evePort = await unusedPort()
    const childScript = [
      "const http = require('node:http')",
      "const host = process.argv[process.argv.indexOf('--host') + 1]",
      "const port = Number(process.argv[process.argv.indexOf('--port') + 1])",
      'const server = http.createServer((req, res) => {',
      "  res.writeHead(req.url === '/eve/v1/health' ? 200 : 404, { 'content-type': 'application/json' })",
      "  res.end(JSON.stringify({ ok: req.url === '/eve/v1/health', tokenInjected: Boolean(process.env.EVE_RAFT_CHANNEL_TOKEN), address: server.address().address }))",
      '})',
      'server.listen(port, host)',
      "process.on('SIGTERM', () => server.close())",
    ].join(';')
    const runtime = await startSupervisedRuntime({
      stateDirectory: directory,
      healthHost: '127.0.0.1',
      healthPort: 0,
      eveHost: '127.0.0.1',
      evePort,
      childCommand: [process.execPath, '-e', childScript, '--'],
      cwd: directory,
    })

    try {
      await runtime.ready
      const health = await (await fetch(new URL('/health', runtime.healthOrigin))).json()
      expect(health).toMatchObject({ ok: true, eveReady: true, state: 'unconfigured' })
      expect(health).toMatchObject({
        runtime: { node: process.versions.node, eve: '0.29.2', eveRaft: '0.1.0' },
      })
      expect(JSON.stringify(health)).not.toContain('channelToken')
      expect(JSON.stringify(health)).not.toContain('apiKey')

      const eveHealth = await (await fetch(`http://127.0.0.1:${evePort}/eve/v1/health`)).json()
      expect(eveHealth).toMatchObject({ ok: true, tokenInjected: true, address: '127.0.0.1' })
      const store = new StateStore(directory)
      expect((await stat(store.settingsPath)).mode & 0o777).toBe(0o600)
      expect((await stat(store.workflowDirectory)).mode & 0o777).toBe(0o700)
      const localWorkflowDirectory = path.join(directory, '.eve', '.workflow-data')
      expect((await lstat(localWorkflowDirectory)).isSymbolicLink()).toBe(true)
      expect(await realpath(localWorkflowDirectory)).toBe(await realpath(store.workflowDirectory))
      expect(await readFile(path.join(store.workflowDirectory, 'existing-session'), 'utf8')).toBe('preserved')
    } finally {
      await runtime.stop()
      await runtime.done
    }
  })

  it('rejects unsupported runtime versions and non-loopback Eve listeners', async () => {
    expect(() => assertRuntimeCompatibility({ node: '23.9.0', eve: '0.29.2' })).toThrow('Node.js 24 or newer')
    expect(() => assertRuntimeCompatibility({ node: '24.0.0', eve: '0.30.0' })).toThrow('Eve >=0.29.2 <0.30')
    expect(() => assertRuntimeCompatibility({ node: '24.0.0', eve: '0.29.1' })).toThrow('Eve >=0.29.2 <0.30')

    await expect(
      startSupervisedRuntime({
        stateDirectory: await mkdtemp(path.join(tmpdir(), 'eve-raft-runtime-host-')),
        healthHost: '127.0.0.1',
        healthPort: 0,
        eveHost: '0.0.0.0',
        evePort: await unusedPort(),
        childCommand: [process.execPath, '-e', 'process.exit(0)'],
      }),
    ).rejects.toThrow('loopback host')
  })

  it.skipIf(process.platform === 'win32')('stops the child process group when a wrapper exits first', async () => {
    const directory = await runtimeWorkspace('eve-raft-runtime-stubborn-')
    const evePort = await unusedPort()
    const eveScript = [
      "const http = require('node:http')",
      "const host = process.argv[process.argv.indexOf('--host') + 1]",
      "const port = Number(process.argv[process.argv.indexOf('--port') + 1])",
      "const server = http.createServer((req, res) => { res.writeHead(req.url === '/eve/v1/health' ? 200 : 404); res.end() })",
      'server.listen(port, host)',
      "process.on('SIGTERM', () => {})",
    ].join(';')
    const wrapperScript = [
      `const { spawn } = require('node:child_process')`,
      `const child = spawn(process.execPath, ['-e', ${JSON.stringify(eveScript)}, '--', ...process.argv.slice(1)], { stdio: 'inherit' })`,
      "process.on('SIGTERM', () => process.exit(0))",
      "child.on('exit', (code) => process.exit(code ?? 0))",
    ].join(';')
    const runtime = await startSupervisedRuntime({
      stateDirectory: directory,
      healthHost: '127.0.0.1',
      healthPort: 0,
      eveHost: '127.0.0.1',
      evePort,
      childCommand: [process.execPath, '-e', wrapperScript, '--'],
      childShutdownGraceMs: 25,
      childKillWaitMs: 250,
      cwd: directory,
    })

    await runtime.ready
    await expect(
      Promise.race([
        runtime.stop().then(() => 'stopped'),
        new Promise<string>((resolve) => setTimeout(() => resolve('timed out'), 2_000)),
      ]),
    ).resolves.toBe('stopped')
    await runtime.done

    const replacement = createServer()
    try {
      await new Promise<void>((resolve, reject) =>
        replacement.listen(evePort, '127.0.0.1', resolve).once('error', reject),
      )
    } finally {
      await new Promise<void>((resolve, reject) =>
        replacement.listening ? replacement.close((error) => (error ? reject(error) : resolve())) : resolve(),
      )
    }
  })
})
