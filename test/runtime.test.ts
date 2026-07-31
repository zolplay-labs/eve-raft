import { createServer } from 'node:net'
import { mkdtemp, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { describe, expect, it } from 'vitest'

import { assertRuntimeCompatibility, startSupervisedRuntime } from '../src/runtime.ts'
import { StateStore } from '../src/state.ts'

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
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-runtime-'))
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

  it('escalates shutdown when the Eve child ignores SIGTERM', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-runtime-stubborn-'))
    const evePort = await unusedPort()
    const childScript = [
      "const http = require('node:http')",
      "const host = process.argv[process.argv.indexOf('--host') + 1]",
      "const port = Number(process.argv[process.argv.indexOf('--port') + 1])",
      "const server = http.createServer((req, res) => { res.writeHead(req.url === '/eve/v1/health' ? 200 : 404); res.end() })",
      'server.listen(port, host)',
      "process.on('SIGTERM', () => {})",
    ].join(';')
    const runtime = await startSupervisedRuntime({
      stateDirectory: directory,
      healthHost: '127.0.0.1',
      healthPort: 0,
      eveHost: '127.0.0.1',
      evePort,
      childCommand: [process.execPath, '-e', childScript, '--'],
      childShutdownGraceMs: 25,
      childKillWaitMs: 250,
    })

    await runtime.ready
    await expect(
      Promise.race([
        runtime.stop().then(() => 'stopped'),
        new Promise<string>((resolve) => setTimeout(() => resolve('timed out'), 2_000)),
      ]),
    ).resolves.toBe('stopped')
    await runtime.done
  })
})
