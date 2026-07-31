import { mkdtemp, readFile, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { describe, expect, it } from 'vitest'

import { connectRaft } from '../src/connect.ts'
import { StateStore } from '../src/state.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

describe('eve-raft connect', () => {
  it('authorizes, validates, and stores a restrictive Raft credential', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-connect-'))
    const server = new FakeRaftServer()
    const lines: string[] = []
    await server.start()

    try {
      const result = await connectRaft(
        { agentId: server.agentId, serverUrl: server.origin, stateDirectory: directory },
        {
          log: (line) => {
            lines.push(line)
            if (line.includes('ABCD-EFGH')) server.approveDevice()
          },
          sleep: async () => undefined,
        },
      )

      expect(result).toMatchObject({
        agentId: server.agentId,
        agentName: server.agentName,
        serverId: server.serverId,
        serverUrl: server.origin,
      })
      expect(lines.join('\n')).toContain(`${server.origin}/login/device?user_code=ABCD-EFGH`)
      expect(lines.join('\n')).toContain('Code: ABCD-EFGH')

      const store = new StateStore(directory)
      expect(await store.loadCredential()).toMatchObject({ apiKey: server.apiKey, agentId: server.agentId })
      expect((await stat(store.credentialPath)).mode & 0o777).toBe(0o600)
      expect((await stat(directory)).mode & 0o777).toBe(0o700)
      expect(await readFile(store.credentialPath, 'utf8')).not.toContain('access-token')
    } finally {
      await server.stop()
    }
  })

  it.each(['agent', 'server'] as const)('fails closed when the Raft %s identity is missing', async (identity) => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-connect-identity-'))
    const server = new FakeRaftServer()
    if (identity === 'agent') server.runtimeAgentId = undefined
    else server.runtimeServerId = undefined
    server.approveDevice()
    await server.start()

    try {
      await expect(
        connectRaft(
          { agentId: server.agentId, serverUrl: server.origin, stateDirectory: directory },
          { log: () => undefined, sleep: async () => undefined },
        ),
      ).rejects.toThrow('Raft credential validation did not match the requested external agent')
      await expect(new StateStore(directory).loadCredential()).resolves.toBeNull()
    } finally {
      await server.stop()
    }
  })
})
