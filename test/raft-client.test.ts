import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { RaftClient, authorizeDevice, mintRaftCredential, pollDeviceTokenOnce } from '../src/raft-client.ts'
import type { RaftCredential } from '../src/state.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

describe('Raft protocol client', () => {
  const server = new FakeRaftServer()
  let credential: RaftCredential

  beforeAll(async () => {
    await server.start()
    const authorization = await authorizeDevice(server.origin)
    expect(authorization.verificationUriComplete).toBe(`${server.origin}/login/device?user_code=ABCD-EFGH`)
    expect(await pollDeviceTokenOnce(server.origin, authorization.deviceCode)).toBeNull()
    server.approveDevice()
    const token = await pollDeviceTokenOnce(server.origin, authorization.deviceCode)
    const minted = await mintRaftCredential(server.origin, server.agentId, token!.accessToken)
    credential = {
      schemaVersion: 1,
      serverUrl: server.origin,
      agentId: minted.agentId,
      agentName: minted.agentName,
      serverId: minted.serverId,
      credentialId: minted.credentialId,
      scopes: minted.scopes ?? [],
      apiKey: minted.apiKey,
      createdAt: '2026-07-31T00:00:00.000Z',
    }
  })

  afterAll(() => server.stop())

  it('polls events and performs idempotent Raft lifecycle mutations', async () => {
    const client = new RaftClient(credential)
    server.events.push({ id: 'message-1', content: 'hello' })
    server.addTask({ channel: '#tasks', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-7' })

    expect(await client.profile()).toMatchObject({ kind: 'agent', id: server.agentId })
    expect(await client.serverInfo()).toMatchObject({ runtimeContext: { agentId: server.agentId } })
    expect(await client.drainOnce()).toMatchObject({ events: [{ id: 'message-1' }] })

    await client.addReaction('message-1', '👀')
    await client.removeReaction('message-1', '👀')
    await client.send('#general:message', 'reply', 'reply-message-1', 12)
    await client.send('#general:message', 'reply', 'reply-message-1', 12)
    await client.forwardActivity([
      {
        schema: 'raft-activity.v1',
        eventId: 'activity-1',
        sessionId: 'session-1',
        hookEventName: 'UserPromptSubmit',
        status: 'ok',
        occurredAt: '2026-07-31T00:00:00.000Z',
      },
    ])
    await client.claimTask('#tasks', 7)
    await client.advanceTaskStatus('#tasks', 7, 'in_progress')
    await client.advanceTaskStatus('#tasks', 7, 'in_review')

    expect(server.sent).toEqual([
      { target: '#general:message', content: 'reply', idempotencyKey: 'reply-message-1', seenUpToSeq: 12 },
    ])
    expect(server.reactions).toEqual([
      { messageId: 'message-1', emoji: '👀', operation: 'add' },
      { messageId: 'message-1', emoji: '👀', operation: 'remove' },
    ])
    expect(server.activity).toHaveLength(1)
    expect(server.tasks[0]?.status).toBe('in_review')
    expect(server.taskClaims).toContainEqual({ channel: '#tasks', taskNumber: 7, operation: 'claim' })
  })
})
