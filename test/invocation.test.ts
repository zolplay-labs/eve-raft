import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { EveRaftService } from '../src/service.ts'
import { StateStore } from '../src/state.ts'
import { FakeEveServer } from './fake-eve-server.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

describe('shared invocation and system tasks', () => {
  let raft: FakeRaftServer
  let eve: FakeEveServer
  let service: EveRaftService

  beforeEach(async () => {
    raft = new FakeRaftServer('My Agent')
    eve = new FakeEveServer('channel-secret')
    await Promise.all([raft.start(), eve.start()])
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-invocation-'))
    const store = new StateStore(directory)
    await store.initialize()
    await store.saveCredential({
      schemaVersion: 1,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: raft.apiKey,
      createdAt: '2026-07-31T00:00:00.000Z',
    })
    service = new EveRaftService({ stateDirectory: directory, eveOrigin: eve.origin, channelToken: 'channel-secret' })
    await service.initialize()
  })

  afterEach(() => Promise.all([raft.stop(), eve.stop()]))

  function shared(overrides: Record<string, unknown>): Record<string, unknown> {
    return {
      seq: 1,
      id: 'abcdefgh-root',
      message_id: 'abcdefgh-root',
      timestamp: '2026-07-31T00:00:00.000Z',
      sender_type: 'human',
      sender_name: 'cali',
      channel_type: 'channel',
      channel_name: 'general',
      content: 'hello everyone',
      ...overrides,
    }
  }

  it('ignores unrelated shared discussion, invokes a mention, and continues its thread', async () => {
    raft.events.push(shared({ id: 'ignored-1', message_id: 'ignored-1' }))
    await service.drain()
    await service.processNext()
    expect(eve.inputs).toHaveLength(0)

    raft.events.push(shared({ content: `@${raft.agentName} please help` }))
    await service.drain()
    await service.processNext()
    expect(raft.sent.at(-1)).toMatchObject({ target: '#general:abcdefgh', content: 'Echo: please help' })

    raft.events.push(
      shared({
        seq: 2,
        id: 'follow-up',
        message_id: 'follow-up',
        channel_type: 'thread',
        channel_name: 'thread-abcdefgh',
        parent_channel_type: 'channel',
        parent_channel_name: 'general',
        content: 'continue',
      }),
    )
    await service.drain()
    await service.processNext()

    expect(raft.sent.at(-1)).toMatchObject({ target: '#general:abcdefgh', content: 'Echo: continue' })
    expect(eve.inputs).toHaveLength(2)
    expect(eve.inputs[1]?.options).toMatchObject({
      auth: { principalId: 'raft:server-1:human-1' },
      continuationToken: 'server-1:agent-1:#general:abcdefgh',
    })
  })

  it('resolves a system assignment notice to the canonical task thread', async () => {
    raft.addTask({ channel: '#tasks', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-7' })
    const canonical = raft.messages.get('task-7')
    if (!canonical) throw new Error('Missing canonical task fixture')
    raft.events.push(
      shared({
        id: 'notice-1',
        message_id: 'notice-1',
        sender_type: 'system',
        sender_name: 'system',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: '📋 1 new task created: #7 "Ship it"',
      }),
      canonical,
    )

    await service.drain()
    await service.processNext()

    expect(raft.sent).toEqual([expect.objectContaining({ target: '#tasks:task-7', content: 'Echo: Ship it' })])
    expect(raft.reactions).toContainEqual({ messageId: 'task-7', emoji: '✅', operation: 'add' })
    expect(raft.tasks[0]?.status).toBe('in_review')
    expect(await service.processNext()).toBe(false)
    expect(raft.taskClaims.filter((claim) => claim.operation === 'claim')).toHaveLength(1)
  })

  it('resolves a production-shaped started-task receipt past inaccessible task boards', async () => {
    raft.missingTaskBoardsReturn404 = true
    raft.addTask({
      channel: '#tasks',
      taskNumber: 7,
      title: 'Ship it',
      status: 'in_progress',
      messageId: 'task-7',
      senderName: raft.agentName,
      senderType: 'agent',
    })
    raft.events.push(
      shared({
        id: 'receipt-1',
        message_id: 'receipt-1',
        sender_type: 'system',
        sender_name: 'system',
        channel_type: 'channel',
        channel_name: 'tasks',
        content: `📌 @${raft.agentName} started task #7 "Ship it"`,
      }),
    )

    await service.drain()
    await service.processNext()

    expect(raft.sent).toEqual([expect.objectContaining({ target: '#tasks:task-7', content: 'Echo: Ship it' })])
    expect(raft.reactions).toContainEqual({ messageId: 'task-7', emoji: '✅', operation: 'add' })
    expect(raft.tasks[0]?.status).toBe('in_review')
    expect(await service.processNext()).toBe(false)
    expect(raft.taskClaims.filter((claim) => claim.operation === 'claim')).toHaveLength(1)
  })

  it.each(['missing', 'ambiguous'])('drops a %s system task resolution without blocking later work', async (mode) => {
    if (mode === 'ambiguous') {
      raft.addTask({ channel: '#tasks-a', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-a' })
      raft.addTask({ channel: '#tasks-b', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-b' })
    }
    raft.events.push(
      shared({
        id: 'notice-invalid',
        message_id: 'notice-invalid',
        sender_type: 'system',
        sender_name: 'system',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: '📋 1 new task created: #7 "Ship it"',
      }),
      shared({
        id: 'after-task',
        message_id: 'after-task',
        channel_type: 'dm',
        channel_name: raft.agentName,
        content: 'still here',
      }),
    )

    await service.drain()
    await service.processNext()
    await service.processNext()

    expect(raft.reactions).toContainEqual({ messageId: 'notice-invalid', emoji: '⚠️', operation: 'add' })
    expect(raft.sent.at(-1)).toMatchObject({ content: 'Echo: still here' })
    expect(service.health.queueDepth).toBe(0)
  })
})
