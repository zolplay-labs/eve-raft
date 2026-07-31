import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { EveRaftService } from '../src/service.ts'
import { StateStore } from '../src/state.ts'
import { FakeEveServer } from './fake-eve-server.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

describe('retry and restart recovery', () => {
  let raft: FakeRaftServer
  let eve: FakeEveServer
  let directory: string

  beforeEach(async () => {
    raft = new FakeRaftServer()
    eve = new FakeEveServer('channel-secret')
    directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-recovery-'))
    await Promise.all([raft.start(), eve.start()])
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
  })

  afterEach(() => Promise.all([raft.stop(), eve.stop()]))

  function message(): Record<string, unknown> {
    return {
      seq: 1,
      id: 'message-1',
      message_id: 'message-1',
      timestamp: '2026-07-31T00:00:00.000Z',
      sender_type: 'human',
      sender_name: 'cali',
      channel_type: 'dm',
      channel_name: 'Dex',
      content: 'hello',
    }
  }

  async function createService(): Promise<EveRaftService> {
    const service = new EveRaftService({
      stateDirectory: directory,
      eveOrigin: eve.origin,
      channelToken: 'channel-secret',
    })
    await service.initialize()
    return service
  }

  it('reuses a durable Eve dispatch after a process-shaped stream failure', async () => {
    raft.events.push(message())
    const first = await createService()
    await first.drain()
    eve.failNextStream = true

    await expect(first.processNext()).rejects.toThrow('Eve returned HTTP 503')
    expect(first.health.queueDepth).toBe(1)
    expect(eve.inputs).toHaveLength(1)

    const restarted = await createService()
    await restarted.processNext()

    expect(eve.inputs).toHaveLength(1)
    expect(raft.sent).toHaveLength(1)
    expect(restarted.health.queueDepth).toBe(0)
  })

  it('drops a freshness-held stale reply and processes the newer message', async () => {
    raft.events.push(message())
    raft.nextSendFailure = {
      status: 200,
      body: {
        state: 'held',
        reason: 'newer messages',
        seenUpToSeq: 2,
        heldMessages: [{ ...message(), id: 'message-2', message_id: 'message-2', seq: 2, content: 'newer' }],
      },
    }
    const service = await createService()
    await service.drain()

    await service.processNext()
    expect(service.health.queueDepth).toBe(1)
    expect(raft.reactions).toContainEqual({ messageId: 'message-1', emoji: '⚠️', operation: 'add' })

    await service.processNext()
    expect(raft.sent).toHaveLength(1)
    expect(raft.sent[0]).toMatchObject({ content: 'Echo: newer', seenUpToSeq: 2 })
    expect(service.health.queueDepth).toBe(0)
  })

  it('defers a started task behind freshness context and retries its delivery checkpoint', async () => {
    const task = {
      ...message(),
      id: 'task-7',
      message_id: 'task-7',
      channel_type: 'channel',
      channel_name: 'tasks',
      content: 'ship it',
      task_number: 7,
    }
    const newer = { ...message(), id: 'message-2', message_id: 'message-2', seq: 2, content: 'newer' }
    raft.addTask({ channel: '#tasks', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-7' })
    raft.events.push(task)
    raft.nextSendFailure = {
      status: 200,
      body: { state: 'held', reason: 'newer messages', seenUpToSeq: 2, heldMessages: [newer] },
    }
    const service = await createService()
    await service.drain()

    await service.processNext()
    const afterHold = await new StateStore(directory).loadQueue()
    expect(afterHold.events.map((event) => event.id)).toEqual(['message-2', 'task-7'])
    expect(afterHold.events[1]).toMatchObject({ taskPhase: 'started', freshnessSeenUpToSeq: 2 })

    await service.processNext()
    await service.processNext()

    expect(raft.taskClaims.filter((claim) => claim.operation === 'claim')).toHaveLength(1)
    expect(raft.tasks[0]?.status).toBe('in_review')
    expect(raft.sent.filter((sent) => sent.target === '#tasks:task-7')).toHaveLength(1)
    expect(raft.sent.find((sent) => sent.target === '#tasks:task-7')).toMatchObject({ seenUpToSeq: 2 })
    expect(service.health.queueDepth).toBe(0)
  })

  it('drops a deterministic invalid target so later work can proceed', async () => {
    raft.events.push(message(), { ...message(), id: 'message-2', message_id: 'message-2', seq: 2 })
    raft.nextSendFailure = { status: 422, body: { code: 'invalid_target', error: 'invalid_target' } }
    const service = await createService()
    await service.drain()

    await service.processNext()
    expect(service.health.queueDepth).toBe(1)
    await service.processNext()

    expect(raft.sent).toHaveLength(1)
    expect(raft.sent[0]?.content).toBe('Echo: hello')
    expect(service.health.queueDepth).toBe(0)
  })

  it('unclaims a started task when delivery fails permanently', async () => {
    raft.addTask({ channel: '#tasks', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-7' })
    raft.events.push(
      {
        ...message(),
        id: 'task-7',
        message_id: 'task-7',
        channel_type: 'channel',
        channel_name: 'tasks',
        content: 'Ship it',
        task_number: 7,
      },
      { ...message(), id: 'message-2', message_id: 'message-2', seq: 2 },
    )
    raft.nextSendFailure = { status: 422, body: { code: 'invalid_target', error: 'invalid_target' } }
    const service = await createService()
    await service.drain()

    await service.processNext()
    await service.processNext()

    expect(raft.taskClaims).toContainEqual({ channel: '#tasks', taskNumber: 7, operation: 'unclaim' })
    expect(raft.sent).toHaveLength(1)
    expect(service.health.queueDepth).toBe(0)
  })
})
