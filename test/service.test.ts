import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { EveRaftService } from '../src/service.ts'
import { StateStore } from '../src/state.ts'
import { FakeEveServer } from './fake-eve-server.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

async function waitFor(predicate: () => boolean, timeoutMs = 6_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 25))
  if (!predicate()) throw new Error('Timed out waiting for service state')
}

function message(overrides: Record<string, unknown> = {}): Record<string, unknown> {
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
    ...overrides,
  }
}

describe('Eve Raft service', () => {
  let raft: FakeRaftServer
  let eve: FakeEveServer
  let stateDirectory: string

  beforeEach(async () => {
    raft = new FakeRaftServer()
    eve = new FakeEveServer('channel-secret')
    stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-service-'))
    await Promise.all([raft.start(), eve.start()])
    const store = new StateStore(stateDirectory)
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

  afterEach(async () => {
    await Promise.all([raft.stop(), eve.stop()])
  })

  async function service(): Promise<EveRaftService> {
    const instance = new EveRaftService({
      stateDirectory,
      eveOrigin: eve.origin,
      channelToken: 'channel-secret',
    })
    await instance.initialize()
    return instance
  }

  it('delivers a direct conversation with reactions and privacy-safe activity', async () => {
    raft.events.push(message())
    const instance = await service()

    await instance.drain()
    await instance.processNext()

    expect(eve.inputs).toHaveLength(1)
    expect(raft.sent).toEqual([expect.objectContaining({ target: 'dm:@Dex:message-', content: 'Echo: hello' })])
    expect(raft.reactions).toEqual([
      { messageId: 'message-1', emoji: '👀', operation: 'add' },
      { messageId: 'message-1', emoji: '✅', operation: 'add' },
      { messageId: 'message-1', emoji: '👀', operation: 'remove' },
    ])
    expect(raft.activity.map((event) => event.hookEventName)).toEqual([
      'UserPromptSubmit',
      'PreToolUse',
      'PostToolUse',
      'Stop',
      'Stop',
    ])
    expect(JSON.stringify(raft.activity)).not.toContain('hello')
    expect(instance.health).toMatchObject({ state: 'connected', queueDepth: 0, lastError: null })
  })

  it('claims an assigned task and stops at in-review after result delivery', async () => {
    raft.addTask({ channel: '#tasks', taskNumber: 7, title: 'Ship it', status: 'todo', messageId: 'task-7' })
    raft.events.push(
      message({
        id: 'task-7',
        message_id: 'task-7',
        channel_type: 'channel',
        channel_name: 'tasks',
        content: 'Ship it',
        task_number: 7,
        task_status: 'todo',
        task_assignee_id: raft.agentId,
        task_assignee_type: 'agent',
      }),
    )
    let statusAtEveInvocation: string | undefined
    eve.onInput = () => {
      statusAtEveInvocation = raft.tasks[0]?.status
    }
    const instance = await service()

    await instance.drain()
    await instance.processNext()

    expect(raft.taskClaims).toContainEqual({ channel: '#tasks', taskNumber: 7, operation: 'claim' })
    expect(statusAtEveInvocation).toBe('in_progress')
    expect(raft.tasks[0]?.status).toBe('in_review')
    expect(raft.sent[0]).toMatchObject({ target: '#tasks:task-7', content: 'Echo: Ship it' })
  })

  it('does not claim or invoke a task assigned to another agent', async () => {
    raft.addTask({ channel: '#tasks', taskNumber: 8, title: 'Not ours', status: 'todo', messageId: 'task-8' })
    raft.events.push(
      message({
        id: 'task-8',
        message_id: 'task-8',
        channel_type: 'channel',
        channel_name: 'tasks',
        content: 'Not ours',
        task_number: 8,
        task_status: 'todo',
        task_assignee_id: 'agent-other',
        task_assignee_type: 'agent',
      }),
    )
    const instance = await service()

    await instance.drain()
    await instance.processNext()

    expect(eve.inputs).toHaveLength(0)
    expect(raft.taskClaims).toHaveLength(0)
    expect(raft.tasks[0]?.status).toBe('todo')
  })

  it('persists numbered human input and resumes it from the same thread', async () => {
    const instance = await service()
    raft.events.push(message({ content: 'ask me first' }))
    await instance.drain()
    await instance.processNext()
    expect(raft.sent.at(-1)?.content).toContain('1) One')

    raft.events.push(
      message({
        seq: 2,
        id: 'message-2',
        message_id: 'message-2',
        channel_type: 'thread',
        channel_name: 'thread-message-',
        parent_channel_type: 'dm',
        parent_channel_name: 'Dex',
        content: '2',
      }),
    )
    await instance.drain()
    await instance.processNext()

    expect(eve.inputs.at(-1)?.input).toMatchObject({
      inputResponses: [{ requestId: 'approval-request', optionId: 'two' }],
    })
    expect(raft.sent.at(-1)?.content).toBe('Resumed: two')
  })

  it('keeps pending input across a restart and guides an invalid answer', async () => {
    const first = await service()
    raft.events.push(message({ content: 'ask me first' }))
    await first.drain()
    await first.processNext()

    const restarted = await service()
    raft.events.push(
      message({
        seq: 2,
        id: 'message-invalid',
        message_id: 'message-invalid',
        channel_type: 'thread',
        channel_name: 'thread-message-',
        parent_channel_type: 'dm',
        parent_channel_name: 'Dex',
        content: '9',
      }),
    )
    await restarted.drain()
    await restarted.processNext()

    expect(eve.inputs).toHaveLength(1)
    expect(raft.sent.at(-1)?.content).toContain('Please answer the pending question')
    expect(raft.sent.at(-1)?.content).toContain('1) One')

    raft.events.push(
      message({
        seq: 3,
        id: 'message-valid',
        message_id: 'message-valid',
        channel_type: 'thread',
        channel_name: 'thread-message-',
        parent_channel_type: 'dm',
        parent_channel_name: 'Dex',
        content: '1',
      }),
    )
    await restarted.drain()
    await restarted.processNext()

    expect(eve.inputs.at(-1)?.input).toMatchObject({
      inputResponses: [{ requestId: 'approval-request', optionId: 'one' }],
    })
    expect(raft.sent.at(-1)?.content).toBe('Resumed: one')
  })

  it('keeps a pending answer durable until Eve accepts its dispatch', async () => {
    const first = await service()
    raft.events.push(message({ content: 'ask me first' }))
    await first.drain()
    await first.processNext()

    raft.events.push(
      message({
        seq: 2,
        id: 'message-answer',
        message_id: 'message-answer',
        channel_type: 'thread',
        channel_name: 'thread-message-',
        parent_channel_type: 'dm',
        parent_channel_name: 'Dex',
        content: '2',
      }),
    )
    await first.drain()
    eve.failNextDispatch = true
    await expect(first.processNext()).rejects.toThrow('Eve returned HTTP 503')
    expect((await new StateStore(stateDirectory).loadPendingInput()).byReplyTarget).not.toEqual({})

    const restarted = await service()
    await restarted.processNext()

    expect(eve.inputs.at(-1)?.input).toMatchObject({
      inputResponses: [{ requestId: 'approval-request', optionId: 'two' }],
    })
    expect((await new StateStore(stateDirectory).loadPendingInput()).byReplyTarget).toEqual({})
    expect(raft.sent.filter((sent) => sent.content === 'Resumed: two')).toHaveLength(1)
  })

  it('keeps a wrong-thread answer isolated and applies a duplicate only once', async () => {
    const instance = await service()
    raft.events.push(message({ content: 'ask me first' }))
    await instance.drain()
    await instance.processNext()

    raft.events.push(
      message({
        seq: 2,
        id: 'message-wrong-thread',
        message_id: 'message-wrong-thread',
        channel_type: 'thread',
        channel_name: 'thread-somewhere-else',
        parent_channel_type: 'dm',
        parent_channel_name: 'Dex',
        content: '2',
      }),
    )
    await instance.drain()
    await instance.processNext()
    expect(eve.inputs.at(-1)?.input).toMatch(/^<!-- eve-raft-event:[a-f0-9]{64} -->\n2$/u)
    expect((await new StateStore(stateDirectory).loadPendingInput()).byReplyTarget).not.toEqual({})

    const answer = message({
      seq: 3,
      id: 'message-correct-thread',
      message_id: 'message-correct-thread',
      channel_type: 'thread',
      channel_name: 'thread-message-',
      parent_channel_type: 'dm',
      parent_channel_name: 'Dex',
      content: '2',
    })
    raft.events.push(answer, answer)
    await instance.drain()
    await instance.processNext()

    expect(eve.inputs.filter((entry) => !Array.isArray(entry.input) && typeof entry.input === 'object')).toHaveLength(1)
    expect(raft.sent.filter((sent) => sent.content === 'Resumed: two')).toHaveLength(1)
  })

  it('retains the unadmitted tail of one drained Raft page', async () => {
    raft.events.push(
      ...Array.from({ length: 1_001 }, (_, index) =>
        message({ seq: index + 1, id: `message-${index + 1}`, message_id: `message-${index + 1}` }),
      ),
    )
    const instance = await service()

    await instance.drain()
    expect(instance.health.queueDepth).toBe(1_000)
    expect(raft.eventPolls).toBe(1)

    const restarted = await service()
    await restarted.processNext()
    await restarted.drain()

    expect(raft.eventPolls).toBe(1)
    expect((await new StateStore(stateDirectory).loadQueue()).events.at(-1)?.id).toBe('message-1001')
  })

  it('warns for an oversized raw event and admits later work from the same page', async () => {
    raft.events.push(
      message({ id: 'oversized', message_id: 'oversized', content: 'x'.repeat(16 * 1024 * 1024) }),
      message({ seq: 2, id: 'message-safe', message_id: 'message-safe', content: 'safe' }),
    )
    const instance = await service()

    await instance.drain()

    expect(raft.reactions).toContainEqual({ messageId: 'oversized', emoji: '⚠️', operation: 'add' })
    expect((await new StateStore(stateDirectory).loadQueue()).events.map((event) => event.id)).toEqual(['message-safe'])
  })

  it('reloads a replaced credential after Raft revokes the active one', async () => {
    const instance = await service()
    raft.apiKey = 'raft-api-key-replacement'
    const controller = new AbortController()
    const running = instance.run(controller.signal)
    try {
      await waitFor(() => instance.health.state === 'disconnected')
      expect(instance.health.lastError).toBe('credential_rejected')

      await new StateStore(stateDirectory).saveCredential({
        schemaVersion: 1,
        serverUrl: raft.origin,
        agentId: raft.agentId,
        agentName: raft.agentName,
        serverId: raft.serverId,
        credentialId: 'credential-2',
        scopes: ['agent'],
        apiKey: raft.apiKey,
        createdAt: '2026-07-31T00:01:00.000Z',
      })
      await waitFor(() => instance.health.state === 'connected')
      expect(instance.health.lastError).toBeNull()
    } finally {
      controller.abort()
      await running
    }
  }, 10_000)

  it.each(['agent', 'server'] as const)(
    'rejects a stored credential with a missing Raft %s identity',
    async (identity) => {
      if (identity === 'agent') raft.runtimeAgentId = undefined
      else raft.runtimeServerId = undefined
      const instance = new EveRaftService({
        stateDirectory,
        eveOrigin: eve.origin,
        channelToken: 'channel-secret',
      })
      await expect(instance.initialize()).rejects.toThrow(
        'Stored Raft credential does not match the configured agent and server',
      )
    },
  )

  it('does not advance a task without a delivered result', async () => {
    raft.addTask({ channel: '#tasks', taskNumber: 8, title: 'Ship it', status: 'todo', messageId: 'task-8' })
    raft.events.push(
      message({
        id: 'task-8',
        message_id: 'task-8',
        channel_type: 'channel',
        channel_name: 'tasks',
        content: 'Ship it',
        task_number: 8,
        task_assignee_id: raft.agentId,
        task_assignee_type: 'agent',
      }),
    )
    eve.emptyNextResult = true
    const instance = await service()

    await instance.drain()
    await instance.processNext()

    expect(raft.taskClaims).toContainEqual({ channel: '#tasks', taskNumber: 8, operation: 'unclaim' })
    expect(raft.tasks[0]?.status).toBe('todo')
    expect(instance.health.queueDepth).toBe(0)
  })

  it('recovers a durably queued message in a new service instance', async () => {
    raft.events.push(message())
    const first = await service()
    await first.drain()
    expect(first.health.queueDepth).toBe(1)

    const restarted = await service()
    await restarted.processNext()

    expect(raft.sent).toHaveLength(1)
    expect(restarted.health.queueDepth).toBe(0)
  })
})
