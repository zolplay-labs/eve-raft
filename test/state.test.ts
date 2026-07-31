import { mkdtemp, readFile, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { describe, expect, it } from 'vitest'

import { StateStore } from '../src/state.ts'

describe('persistent state', () => {
  it('persists credentials and queue checkpoints with restrictive permissions', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-state-'))
    const store = new StateStore(directory)
    await store.initialize()
    await store.saveCredential({
      schemaVersion: 1,
      serverUrl: 'https://app.raft.build',
      agentId: 'agent-1',
      agentName: 'Dex',
      serverId: 'server-1',
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: 'secret',
      createdAt: '2026-07-31T00:00:00.000Z',
    })

    const queue = await store.loadQueue()
    await store.appendEvents(queue, [
      { id: 'message-1', content: 'first' },
      { id: 'message-1', content: 'duplicate' },
      { id: 'message-2', content: 'second' },
    ])
    await store.checkpointHead(queue, 'message-1', { taskPhase: 'started' })

    const reloaded = new StateStore(directory)
    expect(await reloaded.loadCredential()).toMatchObject({ agentId: 'agent-1', apiKey: 'secret' })
    expect(await reloaded.loadQueue()).toMatchObject({
      events: [{ id: 'message-1', taskPhase: 'started' }, { id: 'message-2' }],
    })
    expect((await stat(store.credentialPath)).mode & 0o777).toBe(0o600)
    expect((await stat(store.queuePath)).mode & 0o777).toBe(0o600)
    expect(await readFile(store.queuePath, 'utf8')).not.toContain('duplicate')
  })

  it('advances one durable event without losing the remaining queue', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-queue-'))
    const store = new StateStore(directory)
    await store.initialize()
    const queue = await store.loadQueue()
    await store.appendEvents(queue, [{ id: 'message-1' }, { id: 'message-2' }])

    await store.shiftEvent(queue, 'message-1')

    expect(queue.events.map((event) => event.id)).toEqual(['message-2'])
    expect((await store.loadQueue()).events.map((event) => event.id)).toEqual(['message-2'])
  })

  it('atomically defers a checkpointed head behind newer freshness context', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-defer-'))
    const store = new StateStore(directory)
    await store.initialize()
    const queue = await store.loadQueue()
    await store.appendEvents(queue, [{ id: 'message-1', seq: 1 }])
    await store.checkpointHead(queue, 'message-1', { taskPhase: 'started' })

    await store.deferHeadEvent(queue, 'message-1', [{ id: 'message-2', seq: 2 }], { seenUpToSeq: 2 })

    expect(queue.events).toMatchObject([
      { id: 'message-2' },
      { id: 'message-1', taskPhase: 'started', freshnessSeenUpToSeq: 2 },
    ])
    expect((await store.loadQueue()).events).toEqual(queue.events)
  })

  it('drops an individually oversized raw event without losing the following event', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-oversized-'))
    const store = new StateStore(directory)
    await store.initialize()
    const queue = await store.loadQueue()

    const result = await store.appendEvents(queue, [
      { id: 'oversized', content: 'x'.repeat(16 * 1024 * 1024) },
      { id: 'message-2', content: 'safe' },
    ])

    expect(result).toMatchObject({ added: 1, consumed: 2, dropped: ['oversized'] })
    expect(queue.events.map((event) => event.id)).toEqual(['message-2'])
  })

  it('fails closed on malformed durable checkpoints and pending input', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-invalid-state-'))
    const store = new StateStore(directory)
    await store.initialize()
    await writeFile(
      store.queuePath,
      JSON.stringify({
        schemaVersion: 1,
        events: [
          {
            id: 'message-1',
            receivedAt: '2026-07-31T00:00:00.000Z',
            message: { id: 'message-1' },
            dispatch: { target: '#general' },
          },
        ],
      }),
    )
    await expect(store.loadQueue()).rejects.toThrow('dispatch checkpoint')

    await writeFile(
      store.pendingInputPath,
      JSON.stringify({
        schemaVersion: 1,
        byReplyTarget: {
          '#general:message-1': [{ requestId: 'request-1', prompt: { secret: true }, options: [] }],
        },
      }),
    )
    await expect(store.loadPendingInput()).rejects.toThrow('pending Raft input')
  })
})
