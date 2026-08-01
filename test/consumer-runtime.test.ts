import { mkdtemp, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RaftClient } from '../src/raft-client.ts'
import {
  EveRaftService,
  prepareInlineAttachment,
  type EveRaftConnectionSource,
  type EveRaftTransport,
} from '../src/service.ts'
import type { RaftCredential } from '../src/state.ts'
import type { RaftAttachmentMediaType, RaftEventEnvelope } from '../src/types.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

const PNG_BYTES = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
  'base64',
)

interface StoredAttachment {
  id: string
  fileName: string
  mediaType: RaftAttachmentMediaType
  sizeBytes: number
  storageKey: string
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10))
  if (!predicate()) throw new Error('Timed out waiting for consumer runtime state')
}

function credentialFor(server: FakeRaftServer, overrides: Partial<RaftCredential> = {}): RaftCredential {
  return {
    schemaVersion: 1,
    serverUrl: server.origin,
    agentId: server.agentId,
    agentName: server.agentName,
    serverId: server.serverId,
    credentialId: 'credential-1',
    scopes: ['agent'],
    apiKey: server.apiKey,
    createdAt: '2026-08-01T00:00:00.000Z',
    ...overrides,
  }
}

describe('consumer-owned Eve Raft runtime', () => {
  let raft: FakeRaftServer

  beforeEach(async () => {
    raft = new FakeRaftServer()
    await raft.start()
  })

  afterEach(() => raft.stop())

  it('requires an attachment preparer for a consumer transport', () => {
    expect(
      () =>
        new EveRaftService({
          stateDirectory: '/tmp/eve-raft-consumer-preparer',
          eve: { dispatch: vi.fn(), stream: vi.fn() },
        }),
    ).toThrow('prepareAttachment is required when a consumer Eve transport is provided')
  })

  it('uses consumer transports without storing credentials or attachment bytes', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-'))
    const credential = credentialFor(raft)
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
    }
    const envelopes: RaftEventEnvelope<StoredAttachment>[] = []
    const eve: EveRaftTransport<StoredAttachment> = {
      async dispatch(envelope) {
        envelopes.push(envelope)
        return { accepted: false, reason: 'ignored' }
      },
      stream: vi.fn(),
    }
    const prepareAttachment = vi.fn(async (input) => ({
      id: input.id,
      fileName: input.fileName,
      mediaType: input.mediaType,
      sizeBytes: input.bytes.byteLength,
      storageKey: `private/${input.messageId}/${input.id}`,
    }))
    raft.attachments.set('attachment-1', { bytes: PNG_BYTES, mediaType: 'application/octet-stream' })
    raft.events.push({
      seq: 1,
      id: 'message-1',
      message_id: 'message-1',
      timestamp: '2026-08-01T00:00:00.000Z',
      sender_type: 'human',
      sender_name: 'cali',
      channel_type: 'dm',
      channel_name: 'Dex',
      content: 'review this',
      attachments: [{ id: 'attachment-1', filename: 'brief.png' }],
    })

    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve,
      prepareAttachment,
    })
    await service.initialize()
    await service.drain()
    await service.processNext()

    expect(connectionSource.load).toHaveBeenCalledOnce()
    expect(prepareAttachment).toHaveBeenCalledWith(
      expect.objectContaining({ messageId: 'message-1', id: 'attachment-1', bytes: expect.any(Uint8Array) }),
    )
    expect(Buffer.from(prepareAttachment.mock.calls[0]![0].bytes)).toEqual(PNG_BYTES)
    expect(envelopes[0]?.message.attachments).toEqual([
      {
        id: 'attachment-1',
        fileName: 'brief.png',
        mediaType: 'image/png',
        sizeBytes: PNG_BYTES.byteLength,
        storageKey: 'private/message-1/attachment-1',
      },
    ])
    expect(JSON.stringify(envelopes)).not.toContain(PNG_BYTES.toString('base64'))
  })

  it('delivers consumer-owned immediate responses with a stable Raft send key', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-immediate-'))
    const credential = credentialFor(raft)
    raft.events.push({
      seq: 4,
      id: 'link-message',
      message_id: 'link-message',
      timestamp: '2026-08-01T00:00:00.000Z',
      sender_type: 'human',
      sender_name: 'cali',
      channel_type: 'dm',
      channel_name: 'Dex',
      content: 'link ABCD-EFGH',
    })
    const service = new EveRaftService({
      stateDirectory,
      connectionSource: {
        load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
      },
      eve: {
        dispatch: vi.fn(
          async () =>
            ({
              accepted: true,
              kind: 'immediate',
              target: 'dm:@cali:link-mes',
              messageId: 'link-message',
              content: 'Raft is connected to Cali.',
              task: null,
            }) as const,
        ),
        stream: vi.fn(),
      },
      deliveryKey: (input) => `dex-${input.kind}-${input.sourceMessageId}`,
      prepareAttachment: prepareInlineAttachment,
    })

    await service.initialize()
    await service.drain()
    await service.processNext()

    expect(raft.sent).toEqual([
      expect.objectContaining({
        target: 'dm:@cali:link-mes',
        content: 'Raft is connected to Cali.',
        idempotencyKey: 'dex-immediate-link-message',
        seenUpToSeq: 4,
      }),
    ])
  })

  it('reloads a connection supplied after the runtime starts', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-reload-'))
    const credential = credentialFor(raft)
    let connection: Awaited<ReturnType<EveRaftConnectionSource['load']>> = null
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => connection),
    }
    const eve: EveRaftTransport = {
      dispatch: vi.fn(),
      stream: vi.fn(),
    }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve,
      prepareAttachment: prepareInlineAttachment,
    })
    await service.initialize()
    expect(service.health.state).toBe('unconfigured')

    connection = { identity: credential, client: new RaftClient(credential) }

    await expect(service.reloadConnection()).resolves.toBe(true)
    expect(service.health).toMatchObject({ state: 'connected', serverUrl: raft.origin, lastError: null })
    expect(connectionSource.load).toHaveBeenCalledTimes(2)
  })

  it('fails closed on unbound legacy work unless the consumer explicitly adopts it', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-legacy-state-'))
    await writeFile(
      path.join(stateDirectory, 'queue.json'),
      JSON.stringify({
        schemaVersion: 1,
        events: [
          {
            id: 'legacy-message',
            receivedAt: '2026-08-01T00:00:00.000Z',
            message: { id: 'legacy-message' },
          },
        ],
      }),
    )
    const credential = credentialFor(raft)
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
    }
    const eve: EveRaftTransport = { dispatch: vi.fn(), stream: vi.fn() }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve,
      prepareAttachment: prepareInlineAttachment,
    })

    await service.initialize()

    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'legacy_state_identity_unbound' })
    await expect(service.reloadConnection()).resolves.toBe(false)

    const adopted = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve,
      prepareAttachment: prepareInlineAttachment,
      adoptLegacyState: true,
    })
    await adopted.initialize()
    expect(adopted.health).toMatchObject({ state: 'connected', lastError: null })
  })

  it('waits for an in-flight delivery before reloading the connection', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-serialized-reload-'))
    const credential = credentialFor(raft)
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
    }
    let releaseDispatch: () => void = () => undefined
    const dispatchGate = new Promise<void>((resolve) => {
      releaseDispatch = resolve
    })
    let markDispatchStarted: () => void = () => undefined
    const dispatchStarted = new Promise<void>((resolve) => {
      markDispatchStarted = resolve
    })
    const eve: EveRaftTransport = {
      async dispatch() {
        markDispatchStarted()
        await dispatchGate
        return { accepted: false, reason: 'ignored' }
      },
      stream: vi.fn(),
    }
    raft.events.push({
      seq: 1,
      id: 'message-1',
      message_id: 'message-1',
      timestamp: '2026-08-01T00:00:00.000Z',
      sender_type: 'human',
      sender_name: 'cali',
      channel_type: 'dm',
      channel_name: 'Dex',
      content: 'hello',
    })
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve,
      prepareAttachment: prepareInlineAttachment,
    })
    await service.initialize()
    await service.drain()

    const processing = service.processNext()
    await dispatchStarted
    let reloadResolved = false
    const reloading = service.reloadConnection().then((connected) => {
      reloadResolved = true
      return connected
    })
    await Promise.resolve()

    expect(reloadResolved).toBe(false)
    releaseDispatch()
    await expect(processing).resolves.toBe(true)
    await expect(reloading).resolves.toBe(true)
    expect(connectionSource.load).toHaveBeenCalledTimes(2)
  })

  it('refuses to mix pending delivery state with a different stable Raft identity', async () => {
    const otherRaft = new FakeRaftServer('Other', 'agent-2', 'server-2')
    await otherRaft.start()
    try {
      const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-identity-conflict-'))
      let activeRaft = raft
      const connectionSource: EveRaftConnectionSource = {
        load: vi.fn(async () => {
          const credential = credentialFor(activeRaft)
          return { identity: credential, client: new RaftClient(credential) }
        }),
      }
      const dispatch = vi.fn(async (_envelope: RaftEventEnvelope) => ({ accepted: false, reason: 'ignored' }) as const)
      raft.events.push({
        id: 'message-1',
        message_id: 'message-1',
        timestamp: '2026-08-01T00:00:00.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'dm',
        channel_name: 'Dex',
        content: 'hello',
      })
      const service = new EveRaftService({
        stateDirectory,
        connectionSource,
        eve: { dispatch, stream: vi.fn() },
        prepareAttachment: prepareInlineAttachment,
      })
      await service.initialize()
      await service.drain()

      activeRaft = otherRaft
      await expect(service.reloadConnection()).resolves.toBe(false)
      expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'connection_identity_conflict' })
      expect(dispatch).not.toHaveBeenCalled()

      activeRaft = raft
      await expect(service.reloadConnection()).resolves.toBe(true)
      await expect(service.processNext()).resolves.toBe(true)
      expect(dispatch).toHaveBeenCalledWith(expect.objectContaining({ serverId: raft.serverId }))
    } finally {
      await otherRaft.stop()
    }
  })

  it('clears the completed-event ledger when switching an idle runtime to a new identity', async () => {
    const otherRaft = new FakeRaftServer('Other', 'agent-2', 'server-2')
    await otherRaft.start()
    try {
      const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-idle-rebind-'))
      let activeRaft = raft
      const connectionSource: EveRaftConnectionSource = {
        load: vi.fn(async () => {
          const credential = credentialFor(activeRaft)
          return { identity: credential, client: new RaftClient(credential) }
        }),
      }
      const dispatch = vi.fn(async (_envelope: RaftEventEnvelope) => ({ accepted: false, reason: 'ignored' }) as const)
      const event = {
        id: 'shared-message-id',
        message_id: 'shared-message-id',
        timestamp: '2026-08-01T00:00:00.000Z',
        sender_type: 'human',
        sender_name: 'cali',
        channel_type: 'dm',
        channel_name: 'Dex',
        content: 'hello',
      }
      raft.events.push(event)
      const service = new EveRaftService({
        stateDirectory,
        connectionSource,
        eve: { dispatch, stream: vi.fn() },
        prepareAttachment: prepareInlineAttachment,
      })
      await service.initialize()
      await service.drain()
      await service.processNext()

      activeRaft = otherRaft
      otherRaft.events.push(event)
      await expect(service.reloadConnection()).resolves.toBe(true)
      await expect(service.drain()).resolves.toBe(1)
      await expect(service.processNext()).resolves.toBe(true)

      expect(dispatch).toHaveBeenCalledTimes(2)
      expect(dispatch.mock.calls[1]?.[0]).toEqual(expect.objectContaining({ serverId: otherRaft.serverId }))
    } finally {
      await otherRaft.stop()
    }
  })

  it('rejects a consumer connection whose stable Raft identity does not match', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-identity-rejected-'))
    const credential = credentialFor(raft)
    const rejected = vi.fn()
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({
        identity: { ...credential, agentId: 'different-agent' },
        client: new RaftClient(credential),
      })),
      rejected,
    }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve: { dispatch: vi.fn(), stream: vi.fn() },
      prepareAttachment: prepareInlineAttachment,
    })

    await service.initialize()

    await waitFor(() => rejected.mock.calls.length === 1)
    expect(rejected).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'Stored Raft credential does not match the configured agent and server' }),
    )
    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'credential_rejected' })
  })

  it('notifies the consumer when Raft rejects its active connection', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-rejected-'))
    const credential = credentialFor(raft)
    const rejected = vi.fn()
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
      rejected,
    }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve: { dispatch: vi.fn(), stream: vi.fn() },
      prepareAttachment: prepareInlineAttachment,
    })
    await service.initialize()
    raft.apiKey = 'replacement-key'
    raft.events.push({ id: 'message-rejected' })
    const controller = new AbortController()

    const running = service.run(controller.signal)
    await waitFor(() => rejected.mock.calls.length === 1)
    controller.abort()
    await running

    expect(rejected).toHaveBeenCalledWith(expect.objectContaining({ status: 401 }))
    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'credential_rejected' })
  })

  it('notifies the consumer when Raft rejects its connection during startup', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-startup-rejected-'))
    const credential = credentialFor(raft, { apiKey: 'rejected-key' })
    const rejected = vi.fn()
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
      rejected,
    }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve: { dispatch: vi.fn(), stream: vi.fn() },
      prepareAttachment: prepareInlineAttachment,
    })

    await service.initialize()

    await waitFor(() => rejected.mock.calls.length === 1)
    expect(rejected).toHaveBeenCalledOnce()
    expect(rejected).toHaveBeenCalledWith(expect.objectContaining({ status: 401 }))
    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'credential_rejected' })
  })

  it('stays disconnected when the consumer rejection callback fails', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-rejection-callback-'))
    const credential = credentialFor(raft, { apiKey: 'rejected-key' })
    const rejected = vi.fn(async () => {
      throw new Error('consumer persistence failed')
    })
    const service = new EveRaftService({
      stateDirectory,
      connectionSource: {
        load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
        rejected,
      },
      eve: { dispatch: vi.fn(), stream: vi.fn() },
      prepareAttachment: prepareInlineAttachment,
    })

    await expect(service.initialize()).resolves.toBeUndefined()

    await waitFor(() => rejected.mock.calls.length === 1)
    expect(rejected).toHaveBeenCalledOnce()
    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'credential_rejected' })
  })

  it('lets a rejection callback reload a rotated credential without deadlocking', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-rejection-reload-'))
    let credential = credentialFor(raft, { apiKey: 'rejected-key' })
    let service: EveRaftService
    let reloaded = false
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
      rejected: vi.fn(async () => {
        credential = credentialFor(raft)
        reloaded = await service.reloadConnection()
      }),
    }
    service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve: { dispatch: vi.fn(), stream: vi.fn() },
      prepareAttachment: prepareInlineAttachment,
    })

    await service.initialize()
    await waitFor(() => reloaded)

    expect(service.health).toMatchObject({ state: 'connected', lastError: null })
    expect(connectionSource.load).toHaveBeenCalledTimes(2)
  })
})
