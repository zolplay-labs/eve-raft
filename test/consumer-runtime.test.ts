import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RaftClient } from '../src/raft-client.ts'
import { EveRaftService, type EveRaftConnectionSource, type EveRaftTransport } from '../src/service.ts'
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

describe('consumer-owned Eve Raft runtime', () => {
  let raft: FakeRaftServer

  beforeEach(async () => {
    raft = new FakeRaftServer()
    await raft.start()
  })

  afterEach(() => raft.stop())

  it('uses consumer transports without storing credentials or attachment bytes', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-'))
    const credential = {
      schemaVersion: 1 as const,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: raft.apiKey,
      createdAt: '2026-08-01T00:00:00.000Z',
    }
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

  it('reloads a connection supplied after the runtime starts', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-reload-'))
    const credential = {
      schemaVersion: 1 as const,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: raft.apiKey,
      createdAt: '2026-08-01T00:00:00.000Z',
    }
    let connection: Awaited<ReturnType<EveRaftConnectionSource['load']>> = null
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => connection),
    }
    const eve: EveRaftTransport = {
      dispatch: vi.fn(),
      stream: vi.fn(),
    }
    const service = new EveRaftService({ stateDirectory, connectionSource, eve })
    await service.initialize()
    expect(service.health.state).toBe('unconfigured')

    connection = { identity: credential, client: new RaftClient(credential) }

    await expect(service.reloadConnection()).resolves.toBe(true)
    expect(service.health).toMatchObject({ state: 'connected', serverUrl: raft.origin, lastError: null })
    expect(connectionSource.load).toHaveBeenCalledTimes(2)
  })

  it('waits for an in-flight delivery before reloading the connection', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-serialized-reload-'))
    const credential = {
      schemaVersion: 1 as const,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: raft.apiKey,
      createdAt: '2026-08-01T00:00:00.000Z',
    }
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
    const service = new EveRaftService({ stateDirectory, connectionSource, eve })
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

  it('rejects a consumer connection whose stable Raft identity does not match', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-identity-rejected-'))
    const credential = {
      schemaVersion: 1 as const,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: raft.apiKey,
      createdAt: '2026-08-01T00:00:00.000Z',
    }
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
    })

    await service.initialize()

    expect(rejected).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'Stored Raft credential does not match the configured agent and server' }),
    )
    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'credential_rejected' })
  })

  it('notifies the consumer when Raft rejects its active connection', async () => {
    const stateDirectory = await mkdtemp(path.join(tmpdir(), 'eve-raft-consumer-rejected-'))
    const credential = {
      schemaVersion: 1 as const,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: raft.apiKey,
      createdAt: '2026-08-01T00:00:00.000Z',
    }
    const rejected = vi.fn()
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
      rejected,
    }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve: { dispatch: vi.fn(), stream: vi.fn() },
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
    const credential = {
      schemaVersion: 1 as const,
      serverUrl: raft.origin,
      agentId: raft.agentId,
      agentName: raft.agentName,
      serverId: raft.serverId,
      credentialId: 'credential-1',
      scopes: ['agent'],
      apiKey: 'rejected-key',
      createdAt: '2026-08-01T00:00:00.000Z',
    }
    const rejected = vi.fn()
    const connectionSource: EveRaftConnectionSource = {
      load: vi.fn(async () => ({ identity: credential, client: new RaftClient(credential) })),
      rejected,
    }
    const service = new EveRaftService({
      stateDirectory,
      connectionSource,
      eve: { dispatch: vi.fn(), stream: vi.fn() },
    })

    await service.initialize()

    expect(rejected).toHaveBeenCalledOnce()
    expect(rejected).toHaveBeenCalledWith(expect.objectContaining({ status: 401 }))
    expect(service.health).toMatchObject({ state: 'disconnected', lastError: 'credential_rejected' })
  })
})
