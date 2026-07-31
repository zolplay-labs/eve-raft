import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { EveRaftService } from '../src/service.ts'
import { StateStore } from '../src/state.ts'
import { RAFT_ATTACHMENT_MAX_BYTES, RAFT_ATTACHMENTS_MAX_COUNT } from '../src/types.ts'
import { FakeEveServer } from './fake-eve-server.ts'
import { FakeRaftServer } from './fake-raft-server.ts'

const PNG_BYTES = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
  'base64',
)

describe('Raft attachments', () => {
  let raft: FakeRaftServer
  let eve: FakeEveServer
  let service: EveRaftService

  beforeEach(async () => {
    raft = new FakeRaftServer()
    eve = new FakeEveServer('channel-secret')
    await Promise.all([raft.start(), eve.start()])
    const directory = await mkdtemp(path.join(tmpdir(), 'eve-raft-attachment-'))
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

  function attachmentMessage(id: string): Record<string, unknown> {
    return {
      seq: 1,
      id: 'message-attachment',
      message_id: 'message-attachment',
      timestamp: '2026-07-31T00:00:00.000Z',
      sender_type: 'human',
      sender_name: 'cali',
      channel_type: 'dm',
      channel_name: 'Dex',
      content: 'review this',
      attachments: [{ id, filename: '../brief.png' }],
    }
  }

  it('verifies a supported file from bytes before delivering it to Eve', async () => {
    raft.attachments.set('attachment-1', {
      bytes: PNG_BYTES,
      mediaType: 'application/octet-stream',
    })
    raft.events.push(attachmentMessage('attachment-1'))

    await service.drain()
    await service.processNext()

    const input = eve.inputs[0]?.input as Array<Record<string, unknown>>
    expect(input).toEqual([
      { type: 'text', text: 'review this' },
      expect.objectContaining({ type: 'file', mediaType: 'image/png', filename: 'brief.png' }),
    ])
    expect(Buffer.isBuffer(input[1]?.data)).toBe(true)
  })

  it.each([
    ['application/pdf', 'brief.pdf', new TextEncoder().encode('%PDF-1.7\n1 0 obj\nendobj\n%%EOF')],
    ['image/jpeg', 'brief.jpg', Uint8Array.from([0xff, 0xd8, 0xff, 0xe0, 0, 0, 0xff, 0xd9])],
  ] as const)('verifies and transfers %s bytes', async (mediaType, filename, bytes) => {
    raft.attachments.set('attachment-supported', { bytes, mediaType: 'application/octet-stream' })
    raft.events.push({
      ...attachmentMessage('attachment-supported'),
      attachments: [{ id: 'attachment-supported', filename }],
    })

    await service.drain()
    await service.processNext()

    const input = eve.inputs[0]?.input as Array<Record<string, unknown>>
    expect(input[1]).toMatchObject({ type: 'file', mediaType, filename })
  })

  it('rejects disguised unsupported bytes and advances the durable queue', async () => {
    raft.attachments.set('attachment-2', {
      bytes: new TextEncoder().encode('not an image'),
      mediaType: 'image/png',
    })
    raft.events.push(attachmentMessage('attachment-2'))

    await service.drain()
    await service.processNext()

    expect(eve.inputs).toHaveLength(0)
    expect(raft.reactions).toContainEqual({
      messageId: 'message-attachment',
      emoji: '⚠️',
      operation: 'add',
    })
    expect(service.health.queueDepth).toBe(0)
  })

  it('rejects raw count and declared-size overflow before invoking Eve', async () => {
    raft.events.push({
      ...attachmentMessage('attachment-1'),
      attachments: Array.from({ length: RAFT_ATTACHMENTS_MAX_COUNT + 1 }, (_, index) => ({
        id: `attachment-${index}`,
        filename: `file-${index}.png`,
      })),
    })
    await service.drain()
    await service.processNext()

    raft.attachments.set('attachment-large', {
      bytes: PNG_BYTES,
      mediaType: 'image/png',
      declaredSize: RAFT_ATTACHMENT_MAX_BYTES + 1,
    })
    raft.events.push({ ...attachmentMessage('attachment-large'), id: 'message-large', message_id: 'message-large' })
    await service.drain()
    await service.processNext()

    expect(eve.inputs).toHaveLength(0)
    expect(service.health.queueDepth).toBe(0)
  })

  it('rejects a truncated authenticated attachment transfer and advances', async () => {
    raft.attachments.set('attachment-truncated', {
      bytes: PNG_BYTES,
      mediaType: 'image/png',
      declaredSize: 100,
      truncateTransfer: true,
    })
    raft.events.push(attachmentMessage('attachment-truncated'))

    await service.drain()
    await service.processNext()

    expect(eve.inputs).toHaveLength(0)
    expect(raft.reactions).toContainEqual({
      messageId: 'message-attachment',
      emoji: '⚠️',
      operation: 'add',
    })
    expect(service.health.queueDepth).toBe(0)
  })
})
