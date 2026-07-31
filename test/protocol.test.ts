import { describe, expect, it } from 'vitest'
import { deflateSync } from 'node:zlib'

import { attachmentMatchesMediaType, parseRaftEventEnvelope } from '../src/protocol.ts'
import {
  RAFT_ATTACHMENT_MAX_BYTES,
  RAFT_ATTACHMENTS_MAX_COUNT,
  RAFT_ATTACHMENTS_MAX_TOTAL_BYTES,
} from '../src/types.ts'

const JPEG_BYTES = Buffer.from(
  '/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAAF//8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABBQJ//8QAFBEBAAAAAAAAAAAAAAAAAAAAAP/aAAgBAwEBPwF//8QAFBEBAAAAAAAAAAAAAAAAAAAAAP/aAAgBAgEBPwF//8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQAGPwJ//8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABPyF//9oADAMBAAIAAwAAABD/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAEDAQE/EH//xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAECAQE/EH//xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAE/EH//2Q==',
  'base64',
)

let crcTable: Uint32Array | undefined

function pngCrc(bytes: Uint8Array): number {
  crcTable ??= Uint32Array.from({ length: 256 }, (_, value) => {
    let crc = value
    for (let bit = 0; bit < 8; bit += 1) crc = (crc & 1) !== 0 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1
    return crc >>> 0
  })
  let crc = 0xffffffff
  for (const byte of bytes) crc = crcTable[(crc ^ byte) & 0xff]! ^ (crc >>> 8)
  return (crc ^ 0xffffffff) >>> 0
}

function pngChunk(type: string, data: Uint8Array): Buffer {
  const name = Buffer.from(type, 'ascii')
  const length = Buffer.alloc(4)
  length.writeUInt32BE(data.byteLength)
  const crc = Buffer.alloc(4)
  crc.writeUInt32BE(pngCrc(Buffer.concat([name, data])))
  return Buffer.concat([length, name, data, crc])
}

function oversizedDecodedPng(): Uint8Array {
  const header = Buffer.alloc(13)
  header.writeUInt32BE(10_000, 0)
  header.writeUInt32BE(10_000, 4)
  header.set([8, 6, 0, 0, 0], 8)
  return Buffer.concat([
    Buffer.from('89504e470d0a1a0a', 'hex'),
    pngChunk('IHDR', header),
    pngChunk('IDAT', deflateSync(Uint8Array.from([0]))),
    pngChunk('IEND', new Uint8Array()),
  ])
}

function pdfBytes(): Uint8Array {
  const prefix = '%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n'
  const xrefOffset = Buffer.byteLength(prefix)
  return new TextEncoder().encode(
    `${prefix}xref\n0 2\n0000000000 65535 f \n0000000009 00000 n \ntrailer\n<< /Root 1 0 R /Size 2 >>\nstartxref\n${xrefOffset}\n%%EOF\n`,
  )
}

function envelope(attachments: Record<string, unknown>[] = [], overrides: Record<string, unknown> = {}) {
  return {
    protocolVersion: 1,
    serverId: 'server-1',
    agentId: 'agent-1',
    agentName: 'Dex',
    message: {
      messageId: 'message-1',
      createdAt: '2026-07-31T00:00:00.000Z',
      senderId: 'human-1',
      senderType: 'human',
      senderName: 'cali',
      senderDisplayName: 'Cali',
      channelType: 'dm',
      channelName: 'Dex',
      parentChannelType: null,
      parentChannelName: null,
      content: 'hello',
      target: 'dm:@Dex',
      replyTarget: 'dm:@Dex:message',
      taskChannel: null,
      taskStatus: null,
      taskNumber: null,
      taskAssigneeId: null,
      taskAssigneeType: null,
      attachments,
      ...overrides,
    },
  }
}

function attachment(id: number, sizeBytes = 1): Record<string, unknown> {
  return { id: `file-${id}`, fileName: `file-${id}.png`, mediaType: 'image/png', sizeBytes, data: 'AA==' }
}

describe('Raft protocol boundaries', () => {
  it('accepts exact attachment limits and rejects count, file, and aggregate overflow', () => {
    expect(
      parseRaftEventEnvelope(
        envelope(Array.from({ length: RAFT_ATTACHMENTS_MAX_COUNT }, (_, index) => attachment(index))),
      ),
    ).not.toBeNull()
    expect(
      parseRaftEventEnvelope(
        envelope(Array.from({ length: RAFT_ATTACHMENTS_MAX_COUNT + 1 }, (_, index) => attachment(index))),
      ),
    ).toBeNull()
    expect(parseRaftEventEnvelope(envelope([attachment(1, RAFT_ATTACHMENT_MAX_BYTES + 1)]))).toBeNull()
    expect(
      parseRaftEventEnvelope(
        envelope([
          attachment(1, RAFT_ATTACHMENT_MAX_BYTES),
          attachment(2, RAFT_ATTACHMENTS_MAX_TOTAL_BYTES - RAFT_ATTACHMENT_MAX_BYTES + 1),
        ]),
      ),
    ).toBeNull()
  })

  it('rejects malformed input responses instead of turning them into ordinary messages', () => {
    expect(
      parseRaftEventEnvelope(envelope([], { inputResponses: [{ requestId: 'request-1', optionId: '' }] })),
    ).toBeNull()
    expect(parseRaftEventEnvelope(envelope([], { inputResponses: '2' }))).toBeNull()
  })

  it('detects supported media from bytes', () => {
    expect(attachmentMatchesMediaType(pdfBytes(), 'application/pdf')).toBe(true)
    expect(attachmentMatchesMediaType(JPEG_BYTES, 'image/jpeg')).toBe(true)
    expect(
      attachmentMatchesMediaType(
        Buffer.from(
          'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
          'base64',
        ),
        'image/png',
      ),
    ).toBe(true)
    expect(attachmentMatchesMediaType(new TextEncoder().encode('%PDF-1.7'), 'application/pdf')).toBe(false)
    expect(attachmentMatchesMediaType(Uint8Array.from([0xff, 0xd8, 0xff]), 'image/jpeg')).toBe(false)
    expect(attachmentMatchesMediaType(Uint8Array.from([0xff, 0xd8, 0xff, 0xe0, 0, 0, 0xff, 0xd9]), 'image/jpeg')).toBe(
      false,
    )
    expect(
      attachmentMatchesMediaType(Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), 'image/png'),
    ).toBe(false)
    expect(attachmentMatchesMediaType(new TextEncoder().encode('not a pdf'), 'application/pdf')).toBe(false)
    expect(attachmentMatchesMediaType(oversizedDecodedPng(), 'image/png')).toBe(false)
    const pngWithoutImageData = Buffer.concat([
      Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwC', 'base64'),
      Buffer.from('AAAAAElFTkSuQmCC', 'base64'),
    ])
    expect(attachmentMatchesMediaType(pngWithoutImageData, 'image/png')).toBe(false)
  })
})
