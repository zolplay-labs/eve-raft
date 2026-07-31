import { describe, expect, it } from 'vitest'

import { attachmentMatchesMediaType, parseRaftEventEnvelope } from '../src/protocol.ts'
import {
  RAFT_ATTACHMENT_MAX_BYTES,
  RAFT_ATTACHMENTS_MAX_COUNT,
  RAFT_ATTACHMENTS_MAX_TOTAL_BYTES,
} from '../src/types.ts'

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
    expect(attachmentMatchesMediaType(new TextEncoder().encode('%PDF-1.7\n%%EOF'), 'application/pdf')).toBe(true)
    expect(attachmentMatchesMediaType(Uint8Array.from([0xff, 0xd8, 0xff, 0xe0, 0, 0, 0xff, 0xd9]), 'image/jpeg')).toBe(
      true,
    )
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
    expect(
      attachmentMatchesMediaType(Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), 'image/png'),
    ).toBe(false)
    expect(attachmentMatchesMediaType(new TextEncoder().encode('not a pdf'), 'application/pdf')).toBe(false)
  })
})
