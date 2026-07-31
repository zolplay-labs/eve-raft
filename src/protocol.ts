import type { UserContent } from 'ai'

import {
  RAFT_ATTACHMENT_MAX_BYTES,
  RAFT_ATTACHMENT_MEDIA_TYPES,
  RAFT_ATTACHMENTS_MAX_COUNT,
  RAFT_ATTACHMENTS_MAX_TOTAL_BYTES,
  RAFT_CHANNEL_PROTOCOL_VERSION,
  type EveAuthContext,
  type RaftAttachment,
  type RaftAttachmentMediaType,
  type RaftEventEnvelope,
  type RaftMessage,
  type RaftPrincipalContext,
  type RaftSenderType,
  type RaftSurface,
} from './types.js'

const IDENTIFIER = /^[A-Za-z0-9_-]{1,160}$/u
const MAX_CONTENT_CHARS = 100_000
const MAX_TARGET_CHARS = 500

function boundedString(value: unknown, max: number, allowEmpty = false): string | null {
  return typeof value === 'string' && value.length <= max && (allowEmpty || value.length > 0) ? value : null
}

function nullableString(value: unknown, max: number): string | null | undefined {
  return value === undefined || value === null ? null : (boundedString(value, max) ?? undefined)
}

function senderType(value: unknown): RaftSenderType | null {
  return value === 'human' || value === 'agent' || value === 'system' || value === 'third_party_app' ? value : null
}

export function isRaftAttachmentMediaType(value: unknown): value is RaftAttachmentMediaType {
  return typeof value === 'string' && (RAFT_ATTACHMENT_MEDIA_TYPES as readonly string[]).includes(value)
}

export function attachmentMatchesMediaType(bytes: Uint8Array, mediaType: RaftAttachmentMediaType): boolean {
  const buffer = Buffer.from(bytes)
  if (mediaType === 'application/pdf') {
    const header = buffer.subarray(0, Math.min(buffer.byteLength, 1024)).indexOf('%PDF-')
    const trailer = buffer.subarray(Math.max(0, buffer.byteLength - 1024)).indexOf('%%EOF')
    return buffer.byteLength >= 14 && header >= 0 && trailer >= 0
  }
  if (mediaType === 'image/jpeg') {
    return (
      buffer.byteLength >= 8 &&
      buffer[0] === 0xff &&
      buffer[1] === 0xd8 &&
      buffer[2] === 0xff &&
      buffer.at(-2) === 0xff &&
      buffer.at(-1) === 0xd9
    )
  }
  const signature = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]
  const ihdr = [0x49, 0x48, 0x44, 0x52]
  const iend = [0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82]
  return (
    buffer.byteLength >= 45 &&
    signature.every((value, index) => buffer[index] === value) &&
    ihdr.every((value, index) => buffer[index + 12] === value) &&
    iend.every((value, index) => buffer[buffer.byteLength - iend.length + index] === value)
  )
}

function parseAttachment(value: unknown): RaftAttachment | null {
  if (!value || typeof value !== 'object') return null
  const input = value as Record<string, unknown>
  if (
    typeof input.id !== 'string' ||
    !IDENTIFIER.test(input.id) ||
    typeof input.fileName !== 'string' ||
    input.fileName.length === 0 ||
    input.fileName.length > 255 ||
    !isRaftAttachmentMediaType(input.mediaType) ||
    !Number.isSafeInteger(input.sizeBytes) ||
    (input.sizeBytes as number) <= 0 ||
    (input.sizeBytes as number) > RAFT_ATTACHMENT_MAX_BYTES ||
    typeof input.data !== 'string'
  ) {
    return null
  }
  return {
    id: input.id,
    fileName: input.fileName,
    mediaType: input.mediaType,
    sizeBytes: input.sizeBytes as number,
    data: input.data,
  }
}

function parseMessage(value: unknown): RaftMessage | null {
  if (!value || typeof value !== 'object') return null
  const input = value as Record<string, unknown>
  const messageId = typeof input.messageId === 'string' && IDENTIFIER.test(input.messageId) ? input.messageId : null
  const parsedSenderType = senderType(input.senderType)
  const createdAt =
    typeof input.createdAt === 'string' && Number.isFinite(Date.parse(input.createdAt)) ? input.createdAt : null
  if (!messageId || !parsedSenderType || !createdAt || !Array.isArray(input.attachments)) return null

  const attachments = input.attachments.map(parseAttachment)
  if (
    attachments.length > RAFT_ATTACHMENTS_MAX_COUNT ||
    attachments.some((attachment) => attachment === null) ||
    attachments.reduce((sum, attachment) => sum + (attachment?.sizeBytes ?? 0), 0) > RAFT_ATTACHMENTS_MAX_TOTAL_BYTES
  ) {
    return null
  }

  const senderId = boundedString(input.senderId, 160)
  const senderName = boundedString(input.senderName, 160)
  const senderDisplayName = nullableString(input.senderDisplayName, 160)
  const channelType = boundedString(input.channelType, 100)
  const channelName = boundedString(input.channelName, 160)
  const parentChannelType = nullableString(input.parentChannelType, 100)
  const parentChannelName = nullableString(input.parentChannelName, 160)
  const content = boundedString(input.content, MAX_CONTENT_CHARS, true)
  const target = boundedString(input.target, MAX_TARGET_CHARS)
  const replyTarget = boundedString(input.replyTarget, MAX_TARGET_CHARS)
  const taskChannel = nullableString(input.taskChannel, MAX_TARGET_CHARS)
  const taskStatus = nullableString(input.taskStatus, 100)
  const taskAssigneeId = nullableString(input.taskAssigneeId, 160)
  const taskAssigneeType = nullableString(input.taskAssigneeType, 100)
  const taskNumber =
    input.taskNumber === null || input.taskNumber === undefined
      ? null
      : Number.isSafeInteger(input.taskNumber) && (input.taskNumber as number) > 0
        ? (input.taskNumber as number)
        : undefined
  if (
    !senderId ||
    !senderName ||
    senderDisplayName === undefined ||
    !channelType ||
    !channelName ||
    parentChannelType === undefined ||
    parentChannelName === undefined ||
    content === null ||
    !target ||
    !replyTarget ||
    taskChannel === undefined ||
    taskStatus === undefined ||
    taskAssigneeId === undefined ||
    taskAssigneeType === undefined ||
    taskNumber === undefined
  ) {
    return null
  }

  const inputResponses = Array.isArray(input.inputResponses)
    ? input.inputResponses.map((value) => {
        if (!value || typeof value !== 'object') return null
        const response = value as Record<string, unknown>
        const requestId = boundedString(response.requestId, 200)
        const optionId = nullableString(response.optionId, 200)
        const text = nullableString(response.text, 4_000)
        if (!requestId || optionId === undefined || text === undefined || (!optionId && !text)) return null
        return { requestId, ...(optionId ? { optionId } : {}), ...(text ? { text } : {}) }
      })
    : undefined
  if (
    (input.inputResponses !== undefined && !Array.isArray(input.inputResponses)) ||
    inputResponses?.some((response) => response === null)
  ) {
    return null
  }

  return {
    ...(Number.isSafeInteger(input.seq) && (input.seq as number) >= 0 ? { seq: input.seq as number } : {}),
    messageId,
    createdAt,
    senderId,
    senderType: parsedSenderType,
    senderName,
    senderDisplayName,
    channelType,
    channelName,
    parentChannelType,
    parentChannelName,
    content,
    target,
    replyTarget,
    taskChannel,
    taskStatus,
    taskNumber,
    taskAssigneeId,
    taskAssigneeType,
    attachments: attachments as RaftAttachment[],
    ...(inputResponses && inputResponses.length > 0
      ? { inputResponses: inputResponses as NonNullable<RaftMessage['inputResponses']> }
      : {}),
  }
}

export function parseRaftEventEnvelope(value: unknown): RaftEventEnvelope | null {
  if (!value || typeof value !== 'object') return null
  const input = value as Record<string, unknown>
  if (
    input.protocolVersion !== RAFT_CHANNEL_PROTOCOL_VERSION ||
    typeof input.serverId !== 'string' ||
    !IDENTIFIER.test(input.serverId) ||
    typeof input.agentId !== 'string' ||
    !IDENTIFIER.test(input.agentId) ||
    typeof input.agentName !== 'string' ||
    input.agentName.length === 0 ||
    input.agentName.length > 160
  ) {
    return null
  }
  const message = parseMessage(input.message)
  return message
    ? {
        protocolVersion: RAFT_CHANNEL_PROTOCOL_VERSION,
        serverId: input.serverId,
        agentId: input.agentId,
        agentName: input.agentName,
        message,
      }
    : null
}

export function raftSurface(message: RaftMessage): RaftSurface {
  return message.channelType === 'dm' || message.parentChannelType === 'dm' ? 'direct' : 'shared'
}

export function stripAgentMention(content: string, agentName: string): { mentioned: boolean; content: string } {
  const escaped = agentName.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
  const mention = new RegExp(`(^|\\s)@${escaped}(?=\\s|[,:;.!?]|$)`, 'iu')
  if (!mention.test(content)) return { mentioned: false, content }
  return { mentioned: true, content: content.replace(mention, '$1').trim() }
}

export function taskFor(message: RaftMessage): { channel: string; number: number } | null {
  return message.taskChannel && message.taskNumber ? { channel: message.taskChannel, number: message.taskNumber } : null
}

export function defaultRaftAuth(principal: RaftPrincipalContext): EveAuthContext {
  return {
    authenticator: 'raft',
    issuer: principal.serverId,
    principalId: principal.principalId,
    principalType: 'external',
    attributes: {
      raft_server_id: principal.serverId,
      raft_agent_id: principal.agentId,
      raft_actor_id: principal.actorId,
      raft_actor_type: principal.actorType,
      raft_handle: principal.handle,
      raft_surface: principal.surface,
      raft_target: principal.replyTarget,
    },
  }
}

export function raftUserContent(message: RaftMessage, text: string): string | UserContent {
  if (message.attachments.length === 0) return text
  const files: UserContent = []
  let total = 0
  for (const attachment of message.attachments) {
    const bytes = Buffer.from(attachment.data, 'base64')
    if (bytes.byteLength !== attachment.sizeBytes) throw new Error('Raft attachment size does not match its bytes')
    if (!attachmentMatchesMediaType(bytes, attachment.mediaType)) {
      throw new Error('Raft attachment bytes do not match their declared type')
    }
    total += bytes.byteLength
    if (total > RAFT_ATTACHMENTS_MAX_TOTAL_BYTES) throw new Error('Raft attachments exceed the aggregate limit')
    files.push({ type: 'file', data: bytes, mediaType: attachment.mediaType, filename: attachment.fileName })
  }
  return [{ type: 'text', text: text || 'Please review the attached files.' }, ...files]
}

export function serializeEventStream<T>(stream: ReadableStream<T>): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return stream.pipeThrough(
    new TransformStream<T, Uint8Array>({
      transform(event, controller) {
        controller.enqueue(encoder.encode(`${JSON.stringify(event)}\n`))
      },
    }),
  )
}
