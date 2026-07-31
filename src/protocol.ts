import type { UserContent } from 'ai'
import { inflateSync } from 'node:zlib'

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
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u
const MAX_CONTENT_CHARS = 100_000
const MAX_TARGET_CHARS = 500
const MAX_PNG_INFLATED_BYTES = RAFT_ATTACHMENTS_MAX_TOTAL_BYTES * 2

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

function isPdf(bytes: Uint8Array): boolean {
  const buffer = Buffer.from(bytes)
  if (buffer.byteLength < 32) return false
  const headerSearch = buffer.subarray(0, Math.min(buffer.byteLength, 1_024)).toString('latin1')
  const header = headerSearch.match(/%PDF-(?:1\.[0-7]|2\.0)/u)
  if (!header || header.index === undefined) return false
  const tail = buffer.subarray(Math.max(0, buffer.byteLength - 2_048)).toString('latin1')
  const eof = /%%EOF[\x00\x09\x0A\x0C\x0D\x20]*$/u.exec(tail)
  if (!eof) return false
  const body = buffer.toString('latin1', header.index, buffer.byteLength - tail.length + (eof.index ?? 0))
  const objects = body.match(/(?:^|[\r\n])\s*\d+\s+\d+\s+obj(?:\s|$)/gu)?.length ?? 0
  const ends = body.match(/(?:^|[\r\n])\s*endobj(?:\s|$)/gu)?.length ?? 0
  if (objects === 0 || objects !== ends) return false
  const startXref = /startxref\s+(\d+)\s*$/u.exec(body)
  if (!startXref) return false
  const xrefOffset = Number(startXref[1])
  if (!Number.isSafeInteger(xrefOffset) || xrefOffset < header.index || xrefOffset >= buffer.byteLength) return false
  const xrefTarget = buffer.subarray(xrefOffset, Math.min(buffer.byteLength, xrefOffset + 32)).toString('latin1')
  return /^xref(?:\s|$)/u.test(xrefTarget) || /^\d+\s+\d+\s+obj(?:\s|$)/u.test(xrefTarget)
}

function jpegMarkerHasLength(marker: number): boolean {
  return marker !== 0x01 && marker !== 0xd8 && marker !== 0xd9 && (marker < 0xd0 || marker > 0xd7)
}

function isJpeg(bytes: Uint8Array): boolean {
  if (bytes.length < 16 || bytes[0] !== 0xff || bytes[1] !== 0xd8) return false
  let offset = 2
  let sawFrame = false
  let sawScan = false
  let sawQuantizationTable = false
  let sawHuffmanTable = false
  while (offset < bytes.length) {
    if (bytes[offset] !== 0xff) return false
    while (offset < bytes.length && bytes[offset] === 0xff) offset += 1
    if (offset >= bytes.length) return false
    const marker = bytes[offset++]!
    if (marker === 0x00) return false
    if (marker === 0xd9) {
      return offset === bytes.length && sawFrame && sawScan && sawQuantizationTable && sawHuffmanTable
    }
    if (!jpegMarkerHasLength(marker)) continue
    if (offset + 2 > bytes.length) return false
    const length = (bytes[offset]! << 8) | bytes[offset + 1]!
    if (length < 2 || offset + length > bytes.length) return false
    const payloadOffset = offset + 2
    const payloadLength = length - 2
    if (marker === 0xdb) {
      if (payloadLength < 65) return false
      sawQuantizationTable = true
    }
    if (marker === 0xc4) {
      if (payloadLength < 18) return false
      sawHuffmanTable = true
    }
    if ((marker >= 0xc0 && marker <= 0xc3) || (marker >= 0xc5 && marker <= 0xc7) || marker === 0xc9) {
      if (
        payloadLength < 6 ||
        (bytes[payloadOffset + 1] === 0 && bytes[payloadOffset + 2] === 0) ||
        (bytes[payloadOffset + 3] === 0 && bytes[payloadOffset + 4] === 0)
      ) {
        return false
      }
      sawFrame = true
    }
    offset += length
    if (marker !== 0xda) continue
    if (!sawFrame || payloadLength < 6) return false
    sawScan = true
    while (offset < bytes.length) {
      if (bytes[offset] !== 0xff) {
        offset += 1
        continue
      }
      let markerOffset = offset + 1
      while (markerOffset < bytes.length && bytes[markerOffset] === 0xff) markerOffset += 1
      if (markerOffset >= bytes.length) return false
      const entropyMarker = bytes[markerOffset]!
      if (entropyMarker === 0x00 || (entropyMarker >= 0xd0 && entropyMarker <= 0xd7)) {
        offset = markerOffset + 1
        continue
      }
      offset = markerOffset - 1
      break
    }
  }
  return false
}

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

function pngInflatedBytes(
  width: number,
  height: number,
  bitDepth: number,
  colorType: number,
  interlace: number,
): number | null {
  const channels = colorType === 0 || colorType === 3 ? 1 : colorType === 2 ? 3 : colorType === 4 ? 2 : 4
  const bitsPerPixel = channels * bitDepth
  const passSize = (startX: number, startY: number, stepX: number, stepY: number): number | null => {
    if (width <= startX || height <= startY) return 0
    const passWidth = Math.ceil((width - startX) / stepX)
    const passHeight = Math.ceil((height - startY) / stepY)
    const rowBytes = Math.ceil((passWidth * bitsPerPixel) / 8) + 1
    const bytes = rowBytes * passHeight
    return Number.isSafeInteger(bytes) ? bytes : null
  }
  const passes: ReadonlyArray<readonly [number, number, number, number]> =
    interlace === 0
      ? [[0, 0, 1, 1] as const]
      : [
          [0, 0, 8, 8] as const,
          [4, 0, 8, 8] as const,
          [0, 4, 4, 8] as const,
          [2, 0, 4, 4] as const,
          [0, 2, 2, 4] as const,
          [1, 0, 2, 2] as const,
          [0, 1, 1, 2] as const,
        ]
  let total = 0
  for (const pass of passes) {
    const bytes = passSize(...pass)
    if (bytes === null || !Number.isSafeInteger(total + bytes)) return null
    total += bytes
  }
  return total
}

function isPng(bytes: Uint8Array): boolean {
  const signature = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]
  if (bytes.length < 57 || !signature.every((value, index) => bytes[index] === value)) return false
  let offset = signature.length
  let chunkIndex = 0
  let width = 0
  let height = 0
  let bitDepth = 0
  let colorType = 0
  let interlace = 0
  let sawPalette = false
  let sawData = false
  const imageData: Uint8Array[] = []
  while (offset < bytes.length) {
    if (offset + 12 > bytes.length) return false
    const length =
      bytes[offset]! * 0x1000000 + bytes[offset + 1]! * 0x10000 + bytes[offset + 2]! * 0x100 + bytes[offset + 3]!
    if (!Number.isSafeInteger(length) || length < 0 || offset + 12 + length > bytes.length) return false
    const typeOffset = offset + 4
    const dataOffset = offset + 8
    const crcOffset = dataOffset + length
    const type = Buffer.from(bytes.subarray(typeOffset, dataOffset)).toString('ascii')
    if (!/^[A-Za-z]{4}$/u.test(type)) return false
    const expectedCrc =
      bytes[crcOffset]! * 0x1000000 +
      bytes[crcOffset + 1]! * 0x10000 +
      bytes[crcOffset + 2]! * 0x100 +
      bytes[crcOffset + 3]!
    if (pngCrc(bytes.subarray(typeOffset, crcOffset)) !== expectedCrc >>> 0) return false
    const data = bytes.subarray(dataOffset, crcOffset)
    if (chunkIndex === 0) {
      if (type !== 'IHDR' || length !== 13) return false
      width = data[0]! * 0x1000000 + data[1]! * 0x10000 + data[2]! * 0x100 + data[3]!
      height = data[4]! * 0x1000000 + data[5]! * 0x10000 + data[6]! * 0x100 + data[7]!
      bitDepth = data[8]!
      colorType = data[9]!
      interlace = data[12]!
      const validDepths: Record<number, readonly number[]> = {
        0: [1, 2, 4, 8, 16],
        2: [8, 16],
        3: [1, 2, 4, 8],
        4: [8, 16],
        6: [8, 16],
      }
      if (
        width <= 0 ||
        height <= 0 ||
        !validDepths[colorType]?.includes(bitDepth) ||
        data[10] !== 0 ||
        data[11] !== 0 ||
        (interlace !== 0 && interlace !== 1)
      ) {
        return false
      }
    } else if (type === 'IHDR') {
      return false
    }
    if (type === 'PLTE') {
      if (sawData || length === 0 || length % 3 !== 0 || length > 768) return false
      sawPalette = true
    }
    if (type === 'IDAT') {
      if (colorType === 3 && !sawPalette) return false
      sawData = true
      imageData.push(data)
    }
    offset = crcOffset + 4
    chunkIndex += 1
    if (type !== 'IEND') continue
    if (length !== 0 || !sawData || offset !== bytes.length) return false
    const inflatedBytes = pngInflatedBytes(width, height, bitDepth, colorType, interlace)
    if (inflatedBytes === null || inflatedBytes === 0 || inflatedBytes > MAX_PNG_INFLATED_BYTES) return false
    try {
      const inflated = inflateSync(Buffer.concat(imageData.map((part) => Buffer.from(part))), {
        maxOutputLength: inflatedBytes,
      })
      return inflated.byteLength === inflatedBytes
    } catch {
      return false
    }
  }
  return false
}

export function attachmentMatchesMediaType(bytes: Uint8Array, mediaType: RaftAttachmentMediaType): boolean {
  if (mediaType === 'application/pdf') return isPdf(bytes)
  if (mediaType === 'image/jpeg') return isJpeg(bytes)
  return isPng(bytes)
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
    typeof input.data !== 'string' ||
    input.data.length > Math.ceil((input.sizeBytes as number) / 3) * 4 ||
    !BASE64.test(input.data)
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

export function stripRaftTransportMarkers(content: string): string {
  return content.replace(/<!-- eve-raft-event:[a-f0-9]{64} -->\r?\n?/gu, '').trim()
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
