import { createHash, timingSafeEqual } from 'node:crypto'

import { defineChannel, GET, POST, type Session } from 'eve/channels'

import {
  defaultRaftAuth,
  parseRaftEventEnvelope,
  raftSurface,
  raftUserContent,
  serializeEventStream,
  stripAgentMention,
  taskFor,
} from './protocol.js'
import type { CreateRaftChannelOptions, RaftDispatchResponse, RaftPrincipalContext } from './types.js'

const CHANNEL_TOKEN_HEADER = 'x-eve-raft-token'
const TRANSPORT_MARKER = /^<!-- eve-raft-event:([a-f0-9]{64}) -->$/u

export interface RaftChannelState {
  serverId: string
  agentId: string
  target: string
  replyTarget: string
  messageId: string
  lastEventFingerprint: string | null
}

interface RaftDeliveryPayload {
  message?: unknown
  inputResponses?: unknown
  context?: unknown
  outputSchema?: unknown
}

interface MutableRaftAdapter {
  deliver: (
    payload: RaftDeliveryPayload,
    context: { state: RaftChannelState },
  ) => Record<string, unknown> | undefined | Promise<Record<string, unknown> | undefined>
}

function tokenFrom(options: CreateRaftChannelOptions): string | null {
  return options.channelToken ?? process.env.EVE_RAFT_CHANNEL_TOKEN ?? null
}

function authorized(request: Request, options: CreateRaftChannelOptions): boolean {
  const expected = tokenFrom(options)
  const actual = request.headers.get(CHANNEL_TOKEN_HEADER)
  if (!expected || !actual) return false
  const expectedBytes = Buffer.from(expected)
  const actualBytes = Buffer.from(actual)
  return expectedBytes.byteLength === actualBytes.byteLength && timingSafeEqual(expectedBytes, actualBytes)
}

function principalContext(envelope: NonNullable<ReturnType<typeof parseRaftEventEnvelope>>): RaftPrincipalContext {
  const message = envelope.message
  return {
    principalId: `raft:${envelope.serverId}:${message.senderId}`,
    serverId: envelope.serverId,
    agentId: envelope.agentId,
    actorId: message.senderId,
    actorType: message.senderType,
    handle: message.senderName,
    displayName: message.senderDisplayName ?? null,
    surface: raftSurface(message),
    target: message.target,
    replyTarget: message.replyTarget,
    task: taskFor(message),
  }
}

function eventFingerprint(envelope: NonNullable<ReturnType<typeof parseRaftEventEnvelope>>): string {
  return createHash('sha256')
    .update(`${envelope.serverId}\0${envelope.agentId}\0${envelope.message.messageId}`)
    .digest('hex')
}

function markerFor(fingerprint: string): string {
  return `<!-- eve-raft-event:${fingerprint} -->`
}

function markedText(content: string, fingerprint: string): string {
  return `${markerFor(fingerprint)}\n${content}`
}

function fingerprintFromMessage(message: unknown): string | null {
  const text =
    typeof message === 'string'
      ? message
      : Array.isArray(message)
        ? message.find((part): part is { type: 'text'; text: string } =>
            Boolean(part && typeof part === 'object' && part.type === 'text' && typeof part.text === 'string'),
          )?.text
        : null
  const firstLine = text?.split('\n', 1)[0]
  return firstLine ? (TRANSPORT_MARKER.exec(firstLine)?.[1] ?? null) : null
}

function isMissingSessionInputResponseError(error: unknown): boolean {
  if (!(error instanceof Error)) return false
  const message = error.message.toLowerCase()
  return (
    message.includes('cannot deliver inputresponses') &&
    message.includes('target session was not found') &&
    message.includes('continuation token')
  )
}

function attachIdempotentDelivery(channel: ReturnType<typeof defineChannel<RaftChannelState>>): void {
  const adapter = (channel as unknown as { adapter?: MutableRaftAdapter }).adapter
  if (!adapter || typeof adapter.deliver !== 'function') {
    throw new Error('Installed Eve does not expose the validated channel delivery contract')
  }
  const deliver = adapter.deliver.bind(adapter)
  adapter.deliver = (payload, context) => {
    const { state } = context
    const fingerprint = fingerprintFromMessage(payload.message)
    if (fingerprint && fingerprint === state.lastEventFingerprint) return undefined
    if (fingerprint) state.lastEventFingerprint = fingerprint
    return deliver(payload, context)
  }
}

const STREAM_SCAN_PAGE_SIZE = 256

async function readStreamRange(
  session: Session,
  startIndex: number,
  endIndex: number,
): Promise<Array<{ index: number; type: unknown; data: Record<string, unknown> | undefined }>> {
  const reader = (await session.getEventStream({ startIndex })).getReader()
  const events: Array<{ index: number; type: unknown; data: Record<string, unknown> | undefined }> = []
  try {
    for (let index = startIndex; index <= endIndex; index += 1) {
      const next = await reader.read()
      if (next.done) break
      const event = next.value as unknown as { type?: unknown; data?: Record<string, unknown> }
      events.push({ index, type: event.type, data: event.data })
    }
    return events
  } finally {
    await reader.cancel().catch(() => undefined)
  }
}

async function acceptedStreamStart(session: Session, fingerprint: string): Promise<number | null> {
  const tail = await session.getStreamTailIndex()
  if (tail < 0) return null
  let endIndex = tail
  let waitingBoundaries = 0
  let requiredWaitingBoundaries: number | null = null
  while (endIndex >= 0) {
    const startIndex = Math.max(0, endIndex - STREAM_SCAN_PAGE_SIZE + 1)
    const events = await readStreamRange(session, startIndex, endIndex)
    if (requiredWaitingBoundaries === null) {
      requiredWaitingBoundaries = events.at(-1)?.type === 'session.waiting' ? 2 : 1
    }
    for (const event of events) {
      const turnId = typeof event.data?.turnId === 'string' ? event.data.turnId : null
      if (
        event.type !== 'message.received' ||
        !turnId ||
        typeof event.data?.message !== 'string' ||
        event.data.message.split('\n', 1)[0] !== markerFor(fingerprint)
      ) {
        continue
      }
      const prefix = await readStreamRange(session, Math.max(0, event.index - 4), event.index)
      return (
        prefix.find((candidate) => candidate.type === 'turn.started' && candidate.data?.turnId === turnId)?.index ??
        event.index
      )
    }
    waitingBoundaries += events.filter((event) => event.type === 'session.waiting').length
    if (startIndex === 0 || waitingBoundaries >= requiredWaitingBoundaries) return null
    endIndex = startIndex - 1
  }
  return null
}

function acceptedResponse(
  envelope: NonNullable<ReturnType<typeof parseRaftEventEnvelope>>,
  sessionId: string,
  streamStartIndex: number,
): RaftDispatchResponse {
  return {
    accepted: true,
    kind: 'session',
    target: envelope.message.replyTarget,
    messageId: envelope.message.messageId,
    sessionId,
    streamPath: `/eve/v1/raft/sessions/${encodeURIComponent(sessionId)}/stream`,
    streamStartIndex,
    task: taskFor(envelope.message),
  }
}

export function createRaftChannel(options: CreateRaftChannelOptions = {}) {
  const channel = defineChannel<RaftChannelState>({
    state: { serverId: '', agentId: '', target: '', replyTarget: '', messageId: '', lastEventFingerprint: null },
    routes: [
      POST('/eve/v1/raft/messages', async (request, { getSession, resolveActiveSession, send }) => {
        if (!authorized(request, options)) return Response.json({ ok: false, error: 'unauthorized' }, { status: 401 })

        let input: unknown
        try {
          input = await request.json()
        } catch {
          return Response.json({ ok: false, error: 'invalid_json' }, { status: 400 })
        }
        const envelope = parseRaftEventEnvelope(input)
        if (!envelope) return Response.json({ ok: false, error: 'invalid_message' }, { status: 400 })

        const message = envelope.message
        const continuationToken = `${envelope.serverId}:${envelope.agentId}:${message.replyTarget}`
        const existing = await resolveActiveSession({ continuationToken })
        const fingerprint = eventFingerprint(envelope)
        const mention = stripAgentMention(message.content, envelope.agentName)
        const direct = raftSurface(message) === 'direct'
        const assignedTask =
          message.taskAssigneeType === 'agent' &&
          message.taskAssigneeId === envelope.agentId &&
          message.taskStatus !== 'done' &&
          message.taskStatus !== 'closed'
        const ownMessage = message.senderType === 'agent' && message.senderId === envelope.agentId
        if (ownMessage || (!direct && !mention.mentioned && !assignedTask && !existing)) {
          const ignored: RaftDispatchResponse = { accepted: false, reason: 'ignored' }
          return Response.json(ignored)
        }

        if (existing) {
          const priorStart = await acceptedStreamStart(getSession(existing.sessionId), fingerprint).catch(() => null)
          if (priorStart !== null) return Response.json(acceptedResponse(envelope, existing.sessionId, priorStart))
        }
        const previousTail = existing
          ? await getSession(existing.sessionId)
              .getStreamTailIndex()
              .catch(() => null)
          : null
        const principal = principalContext(envelope)
        const auth = options.resolveAuth ? await options.resolveAuth(principal) : defaultRaftAuth(principal)
        const content = raftUserContent(message, markedText(mention.content, fingerprint))
        const payload = message.inputResponses ? { inputResponses: message.inputResponses } : content
        let session: Session
        try {
          session = await send(payload, {
            auth,
            continuationToken,
            mode: 'conversation',
            state: {
              serverId: envelope.serverId,
              agentId: envelope.agentId,
              target: message.target,
              replyTarget: message.replyTarget,
              messageId: message.messageId,
              lastEventFingerprint: null,
            },
            title: taskFor(message)
              ? `Raft task #${message.taskNumber}`
              : `${principal.surface === 'direct' ? 'Raft DM' : 'Raft thread'} with @${message.senderName}`,
          })
        } catch (error) {
          if (message.inputResponses && isMissingSessionInputResponseError(error)) {
            return Response.json({ ok: false, error: 'input_session_not_found' }, { status: 410 })
          }
          throw error
        }
        const response = acceptedResponse(
          envelope,
          session.id,
          existing && session.id === existing.sessionId && previousTail !== null ? previousTail + 1 : 0,
        )
        return Response.json(response)
      }),
      GET('/eve/v1/raft/sessions/:sessionId/stream', async (request, { getSession, params }) => {
        if (!authorized(request, options)) return Response.json({ ok: false, error: 'unauthorized' }, { status: 401 })
        const sessionId = params.sessionId
        if (!sessionId) return Response.json({ ok: false, error: 'session_not_found' }, { status: 404 })
        const startIndex = Number(new URL(request.url).searchParams.get('startIndex') ?? '0')
        if (!Number.isSafeInteger(startIndex) || startIndex < 0) {
          return Response.json({ ok: false, error: 'invalid_stream_cursor' }, { status: 400 })
        }
        try {
          const stream = serializeEventStream(await getSession(sessionId).getEventStream({ startIndex }))
          return new Response(stream, {
            headers: {
              'cache-control': 'no-store, no-transform',
              'content-type': 'application/x-ndjson; charset=utf-8',
              'x-accel-buffering': 'no',
            },
          })
        } catch {
          return Response.json({ ok: false, error: 'session_not_found' }, { status: 404 })
        }
      }),
    ],
  })
  attachIdempotentDelivery(channel)
  return channel
}
