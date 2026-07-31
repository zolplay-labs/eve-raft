import { timingSafeEqual } from 'node:crypto'

import { defineChannel, GET, POST } from 'eve/channels'

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

export interface RaftChannelState {
  serverId: string
  agentId: string
  target: string
  replyTarget: string
  messageId: string
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

export function createRaftChannel(options: CreateRaftChannelOptions = {}) {
  return defineChannel<RaftChannelState>({
    state: { serverId: '', agentId: '', target: '', replyTarget: '', messageId: '' },
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

        const previousTail = existing
          ? await getSession(existing.sessionId)
              .getStreamTailIndex()
              .catch(() => null)
          : null
        const principal = principalContext(envelope)
        const auth = options.resolveAuth ? await options.resolveAuth(principal) : defaultRaftAuth(principal)
        const content = raftUserContent(message, mention.content)
        const payload = message.inputResponses
          ? { inputResponses: message.inputResponses, ...(message.content ? { message: content } : {}) }
          : content
        const session = await send(payload, {
          auth,
          continuationToken,
          mode: 'conversation',
          state: {
            serverId: envelope.serverId,
            agentId: envelope.agentId,
            target: message.target,
            replyTarget: message.replyTarget,
            messageId: message.messageId,
          },
          title: taskFor(message)
            ? `Raft task #${message.taskNumber}`
            : `${principal.surface === 'direct' ? 'Raft DM' : 'Raft thread'} with @${message.senderName}`,
        })
        const response: RaftDispatchResponse = {
          accepted: true,
          kind: 'session',
          target: message.replyTarget,
          messageId: message.messageId,
          sessionId: session.id,
          streamPath: `/eve/v1/raft/sessions/${encodeURIComponent(session.id)}/stream`,
          streamStartIndex:
            existing && session.id === existing.sessionId && previousTail !== null ? previousTail + 1 : 0,
          task: taskFor(message),
        }
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
}
