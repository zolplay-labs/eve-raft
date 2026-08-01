import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'

import type { HttpRouteDefinition } from 'eve/channels'

import { createRaftChannel } from '../src/channel.ts'

interface FakeSession {
  id: string
  continuationToken: string
  events: Record<string, unknown>[]
  state: Record<string, unknown>
  getEventStream(options?: { startIndex?: number }): Promise<ReadableStream<Record<string, unknown>>>
  getStreamTailIndex(): Promise<number>
}

async function requestBody(request: IncomingMessage): Promise<Uint8Array> {
  const chunks: Buffer[] = []
  for await (const chunk of request) chunks.push(Buffer.from(chunk))
  return Buffer.concat(chunks)
}

async function writeWebResponse(response: ServerResponse, webResponse: Response): Promise<void> {
  response.writeHead(webResponse.status, Object.fromEntries(webResponse.headers.entries()))
  if (webResponse.body) {
    const reader = webResponse.body.getReader()
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      response.write(Buffer.from(value))
    }
  }
  response.end()
}

function textFromInput(input: unknown): string {
  if (typeof input === 'string') return input
  if (Array.isArray(input)) {
    return input
      .flatMap((part) => (part && typeof part === 'object' && part.type === 'text' ? [String(part.text)] : []))
      .join('\n')
  }
  if (input && typeof input === 'object' && 'message' in input)
    return textFromInput((input as { message: unknown }).message)
  return ''
}

export class FakeEveServer {
  readonly inputs: Array<{ input: unknown; options: Record<string, unknown> }> = []
  private readonly sessionsByToken = new Map<string, FakeSession>()
  private readonly sessionsById = new Map<string, FakeSession>()
  private server: Server | null = null
  private sequence = 0
  failNextStream = false
  failNextDispatch = false
  failNextDispatchAfterAccept = false
  emptyNextResult = false
  onInput: ((input: unknown) => void) | null = null
  origin = ''

  constructor(readonly channelToken: string) {}

  expireSessions(): void {
    this.sessionsByToken.clear()
    this.sessionsById.clear()
  }

  async start(): Promise<void> {
    const channel = createRaftChannel({ channelToken: this.channelToken })
    const messageRoute = channel.routes.find(
      (route) => route.method === 'POST' && route.path === '/eve/v1/raft/messages',
    ) as HttpRouteDefinition<unknown> | undefined
    const streamRoute = channel.routes.find(
      (route) => route.method === 'GET' && route.path === '/eve/v1/raft/sessions/:sessionId/stream',
    ) as HttpRouteDefinition<unknown> | undefined
    if (!messageRoute || !streamRoute) throw new Error('Fake Eve server could not find Raft routes')
    const adapter = (
      channel as unknown as {
        adapter?: {
          deliver?: (
            payload: Record<string, unknown>,
            context: { state: Record<string, unknown> },
          ) => Record<string, unknown> | undefined | Promise<Record<string, unknown> | undefined>
        }
      }
    ).adapter

    const send = async (input: unknown, options: Record<string, unknown>) => {
      const token = String(options.continuationToken)
      let session = this.sessionsByToken.get(token)
      const inputResponses =
        input &&
        typeof input === 'object' &&
        !Array.isArray(input) &&
        Array.isArray((input as { inputResponses?: unknown }).inputResponses)
      if (!session && inputResponses) {
        throw new Error('Cannot deliver inputResponses — the target session was not found via continuation token.')
      }
      if (!session) {
        const id = `session-${++this.sequence}`
        const state =
          options.state && typeof options.state === 'object' && !Array.isArray(options.state)
            ? structuredClone(options.state as Record<string, unknown>)
            : {}
        session = this.createSession(id, token, state)
        this.sessionsByToken.set(token, session)
        this.sessionsById.set(id, session)
      }
      if (adapter?.deliver) {
        const payload =
          input && typeof input === 'object' && !Array.isArray(input)
            ? (input as Record<string, unknown>)
            : { message: input }
        const delivered = await adapter.deliver(payload, { state: session.state })
        if (delivered === undefined) return session
      }
      this.inputs.push({ input, options })
      this.onInput?.(input)
      const turnId = `turn-${this.inputs.length}`
      const receivedMessage = textFromInput(input)
      const responses =
        input &&
        typeof input === 'object' &&
        !Array.isArray(input) &&
        Array.isArray((input as { inputResponses?: unknown }).inputResponses)
          ? (input as { inputResponses: Array<{ optionId?: string; text?: string }> }).inputResponses
          : null
      if (responses) {
        session.events.push(
          { type: 'turn.started', data: { turnId } },
          { type: 'message.received', data: { turnId, message: receivedMessage } },
          {
            type: 'message.completed',
            data: { turnId, finishReason: 'stop', message: `Resumed: ${responses[0]?.optionId ?? responses[0]?.text}` },
          },
          { type: 'turn.completed', data: { turnId } },
          { type: 'session.waiting', data: { continuationToken: token, wait: 'next-user-message' } },
        )
      } else if (textFromInput(input).toLowerCase().includes('ask me')) {
        session.events.push(
          { type: 'turn.started', data: { turnId } },
          { type: 'message.received', data: { turnId, message: receivedMessage } },
          {
            type: 'input.requested',
            data: {
              turnId,
              requests: [
                {
                  kind: 'question',
                  requestId: 'approval-request',
                  prompt: 'Choose a path',
                  options: [
                    { id: 'one', label: 'One' },
                    { id: 'two', label: 'Two' },
                  ],
                },
              ],
            },
          },
          { type: 'session.waiting', data: { continuationToken: token, wait: 'next-user-message' } },
        )
      } else {
        const text = textFromInput(input)
        const message = this.emptyNextResult ? undefined : `Echo: ${text}`
        this.emptyNextResult = false
        session.events.push(
          { type: 'turn.started', data: { turnId } },
          { type: 'message.received', data: { turnId, message: receivedMessage } },
          {
            type: 'actions.requested',
            data: {
              turnId,
              actions: [
                { kind: 'tool-call', callId: `call-${this.inputs.length}`, toolName: 'fixture.echo', input: { text } },
              ],
            },
          },
          {
            type: 'action.result',
            data: {
              turnId,
              status: 'completed',
              result: {
                kind: 'tool-result',
                callId: `call-${this.inputs.length}`,
                toolName: 'fixture.echo',
                output: text,
              },
            },
          },
          { type: 'message.completed', data: { turnId, finishReason: 'stop', ...(message ? { message } : {}) } },
          { type: 'turn.completed', data: { turnId } },
          { type: 'session.waiting', data: { continuationToken: token, wait: 'next-user-message' } },
        )
      }
      return session
    }

    const args = (params: Record<string, string> = {}) => ({
      send,
      resolveActiveSession: async ({ continuationToken }: { continuationToken: string }) => {
        const session = this.sessionsByToken.get(continuationToken)
        return session ? { sessionId: session.id } : undefined
      },
      getSession: (sessionId: string) => {
        const session = this.sessionsById.get(sessionId)
        if (!session) throw new Error('Session not found')
        return session
      },
      cancel: async () => ({ status: 'no_active_turn' }),
      reset: async () => ({ status: 'no_active_session' }),
      receive: async () => {
        throw new Error('Not implemented')
      },
      params,
      waitUntil: () => undefined,
      requestIp: '127.0.0.1',
    })

    this.server = createServer(async (incoming, outgoing) => {
      try {
        const body = await requestBody(incoming)
        const webRequest = new Request(new URL(incoming.url ?? '/', this.origin || 'http://127.0.0.1'), {
          method: incoming.method ?? 'GET',
          headers: incoming.headers as HeadersInit,
          ...(body.byteLength > 0 ? { body: body as BodyInit } : {}),
        })
        const streamMatch = /^\/eve\/v1\/raft\/sessions\/([^/]+)\/stream$/u.exec(new URL(webRequest.url).pathname)
        if (streamMatch && this.failNextStream) {
          this.failNextStream = false
          outgoing.writeHead(503, { 'content-type': 'application/json' })
          outgoing.end(JSON.stringify({ error: 'temporary_failure' }))
          return
        }
        if (!streamMatch && this.failNextDispatch) {
          this.failNextDispatch = false
          outgoing.writeHead(503, { 'content-type': 'application/json' })
          outgoing.end(JSON.stringify({ error: 'temporary_failure' }))
          return
        }
        const webResponse = streamMatch
          ? await streamRoute.handler(webRequest, args({ sessionId: decodeURIComponent(streamMatch[1]!) }) as never)
          : await messageRoute.handler(webRequest, args() as never)
        if (!streamMatch && this.failNextDispatchAfterAccept) {
          this.failNextDispatchAfterAccept = false
          outgoing.writeHead(503, { 'content-type': 'application/json' })
          outgoing.end(JSON.stringify({ error: 'response_lost_after_accept' }))
          return
        }
        await writeWebResponse(outgoing, webResponse)
      } catch (error) {
        outgoing.writeHead(500, { 'content-type': 'text/plain' })
        outgoing.end(error instanceof Error ? error.message : String(error))
      }
    })
    await new Promise<void>((resolve) => this.server!.listen(0, '127.0.0.1', resolve))
    const address = this.server.address()
    if (!address || typeof address === 'string') throw new Error('Fake Eve server did not bind')
    this.origin = `http://127.0.0.1:${address.port}`
  }

  async stop(): Promise<void> {
    if (!this.server) return
    await new Promise<void>((resolve, reject) => this.server!.close((error) => (error ? reject(error) : resolve())))
  }

  private createSession(id: string, continuationToken: string, state: Record<string, unknown>): FakeSession {
    const events: Record<string, unknown>[] = []
    return {
      id,
      continuationToken,
      events,
      state,
      getEventStream: async ({ startIndex = 0 } = {}) =>
        new ReadableStream({
          start(controller) {
            for (const event of events.slice(startIndex)) controller.enqueue(event)
            controller.close()
          },
        }),
      getStreamTailIndex: async () => events.length - 1,
    }
  }
}
