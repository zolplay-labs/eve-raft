import { createServer, type Server } from 'node:http'

interface SentMessage {
  target: string
  content: string
  idempotencyKey: string
  seenUpToSeq?: number
}

interface TaskRecord {
  channel: string
  taskNumber: number
  title: string
  status: string
  messageId: string
}

async function jsonBody(request: import('node:http').IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = []
  for await (const chunk of request) chunks.push(Buffer.from(chunk))
  if (chunks.length === 0) return {}
  return JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>
}

function json(response: import('node:http').ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'content-type': 'application/json' })
  response.end(JSON.stringify(body))
}

export class FakeRaftServer {
  readonly agentId = 'agent-1'
  readonly agentName = 'Dex'
  readonly serverId = 'server-1'
  apiKey = 'raft-api-key'
  protocolVersion: number | undefined = 1
  eventPolls = 0
  readonly events: Record<string, unknown>[] = []
  readonly sent: SentMessage[] = []
  readonly reactions: Array<{ messageId: string; emoji: string; operation: 'add' | 'remove' }> = []
  readonly activity: Record<string, unknown>[] = []
  readonly attachments = new Map<
    string,
    { bytes: Uint8Array; mediaType: string; declaredSize?: number; truncateTransfer?: boolean }
  >()
  readonly messages = new Map<string, Record<string, unknown>>()
  readonly tasks: TaskRecord[] = []
  readonly taskClaims: Array<{ channel: string; taskNumber: number; operation: 'claim' | 'unclaim' }> = []
  failNextTaskClaimAfterAccept = false
  deviceApproved = false
  nextSendFailure: { status: number; body: Record<string, unknown> } | null = null
  private readonly taskMutationResults = new Map<string, Record<string, unknown>>()
  private server: Server | null = null
  origin = ''

  async start(): Promise<void> {
    this.server = createServer((request, response) => void this.handle(request, response))
    await new Promise<void>((resolve) => this.server!.listen(0, '127.0.0.1', resolve))
    const address = this.server.address()
    if (!address || typeof address === 'string') throw new Error('Fake Raft server did not bind')
    this.origin = `http://127.0.0.1:${address.port}`
  }

  async stop(): Promise<void> {
    if (!this.server) return
    await new Promise<void>((resolve, reject) => this.server!.close((error) => (error ? reject(error) : resolve())))
  }

  approveDevice(): void {
    this.deviceApproved = true
  }

  addTask(task: TaskRecord): void {
    this.tasks.push(task)
    this.messages.set(task.messageId, {
      id: task.messageId,
      message_id: task.messageId,
      sender_name: 'owner',
      sender_type: 'human',
      channel_type: task.channel.startsWith('dm:') ? 'dm' : 'channel',
      channel_name: task.channel.replace(/^#|^dm:@/u, ''),
      content: task.title,
      task_number: task.taskNumber,
      task_status: task.status,
      task_assignee_id: this.agentId,
      task_assignee_type: 'agent',
    })
  }

  private authorized(request: import('node:http').IncomingMessage): boolean {
    return (
      request.headers.authorization === `Bearer ${this.apiKey}` &&
      request.headers['x-agent-id'] === this.agentId &&
      request.headers['x-server-id'] === this.serverId
    )
  }

  private async handle(
    request: import('node:http').IncomingMessage,
    response: import('node:http').ServerResponse,
  ): Promise<void> {
    const url = new URL(request.url ?? '/', this.origin || 'http://127.0.0.1')
    if (request.method === 'POST' && url.pathname === '/api/auth/device/authorize') {
      json(response, 200, {
        deviceCode: 'device-code',
        userCode: 'ABCD-EFGH',
        verificationUri: '/login/device',
        verificationUriComplete: '/login/device?user_code=ABCD-EFGH',
        expiresIn: 600,
        interval: 1,
      })
      return
    }
    if (request.method === 'POST' && url.pathname === '/api/auth/device/token') {
      json(
        response,
        this.deviceApproved ? 200 : 400,
        this.deviceApproved
          ? { accessToken: 'access-token', refreshToken: 'refresh-token', userId: 'human-1' }
          : { error: 'authorization_pending' },
      )
      return
    }
    if (request.method === 'POST' && url.pathname === `/api/agents/${this.agentId}/credentials`) {
      if (request.headers.authorization !== 'Bearer access-token') {
        json(response, 401, { error: 'unauthorized' })
        return
      }
      json(response, 200, {
        apiKey: this.apiKey,
        agentId: this.agentId,
        agentName: this.agentName,
        serverId: this.serverId,
        credentialId: 'credential-1',
        scopes: ['agent'],
      })
      return
    }
    if (!url.pathname.startsWith('/internal/agent-api/') || !this.authorized(request)) {
      json(response, 401, { error: 'unauthorized' })
      return
    }

    const path = url.pathname.slice('/internal/agent-api'.length)
    if (request.method === 'GET' && path === '/profile') {
      const target = url.searchParams.get('target')
      json(
        response,
        200,
        target
          ? {
              kind: 'human',
              id: target.replace(/^@/u, '') === 'cali' ? 'human-1' : 'human-other',
              name: target.replace(/^@/u, ''),
              displayName: 'Cali',
            }
          : { kind: 'agent', id: this.agentId, name: this.agentName, displayName: 'Dex' },
      )
      return
    }
    if (request.method === 'GET' && path === '/server') {
      json(response, 200, {
        ...(this.protocolVersion === undefined ? {} : { protocolVersion: this.protocolVersion }),
        runtimeContext: { agentId: this.agentId, serverId: this.serverId },
        channels: [
          ...new Set(
            this.tasks.filter((task) => task.channel.startsWith('#')).map((task) => ({ name: task.channel.slice(1) })),
          ),
        ],
        humans: [{ name: 'cali' }],
        agents: [{ name: this.agentName }],
      })
      return
    }
    if (request.method === 'GET' && path === '/events') {
      this.eventPolls += 1
      const events = this.events.splice(0)
      json(response, 200, { events, has_more: false })
      return
    }
    if (request.method === 'POST' && path === '/send') {
      const body = await jsonBody(request)
      const sent = {
        target: String(body.target ?? ''),
        content: String(body.content ?? ''),
        idempotencyKey: String(body.idempotencyKey ?? ''),
        ...(typeof body.seenUpToSeq === 'number' ? { seenUpToSeq: body.seenUpToSeq } : {}),
      }
      const existing = this.sent.find((message) => message.idempotencyKey === sent.idempotencyKey)
      if (!existing && this.nextSendFailure) {
        const failure = this.nextSendFailure
        this.nextSendFailure = null
        json(response, failure.status, failure.body)
        return
      }
      if (!existing) this.sent.push(sent)
      json(response, 200, { state: 'sent', messageId: `sent-${this.sent.indexOf(existing ?? sent) + 1}` })
      return
    }
    if (request.method === 'POST' && path === '/activity') {
      const body = await jsonBody(request)
      const events = Array.isArray(body.events) ? body.events : []
      this.activity.push(...(events as Record<string, unknown>[]))
      json(response, 200, { ok: true, acceptedCount: events.length, rejectedCount: 0 })
      return
    }
    const reactionMatch = /^\/messages\/([^/]+)\/reactions$/u.exec(path)
    if (reactionMatch && (request.method === 'POST' || request.method === 'DELETE')) {
      const body = await jsonBody(request)
      this.reactions.push({
        messageId: decodeURIComponent(reactionMatch[1]!),
        emoji: String(body.emoji ?? ''),
        operation: request.method === 'POST' ? 'add' : 'remove',
      })
      json(response, 200, { ok: true })
      return
    }
    const attachmentMatch = /^\/attachments\/([^/]+)$/u.exec(path)
    if (attachmentMatch && request.method === 'GET') {
      const attachment = this.attachments.get(decodeURIComponent(attachmentMatch[1]!))
      if (!attachment) {
        json(response, 404, { error: 'not_found' })
        return
      }
      response.writeHead(200, {
        'content-type': attachment.mediaType,
        'content-length': String(attachment.declaredSize ?? attachment.bytes.byteLength),
      })
      if (attachment.truncateTransfer) {
        response.write(Buffer.from(attachment.bytes))
        response.destroy()
        return
      }
      response.end(Buffer.from(attachment.bytes))
      return
    }
    const messageMatch = /^\/messages\/([^/]+)\/resolve$/u.exec(path)
    if (messageMatch && request.method === 'GET') {
      json(response, 200, { message: this.messages.get(decodeURIComponent(messageMatch[1]!)) })
      return
    }
    if (request.method === 'GET' && path === '/tasks') {
      const channel = url.searchParams.get('channel')
      json(response, 200, {
        tasks: this.tasks
          .filter((task) => task.channel === channel)
          .map(({ taskNumber, title, status, messageId }) => ({ taskNumber, title, status, messageId })),
      })
      return
    }
    if (request.method === 'POST' && path === '/tasks/claim') {
      const body = await jsonBody(request)
      const idempotencyKey = String(body.idempotencyKey ?? '')
      const replay = this.taskMutationResults.get(idempotencyKey)
      if (idempotencyKey && replay) {
        json(response, 200, replay)
        return
      }
      const channel = String(body.channel ?? '')
      const taskNumber = Number((body.task_numbers as unknown[])?.[0])
      this.taskClaims.push({ channel, taskNumber, operation: 'claim' })
      const result = { results: [{ success: true }] }
      if (idempotencyKey) this.taskMutationResults.set(idempotencyKey, result)
      if (this.failNextTaskClaimAfterAccept) {
        this.failNextTaskClaimAfterAccept = false
        json(response, 503, { error: 'response_lost_after_task_claim' })
        return
      }
      json(response, 200, result)
      return
    }
    if (request.method === 'POST' && path === '/tasks/unclaim') {
      const body = await jsonBody(request)
      const idempotencyKey = String(body.idempotencyKey ?? '')
      const replay = this.taskMutationResults.get(idempotencyKey)
      if (idempotencyKey && replay) {
        json(response, 200, replay)
        return
      }
      const task = this.tasks.find(
        (candidate) => candidate.channel === body.channel && candidate.taskNumber === body.task_number,
      )
      if (task) task.status = 'todo'
      this.taskClaims.push({
        channel: String(body.channel ?? ''),
        taskNumber: Number(body.task_number),
        operation: 'unclaim',
      })
      const result = { success: true }
      if (idempotencyKey) this.taskMutationResults.set(idempotencyKey, result)
      json(response, 200, result)
      return
    }
    if (request.method === 'POST' && path === '/tasks/update-status') {
      const body = await jsonBody(request)
      const idempotencyKey = String(body.idempotencyKey ?? '')
      const replay = this.taskMutationResults.get(idempotencyKey)
      if (idempotencyKey && replay) {
        json(response, 200, replay)
        return
      }
      const task = this.tasks.find(
        (candidate) => candidate.channel === body.channel && candidate.taskNumber === body.task_number,
      )
      if (!task) {
        json(response, 404, { error: 'not_found' })
        return
      }
      task.status = String(body.status)
      const result = { ok: true }
      if (idempotencyKey) this.taskMutationResults.set(idempotencyKey, result)
      json(response, 200, result)
      return
    }
    json(response, 404, { error: 'not_found' })
  }
}
