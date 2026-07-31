import type { RaftActivityEvent } from './types.js'
import type { RaftCredential, RawRaftMessage } from './state.js'

const JSON_HEADERS = { 'content-type': 'application/json' }
const DEFAULT_TIMEOUT_MS = 20_000
const PENDING_DEVICE_CODES = new Set(['authorization_pending', 'device_authorization_not_ready'])

export class HttpResponseError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly requestPath: string | null = null,
  ) {
    super(message)
    this.name = 'HttpResponseError'
  }
}

export interface FreshnessHoldResponse {
  state: 'held'
  reason?: string
  heldMessages?: RawRaftMessage[]
  recentUnread?: RawRaftMessage[]
  newMessageCount?: number
  seenUpToSeq?: number
}

export class RaftFreshnessHoldError extends Error {
  constructor(
    readonly operation: 'send' | 'task_claim' | 'task_status',
    readonly target: string,
    readonly response: FreshnessHoldResponse,
  ) {
    super(response.reason ?? `Raft held ${operation} for ${target}`)
    this.name = 'RaftFreshnessHoldError'
  }
}

export interface DeviceAuthorization {
  deviceCode: string
  userCode: string
  verificationUri: string
  verificationUriComplete?: string
  expiresIn: number
  interval: number
}

export interface DeviceToken {
  accessToken: string
  refreshToken: string
  userId: string
}

export interface MintedRaftCredential {
  apiKey: string
  agentId: string
  agentName: string
  serverId: string
  credentialId: string
  scopes?: string[]
}

export interface RaftProfile {
  kind: 'human' | 'agent'
  id: string
  name: string
  displayName: string | null
}

export interface RaftServerInfo {
  protocolVersion?: number
  runtimeContext?: { agentId?: string; serverId?: string }
  channels?: Array<{ name?: unknown }>
  humans?: Array<{ name?: unknown }>
  agents?: Array<{ name?: unknown }>
}

interface TaskEnvelope {
  taskNumber?: unknown
  title?: unknown
  status?: unknown
  messageId?: unknown
}

function directoryNames(value: unknown): string[] {
  return Array.isArray(value)
    ? value.flatMap((entry) =>
        entry && typeof entry === 'object' && typeof (entry as { name?: unknown }).name === 'string'
          ? [(entry as { name: string }).name]
          : [],
      )
    : []
}

export function canonicalRaftOrigin(value: string): string {
  const parsed = new URL(value)
  const local = parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1' || parsed.hostname === '[::1]'
  if (
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash ||
    parsed.pathname !== '/' ||
    (parsed.protocol !== 'https:' && !(local && parsed.protocol === 'http:'))
  ) {
    throw new Error(`Expected a safe Raft origin without credentials or path: ${value}`)
  }
  return parsed.origin
}

export function canonicalDeviceAuthorizationUrl(value: string, serverOrigin: string): string {
  const parsed = new URL(value, canonicalRaftOrigin(serverOrigin))
  const local = parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1' || parsed.hostname === '[::1]'
  if (parsed.username || parsed.password || (parsed.protocol !== 'https:' && !(local && parsed.protocol === 'http:'))) {
    throw new Error('Raft device authorization URL is not safe')
  }
  return parsed.toString()
}

async function responseError(response: Response, requestPath: string | null = null): Promise<HttpResponseError> {
  const text = (await response.text().catch(() => '')).slice(0, 500)
  let code = `http_${response.status}`
  let message = text || `HTTP ${response.status}`
  try {
    const parsed = JSON.parse(text) as { code?: unknown; error?: unknown }
    if (typeof parsed.code === 'string') code = parsed.code
    if (typeof parsed.error === 'string') {
      if (typeof parsed.code !== 'string') code = parsed.error
      message = parsed.error
    }
  } catch {
    // The bounded response text is the useful error.
  }
  return new HttpResponseError(response.status, code, message, requestPath)
}

async function jsonResponse<T>(response: Response): Promise<T> {
  if (!response.ok) throw await responseError(response)
  try {
    return (await response.json()) as T
  } catch {
    throw new HttpResponseError(response.status, 'invalid_json', 'Raft returned invalid JSON')
  }
}

function isFreshnessHold(value: unknown): value is FreshnessHoldResponse {
  return Boolean(value && typeof value === 'object' && (value as { state?: unknown }).state === 'held')
}

export async function authorizeDevice(serverUrl: string): Promise<DeviceAuthorization> {
  const origin = canonicalRaftOrigin(serverUrl)
  const response = await jsonResponse<{
    deviceCode?: string
    userCode?: string
    verificationUri?: string
    verificationUriComplete?: string
    expiresIn?: number
    interval?: number
  }>(
    await fetch(new URL('/api/auth/device/authorize', origin), {
      method: 'POST',
      headers: JSON_HEADERS,
      body: JSON.stringify({ clientName: 'Eve Raft' }),
      signal: AbortSignal.timeout(DEFAULT_TIMEOUT_MS),
    }),
  )
  if (!response.deviceCode || !response.userCode || !response.verificationUri) {
    throw new Error('Raft device authorization response is incomplete')
  }
  return {
    deviceCode: response.deviceCode,
    userCode: response.userCode,
    verificationUri: canonicalDeviceAuthorizationUrl(response.verificationUri, origin),
    ...(response.verificationUriComplete
      ? { verificationUriComplete: canonicalDeviceAuthorizationUrl(response.verificationUriComplete, origin) }
      : {}),
    expiresIn: response.expiresIn ?? 600,
    interval: response.interval ?? 5,
  }
}

export async function pollDeviceTokenOnce(serverUrl: string, deviceCode: string): Promise<DeviceToken | null> {
  const response = await fetch(new URL('/api/auth/device/token', canonicalRaftOrigin(serverUrl)), {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify({ deviceCode }),
    signal: AbortSignal.timeout(DEFAULT_TIMEOUT_MS),
  })
  if (response.ok) return jsonResponse<DeviceToken>(response)
  const error = await responseError(response)
  const pending = [error.code, error.message].some((value) =>
    PENDING_DEVICE_CODES.has(value.trim().toLowerCase().replaceAll(' ', '_')),
  )
  if (pending) return null
  throw error
}

export async function mintRaftCredential(
  serverUrl: string,
  agentId: string,
  accessToken: string,
): Promise<MintedRaftCredential> {
  return jsonResponse(
    await fetch(new URL(`/api/agents/${encodeURIComponent(agentId)}/credentials`, canonicalRaftOrigin(serverUrl)), {
      method: 'POST',
      headers: { ...JSON_HEADERS, authorization: `Bearer ${accessToken}` },
      body: JSON.stringify({}),
      signal: AbortSignal.timeout(DEFAULT_TIMEOUT_MS),
    }),
  )
}

export class RaftClient {
  readonly serverUrl: string

  constructor(readonly credential: RaftCredential) {
    this.serverUrl = canonicalRaftOrigin(credential.serverUrl)
  }

  private async request(
    pathWithQuery: string,
    input: { method?: string; body?: unknown; timeoutMs?: number } = {},
  ): Promise<Response> {
    const response = await fetch(new URL(`/internal/agent-api${pathWithQuery}`, this.serverUrl), {
      method: input.method ?? 'GET',
      headers: {
        ...JSON_HEADERS,
        authorization: `Bearer ${this.credential.apiKey}`,
        'x-agent-id': this.credential.agentId,
        'x-server-id': this.credential.serverId,
      },
      ...(input.body === undefined ? {} : { body: JSON.stringify(input.body) }),
      signal: AbortSignal.timeout(input.timeoutMs ?? DEFAULT_TIMEOUT_MS),
    })
    if (!response.ok) throw await responseError(response, pathWithQuery.split('?')[0] ?? pathWithQuery)
    return response
  }

  async profile(target?: string): Promise<RaftProfile> {
    return jsonResponse(await this.request(`/profile${target ? `?target=${encodeURIComponent(target)}` : ''}`))
  }

  async serverInfo(): Promise<RaftServerInfo> {
    return jsonResponse(await this.request('/server'))
  }

  async drainOnce(): Promise<{ events?: RawRaftMessage[]; has_more?: boolean }> {
    return jsonResponse(await this.request('/events?since=latest'))
  }

  async downloadAttachment(attachmentId: string): Promise<Response> {
    return this.request(`/attachments/${encodeURIComponent(attachmentId)}`, { timeoutMs: 60_000 })
  }

  async resolveMessage(messageId: string): Promise<RawRaftMessage> {
    const response = await jsonResponse<{ message?: RawRaftMessage }>(
      await this.request(`/messages/${encodeURIComponent(messageId)}/resolve`),
    )
    if (!response.message) throw new Error(`Raft message ${messageId} could not be resolved`)
    return response.message
  }

  async send(target: string, content: string, idempotencyKey: string, seenUpToSeq?: number): Promise<string> {
    const response = await jsonResponse<FreshnessHoldResponse | { state?: string; messageId?: string; error?: string }>(
      await this.request('/send', {
        method: 'POST',
        body: { target, content, idempotencyKey, ...(seenUpToSeq === undefined ? {} : { seenUpToSeq }) },
      }),
    )
    if (isFreshnessHold(response)) throw new RaftFreshnessHoldError('send', target, response)
    if (response.state !== 'sent' || !response.messageId)
      throw new Error(response.error ?? 'Raft held outbound delivery')
    return response.messageId
  }

  async forwardActivity(events: RaftActivityEvent[]): Promise<void> {
    if (events.length === 0) return
    const response = await jsonResponse<{ ok?: boolean; rejectedCount?: number }>(
      await this.request('/activity', {
        method: 'POST',
        body: { schema: 'raft-agent-activity-ingest.v1', adapterInstance: 'eve-raft', events },
        timeoutMs: 3_000,
      }),
    )
    if (response.ok === false || (response.rejectedCount ?? 0) > 0) throw new Error('Raft rejected activity')
  }

  async addReaction(messageId: string, emoji: string): Promise<void> {
    await jsonResponse(
      await this.request(`/messages/${encodeURIComponent(messageId)}/reactions`, {
        method: 'POST',
        body: { emoji },
      }),
    )
  }

  async removeReaction(messageId: string, emoji: string): Promise<void> {
    await jsonResponse(
      await this.request(`/messages/${encodeURIComponent(messageId)}/reactions`, {
        method: 'DELETE',
        body: { emoji },
      }),
    )
  }

  private async tasks(channel: string): Promise<TaskEnvelope[]> {
    const response = await jsonResponse<{ tasks?: TaskEnvelope[] }>(
      await this.request(`/tasks?channel=${encodeURIComponent(channel)}&status=all`),
    )
    if (!Array.isArray(response.tasks)) throw new Error('Raft task list response is incomplete')
    return response.tasks
  }

  async taskMessageId(channel: string, taskNumber: number): Promise<string> {
    const task = (await this.tasks(channel)).find((candidate) => candidate.taskNumber === taskNumber)
    if (!task || typeof task.messageId !== 'string') throw new Error(`Raft task #${taskNumber} is missing its message`)
    return task.messageId
  }

  async resolveTaskBoard(
    taskNumber: number,
    title: string | null,
    messageId: string | null = null,
  ): Promise<{ channel: string; messageId: string }> {
    const server = await this.serverInfo()
    const candidates = [
      ...directoryNames(server.channels).map((name) => `#${name}`),
      ...directoryNames(server.humans).map((name) => `dm:@${name}`),
      ...directoryNames(server.agents)
        .filter((name) => name.toLocaleLowerCase() !== this.credential.agentName.toLocaleLowerCase())
        .map((name) => `dm:@${name}`),
    ]
    if (candidates.length > 100) throw new Error('Raft exposed too many task boards to resolve safely')
    const matches: Array<{ channel: string; messageId: string }> = []
    for (const channel of [...new Set(candidates)]) {
      const tasks = await this.tasks(channel)
      for (const task of tasks) {
        if (
          task.taskNumber === taskNumber &&
          (title === null || task.title === title) &&
          (messageId === null || task.messageId === messageId) &&
          typeof task.messageId === 'string'
        ) {
          matches.push({ channel, messageId: task.messageId })
        }
      }
    }
    if (matches.length === 0) throw new Error('Raft task board resolution found no exact match')
    if (matches.length > 1) throw new Error('Raft task board resolution found multiple exact matches')
    return matches[0]!
  }

  async claimTask(channel: string, taskNumber: number, idempotencyKey: string): Promise<void> {
    const response = await jsonResponse<
      FreshnessHoldResponse | { results?: Array<{ success?: boolean; reason?: string }> }
    >(
      await this.request('/tasks/claim', {
        method: 'POST',
        body: { channel, task_numbers: [taskNumber], idempotencyKey },
      }),
    )
    if (isFreshnessHold(response)) throw new RaftFreshnessHoldError('task_claim', channel, response)
    const result = response.results?.[0]
    if (result?.success === false && result.reason !== 'already claimed by you') {
      throw new Error(result.reason ?? `Could not claim task #${taskNumber}`)
    }
  }

  async unclaimTask(channel: string, taskNumber: number, idempotencyKey: string): Promise<void> {
    const response = await jsonResponse<{ success?: boolean }>(
      await this.request('/tasks/unclaim', {
        method: 'POST',
        body: { channel, task_number: taskNumber, idempotencyKey },
      }),
    )
    if (response.success !== true) throw new Error(`Could not unclaim task #${taskNumber}`)
  }

  async advanceTaskStatus(
    channel: string,
    taskNumber: number,
    status: 'in_progress' | 'in_review',
    idempotencyKey: string,
  ): Promise<void> {
    const task = (await this.tasks(channel)).find((candidate) => candidate.taskNumber === taskNumber)
    if (!task || typeof task.status !== 'string') throw new Error(`Raft task #${taskNumber} returned no status`)
    const rank = { todo: 0, in_progress: 1, in_review: 2, done: 3, closed: 3 } as const
    if (!(task.status in rank)) throw new Error(`Raft task #${taskNumber} returned an unknown status`)
    if (rank[task.status as keyof typeof rank] >= rank[status]) return
    const response = await jsonResponse<FreshnessHoldResponse | { ok?: boolean }>(
      await this.request('/tasks/update-status', {
        method: 'POST',
        body: { channel, task_number: taskNumber, status, idempotencyKey },
      }),
    )
    if (isFreshnessHold(response)) throw new RaftFreshnessHoldError('task_status', channel, response)
  }
}
