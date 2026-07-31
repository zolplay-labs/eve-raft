import { chmod, mkdir, readFile, rename, stat, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { randomBytes, randomUUID } from 'node:crypto'

export interface RaftCredential {
  schemaVersion: 1
  serverUrl: string
  agentId: string
  agentName: string
  serverId: string
  credentialId: string
  scopes: string[]
  apiKey: string
  createdAt: string
}

export interface RuntimeSettings {
  schemaVersion: 1
  channelToken: string
  createdAt: string
}

export interface RawRaftMessage extends Record<string, unknown> {
  id?: unknown
  message_id?: unknown
}

export interface TaskAnchor {
  messageId: string
  replyTarget: string
  taskChannel: string
}

export interface PendingInputRequest {
  requestId: string
  prompt: string
  options: Array<{ id: string; label: string }>
  allowFreeform: boolean
}

export interface QueuedRaftEvent {
  id: string
  receivedAt: string
  message: RawRaftMessage
  taskPhase?: 'started' | 'delivered' | 'reviewed'
  taskAnchor?: TaskAnchor
  freshnessSeenUpToSeq?: number
  freshnessDeferred?: true
  dispatch?: {
    target: string
    messageId: string
    sessionId: string
    streamPath: string
    streamStartIndex: number
    task: { channel: string; number: number } | null
  }
  replyDelivered?: boolean
}

export interface QueueFile {
  schemaVersion: 1
  events: QueuedRaftEvent[]
}

export interface PendingInputFile {
  schemaVersion: 1
  byReplyTarget: Record<string, PendingInputRequest[]>
}

export interface PendingEventsFile {
  schemaVersion: 1
  events: RawRaftMessage[]
}

const MAX_QUEUE_EVENTS = 1_000
const MAX_QUEUE_BYTES = 16 * 1024 * 1024
const MAX_PENDING_TARGETS = 1_000
const MAX_PENDING_REQUESTS_PER_TARGET = 100
const MAX_PENDING_OPTIONS_PER_REQUEST = 100
const MAX_PENDING_PAGE_EVENTS = 10_000
const MAX_PENDING_PAGE_BYTES = MAX_QUEUE_BYTES * 2

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

async function readJson(pathname: string, maxBytes = MAX_QUEUE_BYTES): Promise<unknown | null> {
  try {
    if ((await stat(pathname)).size > maxBytes) throw new Error(`State file exceeds ${maxBytes} bytes`)
    return JSON.parse(await readFile(pathname, 'utf8')) as unknown
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null
    throw error
  }
}

async function writeJsonAtomic(pathname: string, value: unknown): Promise<void> {
  await mkdir(path.dirname(pathname), { recursive: true, mode: 0o700 })
  const temporary = `${pathname}.${process.pid}.${randomUUID()}.tmp`
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 })
  await rename(temporary, pathname)
  await chmod(pathname, 0o600)
}

function messageId(message: RawRaftMessage): string | null {
  const value = typeof message.message_id === 'string' ? message.message_id : message.id
  return typeof value === 'string' && value.length > 0 && value.length <= 200 ? value : null
}

function boundedString(value: unknown, max: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= max
}

function validTaskAnchor(value: unknown): value is TaskAnchor {
  return (
    isRecord(value) &&
    boundedString(value.messageId, 200) &&
    boundedString(value.replyTarget, 500) &&
    boundedString(value.taskChannel, 500)
  )
}

function validDispatch(value: unknown): value is NonNullable<QueuedRaftEvent['dispatch']> {
  if (
    !isRecord(value) ||
    !boundedString(value.target, 500) ||
    !boundedString(value.messageId, 200) ||
    !boundedString(value.sessionId, 200) ||
    !boundedString(value.streamPath, 1_000) ||
    !Number.isSafeInteger(value.streamStartIndex) ||
    Number(value.streamStartIndex) < 0
  ) {
    return false
  }
  return (
    value.task === null ||
    (isRecord(value.task) &&
      boundedString(value.task.channel, 500) &&
      Number.isSafeInteger(value.task.number) &&
      Number(value.task.number) > 0)
  )
}

function validPendingRequest(value: unknown): value is PendingInputRequest {
  if (
    !isRecord(value) ||
    !boundedString(value.requestId, 200) ||
    !boundedString(value.prompt, 4_000) ||
    typeof value.allowFreeform !== 'boolean' ||
    !Array.isArray(value.options) ||
    value.options.length > MAX_PENDING_OPTIONS_PER_REQUEST
  ) {
    return false
  }
  return value.options.every(
    (option) => isRecord(option) && boundedString(option.id, 200) && boundedString(option.label, 500),
  )
}

function validateQueue(value: unknown): QueueFile {
  if (!isRecord(value) || value.schemaVersion !== 1 || !Array.isArray(value.events)) {
    throw new Error('Durable Raft queue is invalid')
  }
  const events: QueuedRaftEvent[] = []
  for (const valueEvent of value.events) {
    if (
      !isRecord(valueEvent) ||
      !boundedString(valueEvent.id, 200) ||
      !boundedString(valueEvent.receivedAt, 100) ||
      !Number.isFinite(Date.parse(valueEvent.receivedAt)) ||
      !isRecord(valueEvent.message)
    ) {
      throw new Error('Durable Raft queue event is invalid')
    }
    if (
      valueEvent.taskPhase !== undefined &&
      valueEvent.taskPhase !== 'started' &&
      valueEvent.taskPhase !== 'delivered' &&
      valueEvent.taskPhase !== 'reviewed'
    ) {
      throw new Error('Durable Raft task checkpoint is invalid')
    }
    if (valueEvent.freshnessDeferred !== undefined && valueEvent.freshnessDeferred !== true) {
      throw new Error('Durable Raft freshness checkpoint is invalid')
    }
    if (
      valueEvent.freshnessSeenUpToSeq !== undefined &&
      (!Number.isSafeInteger(valueEvent.freshnessSeenUpToSeq) || Number(valueEvent.freshnessSeenUpToSeq) < 0)
    ) {
      throw new Error('Durable Raft freshness cursor is invalid')
    }
    if (valueEvent.taskAnchor !== undefined && !validTaskAnchor(valueEvent.taskAnchor)) {
      throw new Error('Durable Raft task anchor is invalid')
    }
    if (valueEvent.dispatch !== undefined && !validDispatch(valueEvent.dispatch)) {
      throw new Error('Durable Raft dispatch checkpoint is invalid')
    }
    if (valueEvent.replyDelivered !== undefined && valueEvent.replyDelivered !== true) {
      throw new Error('Durable Raft reply checkpoint is invalid')
    }
    events.push(valueEvent as unknown as QueuedRaftEvent)
  }
  if (events.length > MAX_QUEUE_EVENTS) throw new Error('Durable Raft queue has too many events')
  return { schemaVersion: 1, events }
}

export class StateStore {
  readonly directory: string
  readonly credentialPath: string
  readonly settingsPath: string
  readonly queuePath: string
  readonly pendingEventsPath: string
  readonly pendingInputPath: string

  constructor(directory: string) {
    this.directory = path.resolve(directory)
    this.credentialPath = path.join(this.directory, 'credential.json')
    this.settingsPath = path.join(this.directory, 'settings.json')
    this.queuePath = path.join(this.directory, 'queue.json')
    this.pendingEventsPath = path.join(this.directory, 'pending-events.json')
    this.pendingInputPath = path.join(this.directory, 'pending-input.json')
  }

  async initialize(): Promise<void> {
    await mkdir(this.directory, { recursive: true, mode: 0o700 })
    await chmod(this.directory, 0o700)
  }

  async loadCredential(): Promise<RaftCredential | null> {
    const value = await readJson(this.credentialPath, 64 * 1024)
    if (value === null) return null
    if (
      !isRecord(value) ||
      value.schemaVersion !== 1 ||
      typeof value.serverUrl !== 'string' ||
      typeof value.agentId !== 'string' ||
      typeof value.agentName !== 'string' ||
      typeof value.serverId !== 'string' ||
      typeof value.credentialId !== 'string' ||
      !Array.isArray(value.scopes) ||
      !value.scopes.every((scope) => typeof scope === 'string') ||
      typeof value.apiKey !== 'string' ||
      typeof value.createdAt !== 'string'
    ) {
      throw new Error('Stored Raft credential is invalid')
    }
    return value as unknown as RaftCredential
  }

  async saveCredential(credential: RaftCredential): Promise<void> {
    await writeJsonAtomic(this.credentialPath, credential)
  }

  async loadOrCreateSettings(): Promise<RuntimeSettings> {
    const value = await readJson(this.settingsPath, 64 * 1024)
    if (value !== null) {
      if (
        !isRecord(value) ||
        value.schemaVersion !== 1 ||
        typeof value.channelToken !== 'string' ||
        value.channelToken.length < 32 ||
        typeof value.createdAt !== 'string'
      ) {
        throw new Error('Stored Eve Raft settings are invalid')
      }
      return value as unknown as RuntimeSettings
    }
    const settings: RuntimeSettings = {
      schemaVersion: 1,
      channelToken: randomBytes(32).toString('base64url'),
      createdAt: new Date().toISOString(),
    }
    await writeJsonAtomic(this.settingsPath, settings)
    return settings
  }

  async loadQueue(): Promise<QueueFile> {
    const value = await readJson(this.queuePath)
    return value === null ? { schemaVersion: 1, events: [] } : validateQueue(value)
  }

  async loadPendingEvents(): Promise<PendingEventsFile> {
    const value = await readJson(this.pendingEventsPath, MAX_PENDING_PAGE_BYTES)
    if (
      value === null ||
      (isRecord(value) &&
        value.schemaVersion === 1 &&
        Array.isArray(value.events) &&
        value.events.length <= MAX_PENDING_PAGE_EVENTS &&
        value.events.every(isRecord))
    ) {
      return value === null ? { schemaVersion: 1, events: [] } : (value as unknown as PendingEventsFile)
    }
    throw new Error('Stored pending Raft event page is invalid')
  }

  async savePendingEvents(events: RawRaftMessage[]): Promise<void> {
    if (
      events.length > MAX_PENDING_PAGE_EVENTS ||
      !events.every(isRecord) ||
      Buffer.byteLength(JSON.stringify({ schemaVersion: 1, events })) > MAX_PENDING_PAGE_BYTES
    ) {
      throw new Error('Pending Raft event page exceeds its durable bounds')
    }
    await writeJsonAtomic(this.pendingEventsPath, { schemaVersion: 1, events })
  }

  async appendEvents(
    queue: QueueFile,
    messages: RawRaftMessage[],
  ): Promise<{ added: number; consumed: number; dropped: string[] }> {
    const known = new Set(queue.events.map((event) => event.id))
    const additions: QueuedRaftEvent[] = []
    const dropped: string[] = []
    let consumed = 0
    for (const message of messages) {
      const id = messageId(message)
      if (!id || known.has(id)) {
        consumed += 1
        continue
      }
      const candidate: QueuedRaftEvent = { id, receivedAt: new Date().toISOString(), message }
      const isolated = { schemaVersion: 1 as const, events: [candidate] }
      if (Buffer.byteLength(JSON.stringify(isolated)) > MAX_QUEUE_BYTES) {
        dropped.push(id)
        known.add(id)
        consumed += 1
        continue
      }
      if (queue.events.length + additions.length >= MAX_QUEUE_EVENTS) break
      const next = { schemaVersion: 1 as const, events: [...queue.events, ...additions, candidate] }
      if (Buffer.byteLength(JSON.stringify(next)) > MAX_QUEUE_BYTES) break
      additions.push(candidate)
      known.add(id)
      consumed += 1
    }
    if (additions.length > 0) {
      queue.events = [...queue.events, ...additions]
      await writeJsonAtomic(this.queuePath, queue)
    }
    return { added: additions.length, consumed, dropped }
  }

  async checkpointHead(
    queue: QueueFile,
    eventId: string,
    patch: Partial<
      Pick<
        QueuedRaftEvent,
        'taskPhase' | 'taskAnchor' | 'freshnessSeenUpToSeq' | 'freshnessDeferred' | 'dispatch' | 'replyDelivered'
      >
    >,
  ): Promise<void> {
    const head = queue.events[0]
    if (!head || head.id !== eventId) throw new Error(`Raft queue head changed before checkpointing ${eventId}`)
    queue.events = [{ ...head, ...patch }, ...queue.events.slice(1)]
    await writeJsonAtomic(this.queuePath, queue)
  }

  async shiftEvent(queue: QueueFile, eventId: string): Promise<void> {
    const head = queue.events[0]
    if (!head || head.id !== eventId) throw new Error(`Raft queue head changed before shifting ${eventId}`)
    queue.events = queue.events.slice(1)
    await writeJsonAtomic(this.queuePath, queue)
  }

  async deferHeadEvent(
    queue: QueueFile,
    eventId: string,
    messages: RawRaftMessage[],
    options: { seenUpToSeq?: number } = {},
  ): Promise<{ moved: boolean; consumed: number }> {
    const head = queue.events[0]
    if (!head || head.id !== eventId) throw new Error(`Raft queue head changed before deferring ${eventId}`)
    if (options.seenUpToSeq !== undefined && (!Number.isSafeInteger(options.seenUpToSeq) || options.seenUpToSeq < 0)) {
      throw new Error(`Invalid Raft freshness cursor for ${eventId}`)
    }
    const rest = queue.events.slice(1)
    const known = new Set(queue.events.map((event) => event.id))
    const additions: QueuedRaftEvent[] = []
    let consumed = 0
    for (const message of messages) {
      const id = messageId(message)
      if (!id || known.has(id)) {
        consumed += 1
        continue
      }
      if (rest.length + additions.length + 1 >= MAX_QUEUE_EVENTS) break
      const addition: QueuedRaftEvent = { id, receivedAt: new Date().toISOString(), message }
      const candidate = { schemaVersion: 1 as const, events: [...rest, ...additions, addition, head] }
      if (Buffer.byteLength(JSON.stringify(candidate)) > MAX_QUEUE_BYTES) break
      additions.push(addition)
      known.add(id)
      consumed += 1
    }
    const moved = rest.length > 0 || additions.length > 0
    const deferred = {
      ...head,
      ...(options.seenUpToSeq !== undefined &&
      (head.freshnessSeenUpToSeq === undefined || options.seenUpToSeq > head.freshnessSeenUpToSeq)
        ? { freshnessSeenUpToSeq: options.seenUpToSeq }
        : {}),
    }
    if (moved) delete deferred.freshnessDeferred
    else deferred.freshnessDeferred = true
    queue.events = moved ? [...rest, ...additions, deferred] : [deferred]
    await writeJsonAtomic(this.queuePath, queue)
    return { moved, consumed }
  }

  async replaceHeadWithEvents(
    queue: QueueFile,
    eventId: string,
    messages: RawRaftMessage[],
  ): Promise<{ added: number; consumed: number }> {
    const head = queue.events[0]
    if (!head || head.id !== eventId) throw new Error(`Raft queue head changed before advancing ${eventId}`)
    const rest = queue.events.slice(1)
    const known = new Set(rest.map((event) => event.id))
    const additions: QueuedRaftEvent[] = []
    let consumed = 0
    for (const message of messages) {
      const id = messageId(message)
      if (!id || id === eventId || known.has(id)) {
        consumed += 1
        continue
      }
      if (rest.length + additions.length >= MAX_QUEUE_EVENTS) break
      const addition: QueuedRaftEvent = { id, receivedAt: new Date().toISOString(), message }
      const candidate = { schemaVersion: 1 as const, events: [...rest, ...additions, addition] }
      if (Buffer.byteLength(JSON.stringify(candidate)) > MAX_QUEUE_BYTES) break
      additions.push(addition)
      known.add(id)
      consumed += 1
    }
    queue.events = [...rest, ...additions]
    await writeJsonAtomic(this.queuePath, queue)
    return { added: additions.length, consumed }
  }

  async loadPendingInput(): Promise<PendingInputFile> {
    const value = await readJson(this.pendingInputPath, 1024 * 1024)
    if (value === null) return { schemaVersion: 1, byReplyTarget: {} }
    if (!isRecord(value) || value.schemaVersion !== 1 || !isRecord(value.byReplyTarget)) {
      throw new Error('Stored pending Raft input is invalid')
    }
    const entries = Object.entries(value.byReplyTarget)
    if (
      entries.length > MAX_PENDING_TARGETS ||
      entries.some(
        ([target, requests]) =>
          !boundedString(target, 500) ||
          !Array.isArray(requests) ||
          requests.length === 0 ||
          requests.length > MAX_PENDING_REQUESTS_PER_TARGET ||
          !requests.every(validPendingRequest),
      )
    ) {
      throw new Error('Stored pending Raft input is invalid')
    }
    return value as unknown as PendingInputFile
  }

  async savePendingInput(value: PendingInputFile): Promise<void> {
    const entries = Object.entries(value.byReplyTarget)
    if (
      value.schemaVersion !== 1 ||
      entries.length > MAX_PENDING_TARGETS ||
      entries.some(
        ([target, requests]) =>
          !boundedString(target, 500) ||
          requests.length === 0 ||
          requests.length > MAX_PENDING_REQUESTS_PER_TARGET ||
          !requests.every(validPendingRequest),
      )
    ) {
      throw new Error('Pending Raft input is invalid')
    }
    await writeJsonAtomic(this.pendingInputPath, value)
  }
}
