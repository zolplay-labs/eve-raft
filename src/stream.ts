import { createHash } from 'node:crypto'

import { activityEventsForEveEvent, type EveStreamEvent } from './activity.js'
import type { PendingInputRequest } from './state.js'
import type { RaftActivityEvent, RaftInputResponse } from './types.js'

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex')
}

function boundedString(value: unknown, max: number): string | null {
  return typeof value === 'string' && value.length > 0 && value.length <= max ? value : null
}

function normalizeInputRequests(value: unknown): PendingInputRequest[] {
  if (!Array.isArray(value)) return []
  return value.flatMap((raw) => {
    if (!raw || typeof raw !== 'object') return []
    const request = raw as Record<string, unknown>
    const requestId = boundedString(request.requestId, 200)
    const prompt = boundedString(request.prompt, 4_000)
    if (!requestId || !prompt) return []
    const options = Array.isArray(request.options)
      ? request.options.flatMap((rawOption) => {
          if (!rawOption || typeof rawOption !== 'object') return []
          const option = rawOption as Record<string, unknown>
          const id = boundedString(option.id, 200)
          const label = boundedString(option.label, 500)
          return id && label ? [{ id, label }] : []
        })
      : []
    return [{ requestId, prompt, options, allowFreeform: request.allowFreeform === true || options.length === 0 }]
  })
}

export function formatInputRequests(requests: PendingInputRequest[]): { content: string; requestKey: string } | null {
  if (requests.length === 0) return null
  const sections = requests.map((request, requestIndex) => {
    const lines = requests.length > 1 ? [`${requestIndex + 1}. ${request.prompt}`] : [request.prompt]
    for (const [optionIndex, option] of request.options.entries()) {
      lines.push(`   ${optionIndex + 1}) ${option.label}`)
    }
    lines.push(
      request.options.length === 0 && request.allowFreeform
        ? 'Reply with your answer.'
        : 'Reply with the option number or label.',
    )
    return lines.join('\n')
  })
  return { content: sections.join('\n\n'), requestKey: hash(requests.map((request) => request.requestId).join('\n')) }
}

function responseFor(answer: string, request: PendingInputRequest): RaftInputResponse | null {
  const normalized = answer.trim()
  const index = /^(\d+)[).]?$/u.exec(normalized)
  if (index) {
    const option = request.options[Number(index[1]) - 1]
    if (option) return { requestId: request.requestId, optionId: option.id }
  }
  const option = request.options.find(
    (candidate) =>
      candidate.id.toLocaleLowerCase() === normalized.toLocaleLowerCase() ||
      candidate.label.toLocaleLowerCase() === normalized.toLocaleLowerCase(),
  )
  if (option) return { requestId: request.requestId, optionId: option.id }
  return request.allowFreeform && normalized ? { requestId: request.requestId, text: normalized } : null
}

export function parseInputResponses(answer: string, requests: PendingInputRequest[]): RaftInputResponse[] | null {
  if (requests.length === 0) return null
  if (requests.length === 1) {
    const response = responseFor(answer, requests[0]!)
    return response ? [response] : null
  }
  const lines = answer
    .split(/\r?\n/u)
    .map((line) => line.replace(/^\s*\d+[).:-]?\s*/u, '').trim())
    .filter(Boolean)
  if (lines.length !== requests.length) return null
  const responses = requests.map((request, index) => responseFor(lines[index]!, request))
  return responses.every((response) => response !== null) ? (responses as RaftInputResponse[]) : null
}

export type StreamOutcome =
  | { kind: 'success'; message: string | null; turnId: string }
  | { kind: 'waiting'; turnId: string }
  | { kind: 'failed'; turnId: string }

interface StreamDelivery {
  target: string
  sourceMessageId: string
  sessionId: string
  send: (target: string, content: string, idempotencyKey: string) => Promise<unknown>
  activity?: (events: RaftActivityEvent[]) => void
  pendingInput?: (target: string, requests: PendingInputRequest[]) => Promise<void>
  deliveryKey?: (input: { kind: 'hitl'; turnId: string; requestKey: string }) => string
}

export async function consumeEveStream(response: Response, delivery: StreamDelivery): Promise<StreamOutcome> {
  if (!response.body) throw new Error('Eve session stream has no body')
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let latestMessage: string | null = null
  let latestTerminalMessage: string | null = null
  let turnId = 'unknown'
  let turnCompleted = false
  let inputRequested = false

  const processEvent = async (event: EveStreamEvent): Promise<StreamOutcome | null> => {
    const type = typeof event.type === 'string' ? event.type : ''
    const data = event.data ?? {}
    if (typeof data.turnId === 'string') turnId = data.turnId
    delivery.activity?.(
      activityEventsForEveEvent(event, {
        sourceMessageId: delivery.sourceMessageId,
        sessionId: delivery.sessionId,
      }),
    )
    if (type === 'message.completed') {
      const message = typeof data.message === 'string' ? data.message.trim() : null
      if (message) {
        latestMessage = message
        if (data.finishReason !== 'tool-calls') latestTerminalMessage = message
      }
    } else if (type === 'input.requested') {
      const requests = normalizeInputRequests(data.requests)
      const prompt = formatInputRequests(requests)
      if (prompt) {
        inputRequested = true
        await delivery.pendingInput?.(delivery.target, requests)
        await delivery.send(
          delivery.target,
          prompt.content,
          delivery.deliveryKey?.({ kind: 'hitl', turnId, requestKey: prompt.requestKey }) ??
            `eve-raft-hitl-${hash(`${delivery.sourceMessageId}:${turnId}:${prompt.requestKey}`)}`,
        )
      }
    } else if (type === 'turn.completed') {
      turnCompleted = true
    } else if (type === 'turn.failed' || type === 'session.failed') {
      return { kind: 'failed', turnId }
    } else if (type === 'session.waiting') {
      if (inputRequested || !turnCompleted) return { kind: 'waiting', turnId }
      return { kind: 'success', message: latestTerminalMessage ?? latestMessage, turnId }
    } else if (type === 'session.completed') {
      return { kind: 'success', message: latestTerminalMessage ?? latestMessage, turnId }
    }
    return null
  }

  while (true) {
    const { done, value } = await reader.read()
    buffer += decoder.decode(value, { stream: !done })
    let newline = buffer.indexOf('\n')
    while (newline >= 0) {
      const line = buffer.slice(0, newline).trim()
      buffer = buffer.slice(newline + 1)
      if (line) {
        const outcome = await processEvent(JSON.parse(line) as EveStreamEvent)
        if (outcome) {
          await reader.cancel()
          return outcome
        }
      }
      newline = buffer.indexOf('\n')
    }
    if (done) break
  }
  const tail = buffer.trim()
  if (tail) {
    const outcome = await processEvent(JSON.parse(tail) as EveStreamEvent)
    if (outcome) return outcome
  }
  throw new Error('Eve session stream ended before a turn boundary')
}
