import { createHash } from 'node:crypto'

import type { RaftActivityEvent, RaftActivityHookEventName } from './types.js'

export interface EveStreamEvent {
  type?: unknown
  data?: Record<string, unknown>
  meta?: { id?: unknown; at?: unknown }
}

const IDENTIFIER = /^[A-Za-z0-9_-]{1,160}$/u
const TOOL_NAME_MAX_LENGTH = 120

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex')
}

function boundedString(value: unknown, max: number): string | null {
  return typeof value === 'string' && value.length > 0 && value.length <= max ? value : null
}

function safeIdentifier(value: string, prefix: string): string {
  return IDENTIFIER.test(value) ? value : `${prefix}_${hash(value).slice(0, 32)}`
}

function eventTime(event: EveStreamEvent): { occurredAt: string; fingerprint: string } {
  const value = event.meta?.at
  if (typeof value === 'string' && Number.isFinite(Date.parse(value))) {
    return { occurredAt: value, fingerprint: value }
  }
  const eventId = boundedString(event.meta?.id, 200)
  return { occurredAt: new Date().toISOString(), fingerprint: eventId ?? 'untimestamped' }
}

function toolName(value: unknown): string | null {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > TOOL_NAME_MAX_LENGTH ||
    !/^[A-Za-z0-9][A-Za-z0-9._:@/-]*$/u.test(value)
  ) {
    return null
  }
  return value
}

function specialistToolName(prefix: 'agent' | 'subagent', value: unknown): string | null {
  if (typeof value !== 'string') return null
  return toolName(`${prefix}:${value}`)
}

function actionIdentity(value: unknown): { callId: string; toolName: string } | null {
  if (!value || typeof value !== 'object') return null
  const action = value as Record<string, unknown>
  const callId = boundedString(action.callId, 200)
  if (!callId) return null
  const name =
    action.kind === 'tool-call'
      ? toolName(action.toolName)
      : action.kind === 'load-skill'
        ? 'load_skill'
        : action.kind === 'subagent-call'
          ? specialistToolName('subagent', action.subagentName)
          : action.kind === 'remote-agent-call'
            ? specialistToolName('agent', action.remoteAgentName)
            : null
  return name ? { callId, toolName: name } : null
}

function resultIdentity(value: unknown): { callId: string; toolName: string } | null {
  if (!value || typeof value !== 'object') return null
  const result = value as Record<string, unknown>
  const callId = boundedString(result.callId, 200)
  if (!callId) return null
  const name =
    result.kind === 'tool-result'
      ? toolName(result.toolName)
      : result.kind === 'load-skill-result'
        ? 'load_skill'
        : result.kind === 'subagent-result'
          ? specialistToolName('subagent', result.subagentName)
          : null
  return name ? { callId, toolName: name } : null
}

function activity(input: {
  sourceMessageId: string
  sessionId: string
  event: EveStreamEvent
  hookEventName: RaftActivityHookEventName
  status: 'ok' | 'error'
  discriminator: string
  toolName?: string
}): RaftActivityEvent {
  const time = eventTime(input.event)
  return {
    schema: 'raft-activity.v1',
    eventId: `eve_raft_${hash(
      `${input.sourceMessageId}:${input.sessionId}:${input.hookEventName}:${input.discriminator}:${time.fingerprint}`,
    )}`,
    sessionId: safeIdentifier(input.sessionId, 'session'),
    hookEventName: input.hookEventName,
    status: input.status,
    occurredAt: time.occurredAt,
    ...(input.toolName ? { toolName: input.toolName } : {}),
  }
}

export function activityEventsForEveEvent(
  event: EveStreamEvent,
  input: { sourceMessageId: string; sessionId: string },
): RaftActivityEvent[] {
  const type = typeof event.type === 'string' ? event.type : ''
  const data = event.data ?? {}
  const turnId = boundedString(data.turnId, 200) ?? 'unknown'
  if (type === 'turn.started' || type === 'step.started') {
    return [
      activity({
        ...input,
        event,
        hookEventName: 'UserPromptSubmit',
        status: 'ok',
        discriminator: `${turnId}:${Number.isSafeInteger(data.stepIndex) ? String(data.stepIndex) : 'turn'}`,
      }),
    ]
  }
  if (type === 'actions.requested') {
    return Array.isArray(data.actions)
      ? data.actions.flatMap((value) => {
          const action = actionIdentity(value)
          return action
            ? [
                activity({
                  ...input,
                  event,
                  hookEventName: 'PreToolUse',
                  status: 'ok',
                  discriminator: `${turnId}:${action.callId}`,
                  toolName: action.toolName,
                }),
              ]
            : []
        })
      : []
  }
  if (type === 'action.result') {
    const result = resultIdentity(data.result)
    if (!result) return []
    const failed = data.status === 'failed' || data.status === 'rejected'
    return [
      activity({
        ...input,
        event,
        hookEventName: failed ? 'PostToolUseFailure' : 'PostToolUse',
        status: failed ? 'error' : 'ok',
        discriminator: `${turnId}:${result.callId}`,
        toolName: result.toolName,
      }),
    ]
  }
  if (type === 'compaction.requested' || type === 'compaction.completed') {
    return [
      activity({
        ...input,
        event,
        hookEventName: type === 'compaction.requested' ? 'PreToolUse' : 'PostToolUse',
        status: 'ok',
        discriminator: `${turnId}:context.compaction`,
        toolName: 'context.compaction',
      }),
    ]
  }
  if (type === 'turn.failed' || type === 'step.failed' || type === 'session.failed') {
    return [
      activity({
        ...input,
        event,
        hookEventName: type === 'session.failed' ? 'SessionEnd' : 'PostToolUseFailure',
        status: 'error',
        discriminator: `${turnId}:${type}`,
        ...(type === 'session.failed' ? {} : { toolName: 'eve.turn' }),
      }),
    ]
  }
  if (
    type === 'input.requested' ||
    type === 'turn.completed' ||
    type === 'turn.cancelled' ||
    type === 'session.waiting'
  ) {
    return [
      activity({
        ...input,
        event,
        hookEventName: 'Stop',
        status: 'ok',
        discriminator: `${turnId}:${type}`,
      }),
    ]
  }
  if (type === 'session.completed') {
    return [
      activity({
        ...input,
        event,
        hookEventName: 'SessionEnd',
        status: 'ok',
        discriminator: 'session.completed',
      }),
    ]
  }
  return []
}
