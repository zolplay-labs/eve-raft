import { describe, expect, it, vi } from 'vitest'

import { activityEventsForEveEvent } from '../src/activity.ts'
import { consumeEveStream, parseInputResponses } from '../src/stream.ts'

function streamResponse(events: unknown[]): Response {
  return new Response(`${events.map((event) => JSON.stringify(event)).join('\n')}\n`, {
    headers: { 'content-type': 'application/x-ndjson' },
  })
}

describe('privacy-safe activity', () => {
  it('projects lifecycle and sanitized action names without private event content', () => {
    const event = {
      type: 'actions.requested',
      data: {
        turnId: 'turn-1',
        actions: [
          {
            kind: 'tool-call',
            callId: 'call-1',
            toolName: 'read_secret',
            input: { token: 'NEVER_TRANSMIT_ME' },
          },
        ],
      },
      meta: { at: '2026-07-31T00:00:00.000Z' },
    }

    const activity = activityEventsForEveEvent(event, { sourceMessageId: 'message-1', sessionId: 'session-1' })

    expect(activity).toEqual([
      expect.objectContaining({
        schema: 'raft-activity.v1',
        hookEventName: 'PreToolUse',
        status: 'ok',
        toolName: 'read_secret',
      }),
    ])
    expect(JSON.stringify(activity)).not.toContain('NEVER_TRANSMIT_ME')
  })

  it('fails closed on content-shaped specialist identities', () => {
    expect(
      activityEventsForEveEvent(
        {
          type: 'actions.requested',
          data: {
            turnId: 'turn-1',
            actions: [
              { kind: 'subagent-call', callId: 'call-1', subagentName: { secret: 'NEVER_TRANSMIT_ME' } },
              { kind: 'remote-agent-call', callId: 'call-2', remoteAgentName: ['NEVER_TRANSMIT_ME'] },
              { kind: 'subagent-call', callId: 'call-3', subagentName: 'review private customer brief now' },
            ],
          },
        },
        { sourceMessageId: 'message-1', sessionId: 'session-1' },
      ),
    ).toEqual([])

    expect(
      activityEventsForEveEvent(
        {
          type: 'action.result',
          data: {
            turnId: 'turn-1',
            result: { kind: 'subagent-result', callId: 'call-4', subagentName: { secret: 'NEVER_TRANSMIT_ME' } },
          },
        },
        { sourceMessageId: 'message-1', sessionId: 'session-1' },
      ),
    ).toEqual([])
  })
})

describe('Eve stream delivery', () => {
  it('publishes a numbered human-input prompt and parks the session', async () => {
    const send = vi.fn(async () => 'sent-prompt')
    const activity = vi.fn()
    const pendingInput = vi.fn(async () => undefined)
    const response = streamResponse([
      { type: 'turn.started', data: { turnId: 'turn-1', sequence: 0 } },
      {
        type: 'input.requested',
        data: {
          turnId: 'turn-1',
          requests: [
            {
              kind: 'question',
              requestId: 'request-1',
              prompt: 'Which environment?',
              options: [
                { id: 'staging', label: 'Staging' },
                { id: 'production', label: 'Production' },
              ],
            },
          ],
        },
      },
      { type: 'session.waiting', data: { continuationToken: 'token', wait: 'next-user-message' } },
    ])

    const outcome = await consumeEveStream(response, {
      target: '#general:message',
      sourceMessageId: 'message-1',
      sessionId: 'session-1',
      send,
      activity,
      pendingInput,
    })

    expect(outcome).toEqual({ kind: 'waiting', turnId: 'turn-1' })
    expect(send).toHaveBeenCalledWith(
      '#general:message',
      'Which environment?\n   1) Staging\n   2) Production\nReply with the option number or label.',
      expect.stringMatching(/^eve-raft-hitl-/u),
    )
    expect(pendingInput).toHaveBeenCalledWith('#general:message', [
      {
        requestId: 'request-1',
        prompt: 'Which environment?',
        options: [
          { id: 'staging', label: 'Staging' },
          { id: 'production', label: 'Production' },
        ],
        allowFreeform: false,
      },
    ])
    expect(activity).toHaveBeenCalled()
  })

  it('returns only the final completed assistant message', async () => {
    const outcome = await consumeEveStream(
      streamResponse([
        { type: 'turn.started', data: { turnId: 'turn-2' } },
        { type: 'message.completed', data: { turnId: 'turn-2', message: 'working', finishReason: 'tool-calls' } },
        { type: 'message.completed', data: { turnId: 'turn-2', message: 'done', finishReason: 'stop' } },
        { type: 'turn.completed', data: { turnId: 'turn-2' } },
        { type: 'session.waiting', data: { continuationToken: 'token', wait: 'next-user-message' } },
      ]),
      {
        target: 'dm:@Dex:message',
        sourceMessageId: 'message-2',
        sessionId: 'session-2',
        send: vi.fn(),
      },
    )

    expect(outcome).toEqual({ kind: 'success', message: 'done', turnId: 'turn-2' })
  })

  it('renders tool approval through the same numbered Markdown interaction', async () => {
    const send = vi.fn(async () => 'sent-prompt')
    const pendingInput = vi.fn(async () => undefined)

    const outcome = await consumeEveStream(
      streamResponse([
        {
          type: 'input.requested',
          data: {
            turnId: 'turn-approval',
            requests: [
              {
                kind: 'tool-approval',
                requestId: 'approval-1',
                prompt: 'Allow this action?',
                options: [
                  { id: 'approve', label: 'Approve' },
                  { id: 'reject', label: 'Reject' },
                ],
              },
            ],
          },
        },
        { type: 'session.waiting', data: { turnId: 'turn-approval' } },
      ]),
      {
        target: '#tasks:task-1',
        sourceMessageId: 'task-1',
        sessionId: 'session-approval',
        send,
        pendingInput,
      },
    )

    expect(outcome).toEqual({ kind: 'waiting', turnId: 'turn-approval' })
    expect(send).toHaveBeenCalledWith(
      '#tasks:task-1',
      'Allow this action?\n   1) Approve\n   2) Reject\nReply with the option number or label.',
      expect.stringMatching(/^eve-raft-hitl-/u),
    )
  })

  it('maps a numbered or freeform reply back to Eve input responses', () => {
    const pending = [
      {
        requestId: 'request-1',
        prompt: 'Which environment?',
        options: [
          { id: 'staging', label: 'Staging' },
          { id: 'production', label: 'Production' },
        ],
        allowFreeform: false,
      },
    ]

    expect(parseInputResponses('2', pending)).toEqual([{ requestId: 'request-1', optionId: 'production' }])
    expect(parseInputResponses('staging', pending)).toEqual([{ requestId: 'request-1', optionId: 'staging' }])
    expect(parseInputResponses('my own answer', [{ ...pending[0]!, options: [], allowFreeform: true }])).toEqual([
      { requestId: 'request-1', text: 'my own answer' },
    ])
    expect(parseInputResponses('9', pending)).toBeNull()
  })
})
