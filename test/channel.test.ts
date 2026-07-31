import { describe, expect, it, vi } from 'vitest'

import { createRaftChannel } from '../src/channel.ts'
import type { RaftEventEnvelope } from '../src/types.ts'

function envelope(overrides: Partial<RaftEventEnvelope['message']> = {}): RaftEventEnvelope {
  return {
    protocolVersion: 1,
    serverId: 'server-1',
    agentId: 'agent-1',
    agentName: 'Dex',
    message: {
      messageId: 'message-1',
      createdAt: '2026-07-31T00:00:00.000Z',
      senderId: 'human-1',
      senderType: 'human',
      senderName: 'cali',
      senderDisplayName: 'Cali',
      channelType: 'dm',
      channelName: 'Dex',
      parentChannelType: null,
      parentChannelName: null,
      content: 'hello',
      target: 'dm:@Dex',
      replyTarget: 'dm:@Dex:message',
      taskChannel: null,
      taskStatus: null,
      taskNumber: null,
      taskAssigneeId: null,
      taskAssigneeType: null,
      attachments: [],
      ...overrides,
    },
  }
}

function request(body: unknown, token = 'channel-secret'): Request {
  return new Request('http://127.0.0.1/eve/v1/raft/messages', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-eve-raft-token': token },
    body: JSON.stringify(body),
  })
}

function routeHarness(options: { existingSessionId?: string } = {}) {
  const send = vi.fn(async (_message: unknown, sendOptions: { continuationToken: string }) => ({
    id: options.existingSessionId ?? 'session-1',
    continuationToken: sendOptions.continuationToken,
    getStreamTailIndex: vi.fn(async () => 4),
  }))
  const getSession = vi.fn(() => ({
    id: options.existingSessionId ?? 'session-1',
    continuationToken: 'continuation',
    getStreamTailIndex: vi.fn(async () => 4),
  }))
  const resolveActiveSession = vi.fn(async () =>
    options.existingSessionId ? { sessionId: options.existingSessionId } : undefined,
  )
  return {
    send,
    getSession,
    resolveActiveSession,
    args: {
      send,
      getSession,
      resolveActiveSession,
      cancel: vi.fn(),
      reset: vi.fn(),
      receive: vi.fn(),
      params: {},
      waitUntil: vi.fn(),
      requestIp: '127.0.0.1',
    },
  }
}

function messageRoute(channel: ReturnType<typeof createRaftChannel>) {
  const route = channel.routes.find(
    (candidate) => candidate.method === 'POST' && candidate.path === '/eve/v1/raft/messages',
  )
  if (!route || route.transport === 'websocket') throw new Error('Raft message route is missing')
  return route
}

describe('Raft channel', () => {
  it('invokes every direct conversation under a stable Raft principal', async () => {
    const resolveAuth = vi.fn(async ({ principalId }: { principalId: string }) => ({
      authenticator: 'consumer',
      principalId: `mapped:${principalId}`,
      principalType: 'user',
      attributes: {},
    }))
    const channel = createRaftChannel({ channelToken: 'channel-secret', resolveAuth })
    const harness = routeHarness()

    const response = await messageRoute(channel).handler(request(envelope()), harness.args as never)
    const result = (await response.json()) as Record<string, unknown>

    expect(response.status).toBe(200)
    expect(result).toMatchObject({
      accepted: true,
      kind: 'session',
      sessionId: 'session-1',
      streamStartIndex: 0,
      target: 'dm:@Dex:message',
    })
    expect(resolveAuth).toHaveBeenCalledWith(
      expect.objectContaining({
        principalId: 'raft:server-1:human-1',
        surface: 'direct',
      }),
    )
    expect(harness.send).toHaveBeenCalledWith(
      'hello',
      expect.objectContaining({
        auth: expect.objectContaining({ principalId: 'mapped:raft:server-1:human-1' }),
        continuationToken: 'server-1:agent-1:dm:@Dex:message',
        mode: 'conversation',
      }),
    )
  })

  it('requires an explicit shared mention until the Raft thread has a continuation', async () => {
    const channel = createRaftChannel({ channelToken: 'channel-secret' })
    const shared = envelope({
      channelType: 'channel',
      channelName: 'general',
      target: '#general',
      replyTarget: '#general:message',
      content: 'hello everyone',
    })
    const newThread = routeHarness()

    const ignored = await messageRoute(channel).handler(request(shared), newThread.args as never)
    expect(await ignored.json()).toEqual({ accepted: false, reason: 'ignored' })
    expect(newThread.send).not.toHaveBeenCalled()

    const mentioned = await messageRoute(channel).handler(
      request({ ...shared, message: { ...shared.message, content: '@Dex please help' } }),
      newThread.args as never,
    )
    expect(await mentioned.json()).toMatchObject({ accepted: true, kind: 'session' })
    expect(newThread.send).toHaveBeenLastCalledWith('please help', expect.any(Object))

    const continuedThread = routeHarness({ existingSessionId: 'session-existing' })
    const continued = await messageRoute(channel).handler(request(shared), continuedThread.args as never)
    expect(await continued.json()).toMatchObject({
      accepted: true,
      sessionId: 'session-existing',
      streamStartIndex: 5,
    })
  })

  it('ignores the connected agent and rejects callers without the channel token', async () => {
    const channel = createRaftChannel({ channelToken: 'channel-secret' })
    const harness = routeHarness()
    const ownMessage = envelope({ senderId: 'agent-1', senderType: 'agent' })

    const ignored = await messageRoute(channel).handler(request(ownMessage), harness.args as never)
    expect(await ignored.json()).toEqual({ accepted: false, reason: 'ignored' })

    const unauthorized = await messageRoute(channel).handler(request(envelope(), 'wrong-token'), harness.args as never)
    expect(unauthorized.status).toBe(401)
    expect(harness.send).not.toHaveBeenCalled()
  })
})
