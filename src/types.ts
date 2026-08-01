export const RAFT_CHANNEL_PROTOCOL_VERSION = 1 as const

export const RAFT_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
export const RAFT_ATTACHMENTS_MAX_TOTAL_BYTES = 24 * 1024 * 1024
export const RAFT_ATTACHMENTS_MAX_COUNT = 20

export const RAFT_ATTACHMENT_MEDIA_TYPES = ['application/pdf', 'image/jpeg', 'image/png'] as const

export type RaftAttachmentMediaType = (typeof RAFT_ATTACHMENT_MEDIA_TYPES)[number]
export type RaftSenderType = 'human' | 'agent' | 'system' | 'third_party_app'
export type RaftSurface = 'direct' | 'shared'

export type RaftActivityHookEventName =
  'UserPromptSubmit' | 'PreToolUse' | 'PostToolUse' | 'PostToolUseFailure' | 'Stop' | 'SessionEnd'

export interface RaftActivityEvent {
  schema: 'raft-activity.v1'
  eventId: string
  sessionId: string
  hookEventName: RaftActivityHookEventName
  status: 'ok' | 'error'
  occurredAt: string
  toolName?: string
}

export interface RaftAttachment {
  id: string
  fileName: string
  mediaType: RaftAttachmentMediaType
  sizeBytes: number
  data: string
}

export interface RaftInputResponse {
  requestId: string
  optionId?: string
  text?: string
}

export interface RaftMessage<TAttachment = RaftAttachment> {
  seq?: number
  messageId: string
  createdAt: string
  senderId: string
  senderType: RaftSenderType
  senderName: string
  senderDisplayName?: string | null
  channelType: string
  channelName: string
  parentChannelType?: string | null
  parentChannelName?: string | null
  content: string
  target: string
  replyTarget: string
  taskChannel?: string | null
  taskStatus?: string | null
  taskNumber?: number | null
  taskAssigneeId?: string | null
  taskAssigneeType?: string | null
  attachments: TAttachment[]
  inputResponses?: RaftInputResponse[]
}

export interface RaftEventEnvelope<TAttachment = RaftAttachment> {
  protocolVersion: typeof RAFT_CHANNEL_PROTOCOL_VERSION
  serverId: string
  agentId: string
  agentName: string
  message: RaftMessage<TAttachment>
}

export interface EveAuthContext {
  authenticator: string
  issuer?: string
  principalId: string
  principalType: string
  subject?: string
  attributes: Readonly<Record<string, string | readonly string[]>>
}

export interface RaftPrincipalContext {
  principalId: string
  serverId: string
  agentId: string
  actorId: string
  actorType: RaftSenderType
  handle: string
  displayName: string | null
  surface: RaftSurface
  target: string
  replyTarget: string
  task: { channel: string; number: number } | null
}

export type ResolveAuth = (principal: RaftPrincipalContext) => EveAuthContext | null | Promise<EveAuthContext | null>

export interface CreateRaftChannelOptions {
  channelToken?: string
  resolveAuth?: ResolveAuth
}

export type RaftDispatchResponse =
  | { accepted: false; reason: 'duplicate' | 'ignored' }
  | {
      accepted: true
      kind: 'immediate'
      target: string
      messageId: string
      content: string
      task?: { channel: string; number: number } | null
    }
  | {
      accepted: true
      kind: 'session'
      target: string
      messageId: string
      sessionId: string
      streamPath: string
      streamStartIndex: number
      task: { channel: string; number: number } | null
    }
