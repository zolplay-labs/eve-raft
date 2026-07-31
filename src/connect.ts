import {
  authorizeDevice,
  canonicalRaftOrigin,
  mintRaftCredential,
  pollDeviceTokenOnce,
  RaftClient,
} from './raft-client.js'
import { StateStore, type RaftCredential } from './state.js'
import { RAFT_CHANNEL_PROTOCOL_VERSION } from './types.js'

export interface ConnectRaftOptions {
  agentId: string
  serverUrl: string
  stateDirectory: string
  replace?: boolean
  signal?: AbortSignal
}

export interface ConnectRaftResult {
  serverUrl: string
  serverId: string
  agentId: string
  agentName: string
  credentialId: string
  protocolVersion: typeof RAFT_CHANNEL_PROTOCOL_VERSION
}

export interface ConnectRaftHooks {
  log(line: string): void
  sleep(milliseconds: number, signal?: AbortSignal): Promise<void>
  now(): number
}

const defaultHooks: ConnectRaftHooks = {
  log: (line) => console.log(line),
  sleep: (milliseconds, signal) =>
    new Promise((resolve) => {
      if (signal?.aborted) return resolve()
      const timeout = setTimeout(resolve, milliseconds)
      signal?.addEventListener(
        'abort',
        () => {
          clearTimeout(timeout)
          resolve()
        },
        { once: true },
      )
    }),
  now: Date.now,
}

export async function connectRaft(
  options: ConnectRaftOptions,
  hooks: Partial<ConnectRaftHooks> = {},
): Promise<ConnectRaftResult> {
  const runtime = { ...defaultHooks, ...hooks }
  const store = new StateStore(options.stateDirectory)
  await store.initialize()
  if ((await store.loadCredential()) && !options.replace) {
    throw new Error(`A Raft credential already exists in ${store.credentialPath}; pass --replace to reconnect`)
  }

  const serverUrl = canonicalRaftOrigin(options.serverUrl)
  const authorization = await authorizeDevice(serverUrl)
  const loginUrl = authorization.verificationUriComplete ?? authorization.verificationUri
  runtime.log(`Open this URL to authorize the Eve agent:\n${loginUrl}`)
  runtime.log(`Code: ${authorization.userCode}`)

  const deadline = runtime.now() + authorization.expiresIn * 1_000
  let token = await pollDeviceTokenOnce(serverUrl, authorization.deviceCode)
  while (!token && runtime.now() < deadline) {
    if (options.signal?.aborted) throw options.signal.reason ?? new Error('Raft connection was cancelled')
    await runtime.sleep(Math.max(1_000, authorization.interval * 1_000), options.signal)
    token = await pollDeviceTokenOnce(serverUrl, authorization.deviceCode)
  }
  if (!token) throw new Error('Raft device authorization expired before it was approved')

  const minted = await mintRaftCredential(serverUrl, options.agentId, token.accessToken)
  if (!minted.apiKey || !minted.agentId || !minted.agentName || !minted.serverId || !minted.credentialId) {
    throw new Error('Raft credential response is incomplete')
  }
  const credential: RaftCredential = {
    schemaVersion: 1,
    serverUrl,
    agentId: minted.agentId,
    agentName: minted.agentName,
    serverId: minted.serverId,
    credentialId: minted.credentialId,
    scopes: Array.isArray(minted.scopes) ? minted.scopes : [],
    apiKey: minted.apiKey,
    createdAt: new Date(runtime.now()).toISOString(),
  }
  if (credential.agentId !== options.agentId) throw new Error('Raft minted a credential for a different external agent')

  const client = new RaftClient(credential)
  const [profile, server] = await Promise.all([client.profile(), client.serverInfo()])
  if (
    profile.kind !== 'agent' ||
    profile.id !== credential.agentId ||
    server.runtimeContext?.agentId !== credential.agentId ||
    server.runtimeContext.serverId !== credential.serverId
  ) {
    throw new Error('Raft credential validation did not match the requested external agent')
  }
  await store.saveCredential(credential)
  runtime.log(`Connected @${credential.agentName}; credential saved in ${store.directory}`)
  return {
    serverUrl,
    serverId: credential.serverId,
    agentId: credential.agentId,
    agentName: profile.name,
    credentialId: credential.credentialId,
    protocolVersion: RAFT_CHANNEL_PROTOCOL_VERSION,
  }
}
