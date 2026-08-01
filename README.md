# Eve Raft

Eve Raft connects a [Raft](https://raft.build) external agent to an [Eve](https://eve.dev) agent. It carries direct conversations, shared mentions and threads, assigned tasks, PDF/JPEG/PNG attachments, native activity, and human-input prompts through a durable runtime.

`@zolplay/eve-raft@0.2.0` is available as an experimental release on npm and through Zolplay's public Eve registry.

## Compatibility

- Node.js 24 or newer
- Eve `>=0.29.2 <0.30`
- Raft External Agents protocol version 1
- PDF, JPEG, and PNG attachments only
- Either a co-located Eve process or a consumer-supplied Eve transport
- Persistent storage for Eve Raft's delivery queue and checkpoints

Credential activation fails closed unless Raft's stable agent and server runtime identities match. Eve 0.30 and later
require a new compatibility check.

## Install in an Eve agent

Add Zolplay's registry and install the Raft channel:

```bash
eve registry add @zolplay=https://zolplay-labs.github.io/eve-raft/r/{name}.json
eve add @zolplay/raft
```

The registry writes a thin root channel at `agent/channels/raft.ts` and installs `@zolplay/eve-raft`. The root file is intentionally small so package updates do not require copying the integration into each agent.

Build the Eve agent, then connect it to a Raft external agent:

```bash
eve build
eve-raft connect --data-dir /data
eve-raft start --data-dir /data -- eve start
```

`connect` asks for the Raft API origin and external-agent ID when omitted. It prints Raft's device-login URL and code, waits for approval, validates the minted agent credential, and stores it under the data directory. No user-managed secret environment variables are required.

The only user-supplied environment value consumed by the default runtime is the platform-provided `PORT` for the public health server. Eve runs on a separate loopback port and receives its private channel token and persistent workflow-data path from the supervisor process.

## Create the Raft external agent

In Raft, create an External Agent for the Eve agent and copy its agent ID. The display name is the handle people mention in shared conversations. Use the Raft API origin `https://api.raft.build` unless your workspace is hosted elsewhere.

Run `eve-raft connect`, open the printed `app.raft.build/login/device` URL, approve access, and leave the command running until it confirms the connected handle. The credential file is written only after Raft returns a matching agent, server, and protocol identity.

## Hosting

Eve Raft is hosting-platform agnostic. It does not require Railway or an inbound webhook. Run it on any server, VM, container host, or process supervisor that can:

- keep the Eve Raft process running continuously
- preserve the selected data directory across restarts and deployments
- make outbound HTTPS requests to the Raft API
- expose the health server's `PORT` when the platform requires a public health check

The included [Dockerfile](./Dockerfile) is a portable container example. The [railway.toml](./railway.toml) file is an optional convenience for Railway.

### Railway example

The included Railway configuration deploys the standalone fixture and configures `/health`.

1. Create an isolated Railway service from this repository.
2. Mount a persistent volume at `/data` before the first connection.
3. Deploy. An unconfigured process is live and reports `state: "unconfigured"` from `/health`.
4. Open a Railway shell in the service and run:

   ```bash
   pnpm exec eve-raft connect --data-dir /data --server-url https://api.raft.build --agent-id YOUR_AGENT_ID
   ```

5. Approve the printed device URL. The running service notices the stored credential without adding an environment variable or rebuilding the image.

The volume owns `credential.json`, `settings.json`, `queue.json`, `pending-events.json`, `pending-input.json`, and Eve's `eve-workflow/` session store. The supervisor always places the local Workflow world inside the selected data directory, so parked human-input turns survive container replacement without another environment variable. Directories use mode `0700`; persisted state files use `0600`.

`GET /health` contains only process liveness, exact Node/Eve/Eve Raft versions, the supported protocol version, the configured server origin, connection state, queue depth, and coarse error codes. It omits raw Raft server, agent, and profile identifiers and never includes credentials, device codes, message content, attachments, prompts, model output, or tool details.

## Behavior

- Every eligible direct-conversation message invokes Eve.
- Shared conversations require an explicit agent mention. Later messages in the resulting Raft thread continue the same Eve session without another mention.
- Messages authored by the connected agent are ignored.
- Assigned tasks are claimed and move from `todo` to `in_progress` to `in_review`. Eve Raft never marks a task done.
- Task replies, reactions, and progress attach to the canonical task thread, including system-authored assignment notices.
- Attachments are limited to 20 files, 20 MiB per file, and 24 MiB total. Media type is checked from bytes.
- Raft activity contains lifecycle state and bounded tool names only. Prompts, reasoning, responses, arguments, results, attachment data, and error details are excluded.
- Eve input requests become numbered Markdown choices. The next reply in the same thread resumes the paused session.
- Incoming events and delivery checkpoints are persisted before side effects. Retries use deterministic idempotency keys and bounded backoff.

## Consumer authorization

Raft principals are stable and consumer-neutral by default. Applications with their own identity system can provide `resolveAuth` in the root channel:

```ts
import { createRaftChannel } from '@zolplay/eve-raft/channel'

export default createRaftChannel({
  resolveAuth: async (principal) => ({
    authenticator: 'my-app',
    principalId: `my-app:${principal.actorId}`,
    principalType: 'user',
    attributes: { raftServerId: principal.serverId },
  }),
})
```

The consumer owns authorization policy. Eve Raft does not include Dex account linking, database access, or Dex-specific permissions.

## Consumer-owned runtime

Applications that already own pairing, credentials, attachment storage, or a private Eve transport can embed the delivery runtime through `@zolplay/eve-raft/consumer`. The consumer supplies the active Raft connection, signed Eve requests, and optional private attachment staging while Eve Raft continues to own polling, queueing, checkpoints, activity, tasks, and restart recovery.

```ts
import {
  EveRaftService,
  HttpResponseError,
  RaftClient,
  type EveRaftConnectionSource,
  type EveRaftTransport,
} from '@zolplay/eve-raft/consumer'

const connectionSource: EveRaftConnectionSource = {
  async load() {
    const credential = await loadActiveCredential()
    return credential ? { identity: credential, client: new RaftClient(credential) } : null
  },
  async rejected(error) {
    if (error instanceof HttpResponseError) await markCredentialRejected(error.status)
  },
}

const eve: EveRaftTransport<PrivateAttachment> = {
  dispatch: (envelope) => dispatchSignedEveRequest(envelope),
  stream: (path, startIndex) => openSignedEveStream(path, startIndex),
}

const service = new EveRaftService<PrivateAttachment>({
  stateDirectory: '/data',
  connectionSource,
  eve,
  prepareAttachment: (input) => stagePrivateAttachment(input),
})

await service.run(abortSignal)
```

Call `service.reloadConnection()` after the consumer activates, rotates, or revokes a credential. The standalone CLI remains the default path and keeps credentials plus inline Base64 attachments inside its own persistent runtime.
Consumers migrating an existing delivery loop can also provide `deliveryKey` to preserve their prior platform idempotency keys while queued work is replayed.

## Development

```bash
pnpm install
pnpm check
pnpm lint
pnpm test
pnpm build
```

`pnpm test` includes a real Eve 0.29.4 fixture that installs the root channel from a locally served registry and loads the packed npm artifact. The protocol suite uses a fake Raft server for deterministic delivery, task, attachment, retry, and restart coverage.

The standalone fixture is under `fixtures/standalone`. Production Dex is not used or modified by this package's release proof.

## License

[MIT](./LICENSE)
