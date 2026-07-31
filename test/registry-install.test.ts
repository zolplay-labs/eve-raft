import { readFile } from 'node:fs/promises'

import { describe, expect, it } from 'vitest'

describe('Zolplay Eve registry', () => {
  it('installs a thin root Raft channel backed by the package', async () => {
    const manifest = JSON.parse(await readFile(new URL('../registry/r/raft.json', import.meta.url), 'utf8')) as {
      dependencies?: string[]
      files?: Array<{ content?: string; target?: string; type?: string }>
      meta?: { eve?: { requires?: string } }
      name?: string
      type?: string
    }

    expect(manifest).toMatchObject({
      name: 'raft',
      type: 'registry:item',
      dependencies: ['@zolplay/eve-raft@^0.1.0'],
      meta: { eve: { requires: '>=0.29.2 <0.30' } },
    })
    expect(manifest.files).toEqual([
      {
        type: 'registry:file',
        path: 'registry/channels/raft.ts',
        target: 'agent/channels/raft.ts',
        content:
          "import { createRaftChannel } from '@zolplay/eve-raft/channel'\n\nexport default createRaftChannel()\n",
      },
    ])
  })
})
