import { describe, expect, it } from 'vitest'

import { parseCli } from '../src/cli-options.ts'

describe('eve-raft CLI options', () => {
  it('parses connect without putting credentials in environment variables', () => {
    expect(
      parseCli(['connect', '--server-url', 'https://api.raft.build', '--agent-id', 'agent-1', '--data-dir', '/data']),
    ).toEqual({
      command: 'connect',
      serverUrl: 'https://api.raft.build',
      agentId: 'agent-1',
      stateDirectory: '/data',
      replace: false,
    })
  })

  it('requires an explicit Eve child command for start', () => {
    expect(parseCli(['start', '--data-dir', '/data', '--eve-port', '3100', '--', 'eve', 'start'])).toEqual({
      command: 'start',
      stateDirectory: '/data',
      healthHost: '0.0.0.0',
      healthPort: 3000,
      eveHost: '127.0.0.1',
      evePort: 3100,
      childCommand: ['eve', 'start'],
    })
    expect(() => parseCli(['start'])).toThrow('start requires a child command after --')
  })

  it('rejects unknown options and invalid ports', () => {
    expect(() => parseCli(['connect', '--wat'])).toThrow('Unknown option')
    expect(() => parseCli(['start', '--eve-port', '70000', '--', 'eve', 'start'])).toThrow('port')
  })
})
