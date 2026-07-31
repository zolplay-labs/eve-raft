import { Buffer } from 'node:buffer'

import type { MockLanguageModelV3 } from 'ai/test'
import { defineAgent } from 'eve'
import { mockModel } from 'eve/evals'

const FIXTURE_PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII='

function base64FileData(data: unknown): string | null {
  if (typeof data === 'string') return data
  if (data instanceof Uint8Array) return Buffer.from(data).toString('base64')
  if (!data || typeof data !== 'object') return null
  const tagged = data as { data?: unknown; type?: unknown }
  if (tagged.type === 'data') return base64FileData(tagged.data)
  if (
    tagged.type === 'Buffer' &&
    Array.isArray(tagged.data) &&
    tagged.data.every((value) => Number.isInteger(value) && value >= 0 && value <= 255)
  ) {
    return Buffer.from(tagged.data).toString('base64')
  }
  return null
}

function containsFixturePng(prompt: unknown): boolean {
  if (!Array.isArray(prompt)) return false
  const latestUserMessage = prompt.findLast(
    (message) => message && typeof message === 'object' && (message as { role?: unknown }).role === 'user',
  ) as { content?: unknown } | undefined
  if (!Array.isArray(latestUserMessage?.content)) return false
  const file = latestUserMessage.content.find(
    (part) =>
      part &&
      typeof part === 'object' &&
      (part as { mediaType?: unknown; type?: unknown }).type === 'file' &&
      (part as { mediaType?: unknown }).mediaType === 'image/png',
  ) as { data?: unknown } | undefined
  return file !== undefined && base64FileData(file.data) === FIXTURE_PNG_BASE64
}

const defaultModel = mockModel(({ lastUserMessage, toolResults }) => {
  const latestResult = toolResults.at(-1)
  if (latestResult) return `Fixture resumed: ${JSON.stringify(latestResult.output)}`
  if (lastUserMessage?.toLowerCase().includes('ask me')) {
    return {
      toolCalls: [
        {
          name: 'ask_question',
          input: {
            prompt: 'Choose a release path',
            options: [
              { id: 'ship', label: 'Ship' },
              { id: 'hold', label: 'Hold' },
            ],
            allowFreeform: true,
          },
        },
      ],
    }
  }
  return `Fixture echo: ${lastUserMessage ?? ''}`
}) as MockLanguageModelV3
const attachmentModel = mockModel('Fixture received exact PNG attachment') as MockLanguageModelV3
const defaultGenerate = defaultModel.doGenerate.bind(defaultModel)
const defaultStream = defaultModel.doStream.bind(defaultModel)

defaultModel.doGenerate = async (options) =>
  containsFixturePng(options.prompt) ? attachmentModel.doGenerate(options) : defaultGenerate(options)
defaultModel.doStream = async (options) =>
  containsFixturePng(options.prompt) ? attachmentModel.doStream(options) : defaultStream(options)

export default defineAgent({
  modelContextWindowTokens: 32_768,
  compaction: { modelContextWindowTokens: 32_768 },
  model: defaultModel,
})
