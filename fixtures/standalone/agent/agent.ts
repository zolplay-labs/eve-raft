import { defineAgent } from 'eve'
import { mockModel } from 'eve/evals'

export default defineAgent({
  modelContextWindowTokens: 32_768,
  compaction: { modelContextWindowTokens: 32_768 },
  model: mockModel(({ lastUserMessage, toolResults }) => {
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
  }),
})
