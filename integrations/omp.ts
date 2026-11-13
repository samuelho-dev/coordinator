import { highlightCode, type ExtensionAPI } from '@oh-my-pi/pi-coding-agent'
import { Text } from '@oh-my-pi/pi-tui'
import { MAGIC_KEYWORDS } from '@oh-my-pi/pi-coding-agent/modes/magic-keywords'
import { isMagicKeyword, setMagicKeywords } from '@oh-my-pi/pi-tui/prompt/magic-keywords'
import { registerNative, type Host } from './native.ts'

export default function coordinator(host: ExtensionAPI) {
  registerNative(host as unknown as Host, 'omp', (text, highlighted) => new Text(highlighted ? highlightCode(text, 'json').join('\n') : text, 0, 0))
  host.on('session_start', async (_event, ctx) => {
    if (!ctx.hasUI || ctx.mode !== 'tui') return
    // ponytail: the native registry has no append API; retain the known core keywords.
    setMagicKeywords([...MAGIC_KEYWORDS.filter(({ word }) => isMagicKeyword(word)), { word: 'coordinate', hue: [190, 260] }])
  })
}
