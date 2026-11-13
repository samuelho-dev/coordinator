import { highlightCode, type ExtensionAPI } from '@earendil-works/pi-coding-agent'
import { Text } from '@earendil-works/pi-tui'
import { registerNative, type Host } from './native.ts'

export default function coordinator(host: ExtensionAPI) {
  registerNative(host as unknown as Host, 'pi', (text, highlighted) => new Text(highlighted ? highlightCode(text, 'json').join('\n') : text, 0, 0))
}
