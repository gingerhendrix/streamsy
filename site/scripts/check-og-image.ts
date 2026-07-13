import { readFile } from 'node:fs/promises'

const bytes = new Uint8Array(
  await readFile(new URL('../public/og.webp', import.meta.url)),
)
const ascii = (start: number, end: number) =>
  String.fromCharCode(...bytes.slice(start, end))

if (bytes.length === 0) throw new Error('OG image is empty')
if (ascii(0, 4) !== 'RIFF' || ascii(8, 12) !== 'WEBP') {
  throw new Error('OG image does not have a valid WebP signature')
}

console.log(`valid WebP OG image (${bytes.length} bytes)`)
