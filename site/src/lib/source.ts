import { loader } from 'fumadocs-core/source'
import { docs } from 'collections/server'
import { lucideIconsPlugin } from 'fumadocs-core/source/lucide-icons'

export const source = loader({
  source: docs.toFumadocsSource(),
  baseUrl: '/docs',
  plugins: [lucideIconsPlugin()],
})

/** Shared, pre-rendered social image served by Cloudflare static assets. */
export function getPageImage(_page: { slugs: string[] }) {
  return { segments: ['og.webp'], url: '/og.webp' }
}
