import { loader } from 'fumadocs-core/source'
import { docs } from 'collections/server'
import { lucideIconsPlugin } from 'fumadocs-core/source/lucide-icons'

export const source = loader({
  source: docs.toFumadocsSource(),
  baseUrl: '/docs',
  plugins: [lucideIconsPlugin()],
})

/**
 * Build the og:image route + URL for a docs page.
 *
 * Mirrors Fumadocs' `getPageImage` convention: append a static `image.webp`
 * segment to the page slugs. The matching route handler lives at
 * `src/routes/og/docs/$.tsx`.
 */
export function getPageImage(page: { slugs: string[] }) {
  const segments = [...page.slugs, 'image.webp']
  return {
    segments,
    url: `/og/docs/${segments.join('/')}`,
  }
}
