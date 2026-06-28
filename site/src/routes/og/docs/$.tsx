import { createFileRoute, notFound } from '@tanstack/react-router'
import { generateOGImage } from 'fumadocs-ui/og/takumi'
import { source } from '#/lib/source'

/**
 * Dynamic Open Graph image route for docs pages.
 *
 * URL shape (see `getPageImage` in `#/lib/source`): `/og/docs/<...slugs>/image.webp`.
 * The trailing `image.webp` segment is dropped to recover the page slugs.
 *
 * Rendering uses Takumi via Fumadocs' `generateOGImage` helper, which returns an
 * `ImageResponse` backed by `@takumi-rs/image-response` -> `takumi-js`. `takumi-js`
 * auto-selects its renderer at runtime: native (`@takumi-rs/core`) on Node, and
 * WebAssembly (`@takumi-rs/wasm`) on Cloudflare Workers / edge runtimes. On the
 * `workerd` runtime the `@takumi-rs/wasm/auto` export resolves to a bundled
 * `takumi_wasm_bg.wasm` module import, so no manual WASM init is required here.
 */
export const Route = createFileRoute('/og/docs/$')({
  server: {
    handlers: {
      GET: async ({ request }) => {
        const { pathname } = new URL(request.url)
        const rest = pathname
          .replace(/^\/og\/docs\//, '')
          .replace(/\/image\.webp$/, '')
        const slugs = rest.split('/').filter(Boolean)

        const page = source.getPage(slugs)
        if (!page) throw notFound()

        return generateOGImage({
          title: page.data.title,
          description: page.data.description,
          site: 'Streamsy',
          format: 'webp',
        })
      },
    },
  },
})
