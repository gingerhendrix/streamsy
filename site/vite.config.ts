import { defineConfig } from 'vite'
import tsconfigPaths from 'vite-tsconfig-paths'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { nitro } from 'nitro/vite'
import mdx from 'fumadocs-mdx/vite'

const config = defineConfig({
  // Takumi (og:image) ships native + WASM bindings that the SSR bundler must not
  // try to transform. Per Fumadocs' Takumi guide, externalize the package for SSR;
  // `takumi-js` then resolves the right renderer at runtime (native on Node, the
  // bundled `@takumi-rs/wasm` module on Cloudflare's `workerd` runtime).
  ssr: {
    external: ['@takumi-rs/image-response'],
  },
  plugins: [
    nitro({
      preset: 'cloudflare_module',
      rollupConfig: { external: [/^@sentry\//] },
    }),
    tsconfigPaths({ projects: ['./tsconfig.json'] }),
    mdx(),
    tailwindcss(),
    tanstackStart(),
    viteReact({ include: /\.(jsx|tsx)$/ }),
  ],
})

export default config
