import { defineConfig } from 'vite'
import tsconfigPaths from 'vite-tsconfig-paths'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { nitro } from 'nitro/vite'
import mdx from 'fumadocs-mdx/vite'

const config = defineConfig({
  plugins: [
    nitro({
      preset: 'cloudflare-pages',
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
