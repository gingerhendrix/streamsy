import { defineConfig, defineDocs } from 'fumadocs-mdx/config'
import {
  createFileSystemGeneratorCache,
  createGenerator,
  remarkAutoTypeTable,
} from 'fumadocs-typescript'

export const docs = defineDocs({
  dir: 'content/docs',
  docs: {
    postprocess: {
      includeProcessedMarkdown: true,
    },
  },
})

// TypeScript reference generator. Runs at MDX compile time (Node/filesystem) and
// transforms `<auto-type-table>` tags into static `TypeTable` nodes, so the
// generated tables render fine in the client-rendered TanStack Start MDX pipeline.
const generator = createGenerator({
  cache: createFileSystemGeneratorCache('.cache/fumadocs-typescript'),
})

export default defineConfig({
  mdxOptions: {
    remarkPlugins: [[remarkAutoTypeTable, { generator }]],
  },
})
