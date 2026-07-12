import { defineConfig, defineDocs } from 'fumadocs-mdx/config'
import { remarkAutoFunctionTable } from './src/lib/remark-auto-function-table'

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
    remarkPlugins: [
      remarkAutoFunctionTable,
      [remarkAutoTypeTable, { generator }],
      [remarkAutoTypeTable, { name: 'auto-argument-table', outputName: 'ArgumentTable', generator }],
    ],
  },
})
