import { defineCollections, defineConfig, defineDocs } from "fumadocs-mdx/config";
import { rehypeCodeDefaultOptions } from "fumadocs-core/mdx-plugins";
import { pageSchema } from "fumadocs-core/source/schema";
import { join } from "node:path";
import { z } from "zod";
import { processedMarkdownCode, twoslash } from "./twoslash.ts";

export const articles = defineCollections({
  type: "doc",
  dir: "content/articles",
  schema: pageSchema.extend({
    date: z.iso.date().or(z.date()),
  }),
});

export const docs = defineDocs({
  dir: "content/docs",
  docs: {
    postprocess: {
      includeProcessedMarkdown: { handlers: { code: processedMarkdownCode } },
    },
  },
});

// Content images live in `assets/`, outside the Vite public directory. An image
// link such as `/docs/examples/x.png` resolves to `assets/docs/examples/x.png`, and
// fumadocs imports it as a bundled asset. Imports from `public/` make Vite warn.
//
// Twoslash type-checks each `ts` code block when the page compiles. See `twoslash.ts`.
export default defineConfig({
  mdxOptions: {
    remarkImageOptions: { publicDir: join(process.cwd(), "assets") },
    rehypeCodeOptions: {
      ...rehypeCodeDefaultOptions,
      transformers: [...(rehypeCodeDefaultOptions.transformers ?? []), twoslash()],
    },
  },
});
