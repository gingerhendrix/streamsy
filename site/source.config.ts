import { defineCollections, defineConfig, defineDocs } from "fumadocs-mdx/config";
import { pageSchema } from "fumadocs-core/source/schema";
import { join } from "node:path";
import { z } from "zod";

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
      includeProcessedMarkdown: true,
    },
  },
});

// Content images live in `assets/`, outside the Vite public directory. An image
// link such as `/docs/demos/x.png` resolves to `assets/docs/demos/x.png`, and
// fumadocs imports it as a bundled asset. Imports from `public/` make Vite warn.
export default defineConfig({
  mdxOptions: {
    remarkImageOptions: { publicDir: join(process.cwd(), "assets") },
  },
});
