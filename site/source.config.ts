import { defineCollections, defineConfig, defineDocs } from "fumadocs-mdx/config";
import { pageSchema } from "fumadocs-core/source/schema";
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

export default defineConfig();
