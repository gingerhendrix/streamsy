import { llms, loader } from "fumadocs-core/source";
import { articles, docs } from "collections/server";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import { toFumadocsSource } from "fumadocs-mdx/runtime/server";
import { withBasePath } from "#/lib/base-path";

export const articlesSource = loader({
  source: toFumadocsSource(articles, []),
  baseUrl: withBasePath("/articles"),
});

export const source = loader({
  source: docs.toFumadocsSource(),
  baseUrl: withBasePath("/docs"),
  plugins: [lucideIconsPlugin()],
});

export const docsLlms = llms(source, {
  renderPage: async (page) => `# ${page.data.title} (${page.url})

${await page.data.getText("processed")}`,
});

/** Shared, pre-rendered social image served by Cloudflare static assets. */
export function getPageImage(_page: { slugs: string[] }) {
  return { segments: ["og.webp"], url: withBasePath("/og.webp") };
}
