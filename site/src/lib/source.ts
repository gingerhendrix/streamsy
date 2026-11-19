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

/**
 * Social image served by Cloudflare static assets. A page sets `image` in its
 * frontmatter to use its own file under `public/`. Other pages share `/og.webp`.
 */
export function getPageImage(page: { data: { image?: `/${string}` | undefined } }) {
  return { url: withBasePath(page.data.image ?? "/og.webp") };
}
