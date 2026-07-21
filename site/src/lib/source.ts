import { loader } from "fumadocs-core/source";
import { articles, docs } from "collections/server";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import { toFumadocsSource } from "fumadocs-mdx/runtime/server";

export const articlesSource = loader({
  source: toFumadocsSource(articles, []),
  baseUrl: "/articles",
});

export const source = loader({
  source: docs.toFumadocsSource(),
  baseUrl: "/docs",
  plugins: [lucideIconsPlugin()],
});

/** Shared, pre-rendered social image served by Cloudflare static assets. */
export function getPageImage(_page: { slugs: string[] }) {
  return { segments: ["og.webp"], url: "/og.webp" };
}
