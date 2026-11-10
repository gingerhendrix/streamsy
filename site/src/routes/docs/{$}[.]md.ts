import { createFileRoute } from "@tanstack/react-router";
import { decodeMarkdownUrl } from "#/lib/shared";
import { docsLlms, source } from "#/lib/source";

export const Route = createFileRoute("/docs/{$}.md")({
  server: {
    handlers: {
      GET: async ({ params }) => {
        const slugs = decodeMarkdownUrl(params["_splat"]?.split("/") ?? []);
        const page = source.getPage(slugs);

        // Moved slugs have no source page and deliberately return 404 here.
        if (!page) return new Response("Not found", { status: 404 });

        return new Response(await docsLlms.page(page), {
          headers: { "Content-Type": "text/markdown; charset=utf-8" },
        });
      },
    },
  },
});
