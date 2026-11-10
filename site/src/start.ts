import { Array } from "effect";
import { createMiddleware, createStart } from "@tanstack/react-start";
import { redirect } from "@tanstack/react-router";
import { appendResponseHeader } from "@tanstack/react-start/server";
import { isMarkdownPreferred } from "fumadocs-core/negotiation";
import { docsRoute, getPageMarkdownUrl } from "#/lib/shared";

const llmMiddleware = createMiddleware().server(({ next, request }) => {
  const url = new URL(request.url);

  if (
    (url.pathname === docsRoute || url.pathname.startsWith(`${docsRoute}/`)) &&
    !url.pathname.endsWith(".md")
  ) {
    if (isMarkdownPreferred(request)) {
      const slugs = Array.filter(url.pathname.slice(docsRoute.length).split("/"), Boolean);
      url.pathname = getPageMarkdownUrl({ slugs }).url;

      throw redirect({ href: url.href, headers: { Vary: "Accept" } });
    }

    // Both representations vary, including the ordinary HTML response.
    appendResponseHeader("Vary", "Accept");
  }

  return next();
});

export const startInstance = createStart(() => ({
  requestMiddleware: [llmMiddleware],
}));
