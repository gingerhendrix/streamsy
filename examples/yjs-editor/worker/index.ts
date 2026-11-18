/* oxlint-disable effecttsgo/async-function -- Native Workers fetch boundary. */
import type { Env } from "./env.ts";
import { backingStreamOwner, documentId, parseDocumentRoute } from "./route.ts";

export { DocumentObject } from "./objects.ts";

const error = (code: string, message: string, status: number) =>
  Response.json({ error: { code, message } }, { status });

/** Forward each document request to the one object that owns the document. */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const route = parseDocumentRoute(url.pathname);
    const owner = backingStreamOwner(url.pathname) ?? (route ? documentId(route) : undefined);

    if (owner !== undefined) {
      try {
        return await env.DOCUMENTS.get(env.DOCUMENTS.idFromName(owner)).fetch(request);
      } catch {
        return error("INTERNAL_ERROR", "Document unavailable; retry", 503);
      }
    }

    if (url.pathname.startsWith("/v1/yjs/"))
      return error("INVALID_REQUEST", "Invalid document path", 400);

    // The page and the Yjs routes share an origin, so no CORS headers are needed.
    if (url.pathname.startsWith("/v1/") || url.pathname.startsWith("/__"))
      return new Response(null, { status: 404 });

    return env.ASSETS.fetch(request);
  },
};
