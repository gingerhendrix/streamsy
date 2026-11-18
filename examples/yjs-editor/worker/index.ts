/* oxlint-disable effecttsgo/async-function -- Native Workers fetch boundary. */
import type { ExecutionContext, IncomingRequestCfProperties } from "@cloudflare/workers-types";
import { router } from "@streamsy/serve/cloudflare";
import type { Env } from "./env.ts";
import { placement } from "./placement.ts";
import { documentId, parseDocumentRoute } from "./route.ts";

export { DocumentObject } from "./objects.ts";

const error = (code: string, message: string, status: number) =>
  Response.json({ error: { code, message } }, { status });

/** The read-only backing streams, each sent to its document's object. */
const streams = router<Env>({
  namespace: (env) => env.DOCUMENTS,
  placement,
  prefix: "/v1/stream",
});

/** Forward each document request to the one object that owns the document. */
export default {
  async fetch(
    request: Request<unknown, IncomingRequestCfProperties>,
    env: Env,
    context: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);
    const route = parseDocumentRoute(url.pathname);

    if (route !== null) {
      try {
        return await env.DOCUMENTS.get(env.DOCUMENTS.idFromName(documentId(route))).fetch(request);
      } catch {
        return error("INTERNAL_ERROR", "Document unavailable; retry", 503);
      }
    }

    if (url.pathname.startsWith("/v1/yjs/"))
      return error("INVALID_REQUEST", "Invalid document path", 400);

    if (url.pathname.startsWith("/v1/stream/")) return streams.fetch!(request, env, context);

    // The page and the Yjs routes share an origin, so no CORS headers are needed.
    if (url.pathname.startsWith("/v1/") || url.pathname.startsWith("/__"))
      return new Response(null, { status: 404 });

    return env.ASSETS.fetch(request);
  },
};
