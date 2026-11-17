/* oxlint-disable effecttsgo/async-function -- Native Workers fetch boundary. */
import { Effect } from "effect";
import { documentId, parseDocumentRoute, protocolError } from "./objects.ts";
import type { Env } from "./env.ts";

export { DocumentObject } from "./objects.ts";

const cors = (response: Response) => {
  const headers = new Headers(response.headers);
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET, HEAD, PUT, POST, DELETE, OPTIONS");
  headers.set(
    "access-control-allow-headers",
    "authorization, content-type, stream-ttl, producer-id, producer-epoch, producer-seq, stream-producer-id, stream-producer-epoch, stream-producer-seq",
  );
  headers.set(
    "access-control-expose-headers",
    "stream-next-offset, stream-up-to-date, stream-cursor, stream-sse-data-encoding, stream-ttl, location",
  );

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") return cors(new Response(null, { status: 204 }));

    // The example exposes the underlying streams just as the upstream proxy does.
    const raw =
      /^\/v1\/stream\/(yjs\/[^/]+\/docs\/.+?)\/(\.updates|\.index|\.awareness-index|\.awareness\/[^/]+|\.snapshots\/[^/]+)$/.exec(
        url.pathname,
      );

    const route = parseDocumentRoute(url.pathname);
    let owner: string | undefined;

    if (raw?.[1]) owner = raw[1];
    else if (route) owner = documentId(route);

    if (owner) {
      try {
        return cors(await env.DOCUMENTS.get(env.DOCUMENTS.idFromName(owner)).fetch(request));
      } catch (error) {
        await Effect.runPromise(Effect.logError("Yjs request failed", error));

        return cors(protocolError("INTERNAL_ERROR", "Document unavailable; retry", 503));
      }
    }

    if (url.pathname.startsWith("/v1/yjs/") && !url.pathname.includes("/__ds/"))
      return cors(protocolError("INVALID_REQUEST", "Invalid document path", 400));

    if (url.pathname.startsWith("/v1/") || url.pathname.startsWith("/__"))
      return cors(new Response(null, { status: 404 }));

    return env.ASSETS.fetch(request);
  },
};
