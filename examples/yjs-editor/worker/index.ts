import { Context, Effect } from "effect";
import { HttpRouter, HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";
import { documentId, parseDocumentRoute } from "./route.ts";
import type { Env } from "./env.ts";
import { protocolErrorResponse, YjsProtocolError } from "./protocol-error.ts";

export { DocumentObject } from "./objects.ts";

class WorkerEnv extends Context.Service<WorkerEnv, Env>()("yjs-editor/WorkerEnv") {}

const routes = HttpRouter.add(
  "*",
  "/*",
  Effect.gen(function* (): Effect.fn.Return<
    HttpServerResponse.HttpServerResponse,
    HttpServerError.RequestError | YjsProtocolError,
    HttpServerRequest.HttpServerRequest | WorkerEnv
  > {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const env = yield* WorkerEnv;
    const webRequest = yield* HttpServerRequest.toWeb(request);
    const url = new URL(request.url, "https://internal");

    // The example exposes the underlying streams just as the upstream proxy does.
    const raw =
      /^\/v1\/stream\/(yjs\/[^/]+\/docs\/.+?)\/(\.updates|\.index|\.awareness-index|\.awareness\/[^/]+|\.snapshots\/[^/]+)$/.exec(
        url.pathname,
      );

    const route = parseDocumentRoute(url.pathname);
    const owner = raw?.[1] ?? (route === null ? undefined : documentId(route));

    if (owner !== undefined) {
      return yield* Effect.tryPromise({
        try: () => env.DOCUMENTS.get(env.DOCUMENTS.idFromName(owner)).fetch(webRequest),
        catch: (cause) =>
          new YjsProtocolError({
            code: "INTERNAL_ERROR",
            message: "Document unavailable; retry",
            status: 503,
            cause,
          }),
      }).pipe(
        Effect.map(HttpServerResponse.fromWeb),
        Effect.catchTag("YjsProtocolError", (error) =>
          Effect.logError("Yjs request failed", error).pipe(
            Effect.as(protocolErrorResponse(error)),
          ),
        ),
      );
    }

    if (url.pathname.startsWith("/v1/yjs/") && !url.pathname.includes("/__ds/"))
      return yield* new YjsProtocolError({
        code: "INVALID_REQUEST",
        message: "Invalid document path",
        status: 400,
      });

    if (url.pathname.startsWith("/v1/") || url.pathname.startsWith("/__"))
      return HttpServerResponse.empty({ status: 404 });

    return HttpServerResponse.fromWeb(yield* Effect.promise(() => env.ASSETS.fetch(webRequest)));
  }).pipe(
    Effect.catchTag("YjsProtocolError", (error) => Effect.succeed(protocolErrorResponse(error))),
  ),
);

// The page and Yjs routes share an origin, so no CORS middleware is needed.
const app = HttpRouter.toWebHandler(routes);

export default {
  fetch: (request: Request, env: Env): Promise<Response> =>
    app.handler(request, Context.make(WorkerEnv, env)),
};
