// oxlint-disable-next-line effecttsgo/node-builtin-import -- NodeHttpServer.layer requires a node:http Server factory; Effect has no server-options factory replacement.
import { createServer, type ServerOptions } from "node:http";
import { Effect, Layer, type Duration } from "effect";
import { HttpServer, HttpServerResponse } from "effect/http";
import { NodeHttpServer } from "@effect/platform-node";

export const DEFAULT_PORT = 3000;

export const DEFAULT_HOSTNAME = "127.0.0.1";

export interface ListenerOptions {
  readonly port?: number;
  readonly hostname?: string;
  /**
   * Options for `node:http` `createServer`. The Node defaults keep a parked
   * long poll and an open SSE body alive: `requestTimeout` bounds only the
   * time to receive a request, not the time to answer it.
   */
  readonly server?: ServerOptions;
  readonly gracefulShutdownTimeout?: Duration.Input;
}

const listenOptions = (options: ListenerOptions) => ({
  port: options.port ?? DEFAULT_PORT,
  host: options.hostname ?? DEFAULT_HOSTNAME,
  // The same rule as the Bun listener: the scope finalizer closes the server
  // and waits for in-flight requests, so the preemptive close comes back only
  // when the caller sets a bound.
  ...(options.gracefulShutdownTimeout === undefined
    ? { disablePreemptiveShutdown: true }
    : { gracefulShutdownTimeout: options.gracefulShutdownTimeout }),
});

/**
 * Streamsy's protocol answers some requests with a raw body that holds a web
 * `Response`. The web hosts (Bun, Deno, workerd) send that `Response` as it
 * is. The Node server passes a raw body to `ServerResponse.end`, which cannot
 * write a `Response`, so the request never ends. This turns such a body into
 * a stream body with the same status and headers before Node writes it.
 */
const writableOnNode = (
  response: HttpServerResponse.HttpServerResponse,
): HttpServerResponse.HttpServerResponse => {
  const body = response.body;

  if (!("body" in body) || !(body.body instanceof Response)) return response;

  return HttpServerResponse.fromWeb(body.body).pipe(
    HttpServerResponse.setStatus(response.status, response.statusText),
    HttpServerResponse.setHeaders(response.headers),
    HttpServerResponse.mergeCookies(response.cookies),
  );
};

// oxlint-disable effecttsgo/any-unknown-in-error-context -- `HttpServer.make` types the app error as unknown; this wrapper only forwards it.
const nodeServer = Layer.effect(
  HttpServer.HttpServer,
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;

    return HttpServer.make({
      address: server.address,
      serve: (httpEffect, middleware) =>
        server.serve(
          Effect.map(
            middleware === undefined ? httpEffect : middleware(httpEffect),
            writableOnNode,
          ),
        ),
    });
  }),
);
// oxlint-enable effecttsgo/any-unknown-in-error-context

/**
 * `NodeHttpServer` binds the port when the listener Layer builds. It adds its
 * `request` handler only when the routes serve, and it removes that handler
 * before the server closes. Node drops a request that has no `request`
 * listener and never answers it. Bun and Deno answer such a request with 404.
 *
 * This server always has one `request` listener of its own. When no other
 * listener is attached, it answers 503 with `Retry-After` and closes the
 * connection. A client that retries then reaches the routes after they serve.
 */
const createUnavailableAwareServer = (options: ServerOptions) => {
  const server = createServer(options);

  server.on("request", (_request, response) => {
    if (server.listenerCount("request") > 1) return;
    response.writeHead(503, {
      "content-type": "text/plain; charset=utf-8",
      "retry-after": "1",
      connection: "close",
    });
    response.end("service unavailable");
  });

  return server;
};

/** The Node listener for a Streamsy host. */
export const listener = (options: ListenerOptions = {}) =>
  nodeServer.pipe(
    Layer.provideMerge(
      NodeHttpServer.layer(
        () => createUnavailableAwareServer(options.server ?? {}),
        listenOptions(options),
      ),
    ),
  );
