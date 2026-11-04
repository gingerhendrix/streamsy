import { Effect, Layer, type Duration } from "effect";
import { Cookies, HttpServer, HttpServerResponse } from "effect/http";
import { BunHttpServer } from "@effect/platform-bun";

export const DEFAULT_PORT = 3000;

export const DEFAULT_HOSTNAME = "127.0.0.1";

export interface ListenerOptions {
  readonly port?: number;
  readonly hostname?: string;
  readonly idleTimeout?: number;
  readonly gracefulShutdownTimeout?: Duration.Input;
}

const listenerOptions = (options: ListenerOptions) => ({
  port: options.port ?? DEFAULT_PORT,
  hostname: options.hostname ?? DEFAULT_HOSTNAME,
  // Bun closes an idle connection at 10 seconds by default, which would cut a
  // parked long poll short. Zero disables that close.
  idleTimeout: options.idleTimeout ?? 0,
  // The listener stops from its own scope finalizer, which drains in-flight
  // requests. The serve finalizer would otherwise call the same stop again
  // behind a bound the caller did not ask for and make every shutdown wait for
  // it. The preemptive wrapper comes back only when the caller sets a bound.
  ...(options.gracefulShutdownTimeout === undefined
    ? { disablePreemptiveShutdown: true }
    : { gracefulShutdownTimeout: options.gracefulShutdownTimeout }),
});

/**
 * Keep Bun's implicit string content type when Effect 4.1 sends a raw Web response.
 * Copying its body to a ReadableStream loses that type on Bun 1.4.
 */
const writableOnBun = (
  response: HttpServerResponse.HttpServerResponse,
): HttpServerResponse.HttpServerResponse => {
  const body = response.body;

  if (!("body" in body) || !(body.body instanceof Response)) return response;

  if (response.status !== body.body.status) return response;

  const web = body.body;

  for (const [key, value] of new Headers(response.headers)) web.headers.set(key, value);

  for (const cookie of Cookies.toSetCookieHeaders(response.cookies))
    web.headers.append("set-cookie", cookie);

  // The headers now belong to the Web response, so the platform can send it directly.
  return HttpServerResponse.raw(web, {
    status: response.status,
    statusText: response.statusText,
  });
};

// oxlint-disable effecttsgo/any-unknown-in-error-context -- `HttpServer.make` types the app error as unknown; this wrapper only forwards it.
const bunServer = Layer.effect(
  HttpServer.HttpServer,
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;

    return HttpServer.make({
      address: server.address,
      serve: (httpEffect, middleware) =>
        server.serve(
          Effect.map(middleware === undefined ? httpEffect : middleware(httpEffect), writableOnBun),
        ),
    });
  }),
);
// oxlint-enable effecttsgo/any-unknown-in-error-context

/** The Bun listener that keeps a parked long poll alive. */
export const listener = (options: ListenerOptions = {}) =>
  bunServer.pipe(Layer.provideMerge(BunHttpServer.layer(listenerOptions(options))));
