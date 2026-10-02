/**
 * The matrix app: one web handler for each mounted backend, under
 * `/<backend>`, and one outer route that picks the handler by the first path
 * segment. Each handler owns its storage graph, so the stream services of two
 * backends do not clash (spike S7).
 *
 * `GET /healthz` names the runtime and lists the mounted backends, and each
 * backend that is not mounted with the reason.
 */
import { Http } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import * as Backends from "./backends.ts";

type WebHandler = (request: Request) => Promise<Response>;

/**
 * Builds the backend storage first, so a bad backend fails the start and not
 * the first request. The routes are served under the backend prefix, with the
 * host's shutdown signal, so a stop drains their live reads too.
 */
const mount = (backend: Backends.Mounted) =>
  Effect.gen(function* () {
    const signal = yield* Http.ShutdownSignal;
    const storage = yield* Layer.build(backend.layer);
    const web = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          Http.routes(Backends.streams, { prefix: `/${backend.name}`, access: "full" }).pipe(
            Layer.provideMerge(Layer.succeedContext(storage)),
            Layer.provideMerge(Layer.succeed(Http.ShutdownSignal, signal)),
          ),
          { disableLogger: true },
        ),
      ),
      ({ dispose }) => Effect.promise(dispose),
    );
    const handler: WebHandler = (request) => web.handler(request);
    return [backend.name, handler] as const;
  });

const firstSegment = (url: string) =>
  new URL(url, "http://matrix.invalid").pathname.split("/")[1] ?? "";

const forward = (handler: WebHandler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    // Aborts the inner request when the client goes away. `Effect.abortSignal`
    // would also abort when the request scope closes, which can come before a
    // streamed body ends.
    // oxlint-disable-next-line effecttsgo/abort-controller-in-effect -- the signal must outlive the request scope; see above.
    const controller = new AbortController();
    const web = yield* HttpServerRequest.toWeb(request, { signal: controller.signal });
    const response = yield* Effect.promise(() => handler(web)).pipe(
      Effect.onInterrupt(() => Effect.sync(() => controller.abort())),
    );
    return HttpServerResponse.fromWeb(response);
  }).pipe(
    Effect.orElseSucceed(() =>
      HttpServerResponse.empty({ status: 400, headers: Http.securityHeaders }),
    ),
  );

export interface Options {
  /** The runtime name that `/healthz` reports. */
  readonly runtime: string;
  readonly drivers: Backends.Drivers;
}

/** The app routes. Provide `Http.ShutdownSignal`; `Serve.layer` does. */
export const routes = (options: Options) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const selection = yield* Backends.select(options.drivers);
      const handlers = new Map(yield* Effect.forEach(selection.mounted, mount));
      const health = HttpServerResponse.jsonUnsafe({
        runtime: options.runtime,
        backends: selection.mounted.map((backend) => backend.name),
        unmounted: Object.fromEntries(
          selection.unmounted.map((backend) => [backend.name, backend.reason]),
        ),
      });
      const dispatch = Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const handler = handlers.get(firstSegment(request.url));
        if (handler === undefined) {
          return HttpServerResponse.empty({ status: 404, headers: Http.securityHeaders });
        }
        return yield* forward(handler);
      });
      return Layer.mergeAll(
        Serve.route("/healthz", Effect.succeed(health)),
        HttpRouter.add("*", "/*", dispatch),
      );
    }),
  );
