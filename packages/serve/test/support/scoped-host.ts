import { routes as protocolRoutes } from "@streamsy/core/protocol";
import { Context, Deferred, Effect, Fiber, Layer } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { NetAddress } from "effect/net";
import { Http } from "@streamsy/core";
import { type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import { Serve } from "../../src/index.ts";
import { listener, type ListenerOptions } from "./runtime.ts";

export interface TestHostOptions<E = never> extends ListenerOptions, Http.HttpOptions {
  readonly layer: Layer.Layer<StreamsReader | StreamsWriter, E>;
  /** Serve through `Serve.layer`, which drains live reads. Unset keeps the abrupt shutdown. */
  readonly drain?: Pick<Serve.LayerOptions, "drainTimeout">;
}

export interface TestHost {
  readonly port: number;
  readonly url: string;
  readonly stop: Effect.Effect<void>;
}

/** Test framework hooks keep this scoped owner alive until teardown interrupts and joins it. */
export function testHost<E>(options: TestHostOptions<E>) {
  return Effect.gen(function* () {
    const ready = yield* Deferred.make<
      HttpServer.HttpServer["Service"],
      E | import("effect/http/HttpServerError").ServeError
    >();

    const owner = yield* Effect.forkDetach(
      Effect.scoped(
        Effect.gen(function* () {
          const routes = protocolRoutes({
            ...options,
            prefix:
              options.pathPrefix === undefined ? "/" : `/${options.pathPrefix.replace(/^\//, "")}`,
          });

          const serveOptions = { disableLogger: true, disableListenLog: true } as const;

          const serve =
            options.drain === undefined
              ? HttpRouter.serve(routes, serveOptions)
              : Serve.layer(routes, { ...serveOptions, ...options.drain });

          const context = yield* Layer.build(
            serve.pipe(Layer.provide(options.layer), Layer.provideMerge(listener(options))),
          );

          yield* Deferred.succeed(ready, Context.get(context, HttpServer.HttpServer));

          return yield* Effect.never;
        }),
      ).pipe(Effect.onError((cause) => Deferred.failCause(ready, cause))),
    );

    const server = yield* Deferred.await(ready).pipe(
      Effect.onInterrupt(() => Fiber.interrupt(owner)),
    );

    return {
      port: NetAddress.isInetAddress(server.address) ? server.address.port : 0,
      url: HttpServer.formatAddress(server.address),
      stop: Fiber.interrupt(owner).pipe(Effect.asVoid),
    } satisfies TestHost;
  });
}
