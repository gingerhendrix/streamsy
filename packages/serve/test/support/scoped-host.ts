import { Context, Deferred, Effect, Fiber, Layer } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { NetAddress } from "effect/unstable/net";
import { Http, type StreamsReader, type StreamsWriter } from "@streamsy/core";
import { listener, type ListenerOptions } from "../../src/bun.ts";

export interface TestHostOptions<E = never> extends ListenerOptions, Http.HttpOptions {
  readonly layer: Layer.Layer<StreamsReader | StreamsWriter, E>;
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
      E | import("effect/unstable/http/HttpServerError").ServeError
    >();
    const owner = yield* Effect.forkDetach(
      Effect.scoped(
        Effect.gen(function* () {
          const routes = Http.routes({
            ...options,
            prefix:
              options.pathPrefix === undefined ? "/" : `/${options.pathPrefix.replace(/^\//, "")}`,
          });
          const context = yield* Layer.build(
            HttpRouter.serve(routes, { disableLogger: true, disableListenLog: true }).pipe(
              Layer.provide(options.layer),
              Layer.provideMerge(listener(options)),
            ),
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
