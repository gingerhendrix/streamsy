import { Context, Effect, Exit, Fiber, Layer, Predicate, Scope } from "effect";
import { HttpServer } from "effect/unstable/http";
import { BunHttpServer } from "@effect/platform-bun";
import type { StreamsReader, StreamsWriter } from "@streamsy/core";
import { app, type HttpOptions } from "@streamsy/core/http";

export interface ServeOptions<E = never> extends HttpOptions {
  readonly layer: Layer.Layer<StreamsReader | StreamsWriter, E>;
  readonly port?: number;
  readonly hostname?: string;
  /**
   * Forwarded to `Bun.serve`. Zero disables Bun's idle close, which a parked
   * long poll needs: the default 10 second close would cut it short.
   */
  readonly idleTimeout?: number;
}

export const DEFAULT_PORT = 3000;
export const DEFAULT_HOSTNAME = "127.0.0.1";

interface ListenerOptions {
  readonly port?: number;
  readonly hostname?: string;
  readonly idleTimeout?: number;
}

const listenerOptions = (options: ListenerOptions) => ({
  port: options.port ?? DEFAULT_PORT,
  hostname: options.hostname ?? DEFAULT_HOSTNAME,
  // Bun closes an idle connection at 10 seconds by default, which would cut a
  // parked long poll short. Zero disables that close.
  idleTimeout: options.idleTimeout ?? 0,
  // The listener stops from its own scope finalizer, which drains in-flight
  // requests. The serve finalizer would otherwise call the same stop again
  // behind a 20 second bound and make every shutdown wait for that bound.
  disablePreemptiveShutdown: true,
});

/** The Bun listener that keeps a parked long poll alive. */
export const listenerLayer = (options: ListenerOptions) =>
  BunHttpServer.layer(listenerOptions(options));

/**
 * The whole Streamsy application on Bun, as one Layer.
 *
 * It serves `Http.app` over the given protocol Layer, with `BunHttpServer` as
 * the listener. Merging the listener rather than providing it keeps
 * `HttpServer` in the output, so a caller reads the bound address from the same
 * `Context`.
 *
 * The program is wrapped in `Effect.interruptible` because `BunHttpServer`
 * serves each request on an uninterruptible fiber. Without the wrapper a client
 * abort and a parked long poll interrupt reach a fiber that cannot take them,
 * so a stopped host would wait out the whole drain bound.
 *
 * Launch it with `Layer.launch`, or merge a projection or an outbox drain
 * beside it and provide the same storage Layer value once.
 */
export const serveLayer = <E>(options: ServeOptions<E>) =>
  HttpServer.serve(Effect.interruptible(app(options))).pipe(
    Layer.provide(options.layer),
    Layer.provideMerge(listenerLayer(options)),
  );

/** A started host that reports its bound address and stops on request. */
export interface RunningHost {
  readonly port: number;
  readonly url: string;
  /** Closes the host's scope, which drains and stops the listener. */
  readonly stop: Effect.Effect<void>;
}

/** The scoped fiber that builds one host. Kept so `serveScoped` can interrupt it. */
const start = <E>(options: ServeOptions<E>) =>
  Effect.gen(function* () {
    const context = yield* serveLayer(options).pipe(
      Layer.build,
      Effect.provideService(Scope.Scope, yield* Effect.scope),
    );
    return Context.get(context, HttpServer.HttpServer);
  });

/**
 * Start the composition and report its bound address.
 *
 * The host owns one scope, so the caller may stop it early and the returned
 * `stop` stays idempotent. Wrap the call in `Effect.acquireRelease` to tie
 * `stop` to an outer scope. A long-running process uses
 * `Layer.launch(serveLayer(options))` and never needs this shape.
 */
export const serveScoped = <E>(options: ServeOptions<E>): Effect.Effect<RunningHost, E> =>
  Effect.gen(function* () {
    const scope = Scope.makeUnsafe();
    const boot = yield* Effect.forkIn(
      start(options).pipe(Effect.provideService(Scope.Scope, scope)),
      scope,
    );
    const listener = yield* Fiber.join(boot);
    return {
      port: Predicate.isTagged(listener.address, "TcpAddress") ? listener.address.port : 0,
      url: HttpServer.formatAddress(listener.address),
      stop: Effect.catchCause(Scope.close(scope, Exit.void), () => Effect.void),
    };
  });
