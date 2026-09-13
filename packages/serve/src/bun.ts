import { Context, Effect, Exit, Fiber, Layer, Scope } from "effect";
import type { Duration } from "effect";
import { HttpServer } from "effect/unstable/http";
import { NetAddress } from "effect/unstable/net";
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
  /**
   * Bounds how long a stop waits for in-flight requests. When the option is
   * absent the drain is unbounded, which is the default shape.
   */
  readonly gracefulShutdownTimeout?: Duration.Input;
}

export const DEFAULT_PORT = 3000;
export const DEFAULT_HOSTNAME = "127.0.0.1";

interface ListenerOptions {
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

/** The Bun listener that keeps a parked long poll alive. */
export const listener = (options: ListenerOptions) =>
  // `BunHttpServer.layer` gained an `Error.ServeError` channel in rc.115; at
  // rc.112 a `Bun.serve` bind failure was a defect. `Layer.orDie` restores that
  // shape so the host keeps one caller-visible error channel, storage's `E`,
  // and a bind failure stays a defect. `@effect/platform-bun`'s own `layerTest`
  // absorbs the same channel the same way.
  BunHttpServer.layer(listenerOptions(options)).pipe(Layer.orDie);

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
export const layer = <E>(options: ServeOptions<E>) =>
  HttpServer.serve(Effect.interruptible(app(options))).pipe(
    Layer.provide(options.layer),
    Layer.provideMerge(listener(options)),
  );

/** A started host that reports its bound address and stops on request. */
export interface Host {
  readonly port: number;
  readonly url: string;
  /** Closes the host's scope, which drains and stops the listener. */
  readonly stop: Effect.Effect<void>;
}

/** Build the composition inside one given scope and return the bound server. */
const buildHost = <E>(options: ServeOptions<E>) =>
  Effect.gen(function* () {
    const context = yield* layer(options).pipe(
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
 * `Layer.launch(layer(options))` and never needs this shape.
 */
export const start = <E>(options: ServeOptions<E>): Effect.Effect<Host, E> =>
  Effect.gen(function* () {
    const scope = Scope.makeUnsafe();
    const boot = yield* Effect.forkIn(
      buildHost(options).pipe(Effect.provideService(Scope.Scope, scope)),
      scope,
    );
    const server = yield* Fiber.join(boot);
    return {
      // rc.115 widens `HttpServer.address` to `NetAddress.SocketAddress`, whose
      // inet tags are `InetAddressV4` and `InetAddressV6`; rc.112 used the single
      // `TcpAddress` tag. Only an inet address reports a TCP port.
      port: NetAddress.isInetAddress(server.address) ? server.address.port : 0,
      url: HttpServer.formatAddress(server.address),
      stop: Effect.catchCause(Scope.close(scope, Exit.void), () => Effect.void),
    };
  });
