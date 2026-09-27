import { Duration, Effect, Layer } from "effect";
import { BunHttpServer } from "@effect/platform-bun";
import { ShutdownSignal, layerShutdownSignal } from "@streamsy/core/http";

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

/** The Bun listener that keeps a parked long poll alive. */
export const listener = (options: ListenerOptions = {}) =>
  BunHttpServer.layer(listenerOptions(options));

export interface DrainOptions {
  /** Upper bound on the wait for live reads to finish; default 2 seconds. */
  readonly timeout?: Duration.Input;
}

/**
 * Wrap the `HttpRouter.serve(...)` layer so live readers drain cleanly on
 * shutdown. Before the serve layer closes, the drain signals every live read:
 * a parked long poll answers `204` with its offset and cursor, and an open SSE
 * body ends after its last whole batch. It then waits until no live read is
 * running, or `timeout` passes. After that the serve layer closes as before.
 *
 * Apply it directly to the serve layer, before providing storage and the
 * listener, so the drain closes first:
 *
 * ```ts
 * HttpRouter.serve(routes).pipe(
 *   drainOnShutdown(),
 *   Layer.provide(storage),
 *   Layer.provideMerge(listener()),
 * );
 * ```
 */
export const drainOnShutdown =
  (options: DrainOptions = {}) =>
  <A, E, R>(serve: Layer.Layer<A, E, R>): Layer.Layer<A, E, Exclude<R, ShutdownSignal>> => {
    const timeout = options.timeout ?? Duration.seconds(2);
    // Built after `serve`, so its finalizer runs before `serve` interrupts the handlers.
    const trigger = Layer.effectDiscard(
      Effect.gen(function* () {
        const signal = yield* ShutdownSignal;
        yield* Effect.addFinalizer(() => signal.drain(timeout));
      }),
    );
    return trigger.pipe(Layer.provideMerge(serve), Layer.provide(layerShutdownSignal));
  };
