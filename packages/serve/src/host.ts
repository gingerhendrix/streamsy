import { Cause, Duration, Effect, Exit, Layer, Runtime } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { ShutdownSignal, layerShutdownSignal } from "@streamsy/core/http";

type DefaultServeRequires<R> =
  | HttpRouter.Request.Only<"Requires", R>
  | HttpRouter.Request.Only<"GlobalRequires", R>;

/** The options of `HttpRouter.serve`, plus the drain bound. */
export type LayerOptions<R = never, HE = never, HR = DefaultServeRequires<R>> = NonNullable<
  Parameters<typeof HttpRouter.serve<unknown, unknown, R, HE, HR>>[1]
> & {
  /** Upper bound on the wait for live reads to finish; default 2 seconds. */
  readonly drainTimeout?: Duration.Input | undefined;
};

// Built after `serve`, so its finalizer runs before `serve` interrupts the handlers.
const drain =
  (timeout: Duration.Input = Duration.seconds(2)) =>
  <A, E, R>(serve: Layer.Layer<A, E, R>): Layer.Layer<A, E, Exclude<R, ShutdownSignal>> => {
    const trigger = Layer.effectDiscard(
      Effect.gen(function* () {
        const signal = yield* ShutdownSignal;
        yield* Effect.addFinalizer(() => signal.drain(timeout));
      }),
    );
    return trigger.pipe(Layer.provideMerge(serve), Layer.provide(layerShutdownSignal));
  };

/**
 * `HttpRouter.serve` with the Streamsy drain applied in the only correct place.
 *
 * Before the serve layer closes, the drain signals every live read: a parked
 * long poll answers `204` with its offset and cursor, and an open SSE body ends
 * after its last whole batch. It then waits until no live read is running, or
 * `drainTimeout` passes. After that the serve layer interrupts any handler that
 * is still running.
 *
 * The host provides storage, the listener and the Effect main:
 *
 * ```ts
 * const Server = Serve.layer(routes).pipe(
 *   Layer.provide(MemoryStorage.layer(streams)),
 *   Layer.provide(listener()),
 * );
 * BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });
 * ```
 */
export const layer = <A, E, R, HE = never, HR = DefaultServeRequires<R>>(
  routes: Layer.Layer<A, E, R>,
  options?: LayerOptions<R, HE, HR>,
) => HttpRouter.serve(routes, options).pipe(drain(options?.drainTimeout));

/**
 * Exit policy for a long-running Streamsy host after `Serve.layer` drains.
 * Pass it to the runtime's `runMain` as `{ teardown: Serve.teardown }`.
 *
 * An exit that holds only interrupts maps to 0: after the drain, an interrupt
 * from `SIGINT` or `SIGTERM` is the normal way for the server to stop. Every
 * other exit uses `Runtime.defaultTeardown`, so a startup failure stays
 * non-zero. Any interrupt-only exit maps to 0, including one that no signal
 * caused. In a `Layer.launch` main the fiber ends only on failure or a signal.
 */
export const teardown: Runtime.Teardown = (exit, onExit) =>
  Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
    ? onExit(0)
    : Runtime.defaultTeardown(exit, onExit);
