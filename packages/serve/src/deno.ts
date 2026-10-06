import { Effect, type Duration } from "effect";
import { DenoHttpServer } from "@effect/platform-deno";

export const DEFAULT_PORT = 3000;

export const DEFAULT_HOSTNAME = "127.0.0.1";

export interface ListenerOptions {
  readonly port?: number;
  readonly hostname?: string;
  readonly gracefulShutdownTimeout?: Duration.Input;
}

const listenerOptions = (options: ListenerOptions) => ({
  port: options.port ?? DEFAULT_PORT,
  hostname: options.hostname ?? DEFAULT_HOSTNAME,
  // `Deno.serve` has no idle or response timeout that cuts a parked long poll
  // or an SSE body. The shutdown rule is the same as the Bun listener: the
  // preemptive close comes back only when the caller sets a bound.
  ...(options.gracefulShutdownTimeout === undefined
    ? { disablePreemptiveShutdown: true }
    : { gracefulShutdownTimeout: options.gracefulShutdownTimeout }),
});

/** The Deno listener for a Streamsy host. */
export const listener = (options: ListenerOptions = {}) =>
  DenoHttpServer.layer(listenerOptions(options));

interface LoadExtensionPrototype {
  enableLoadExtension?: (allow: boolean) => void;
}

/**
 * Lets `@effect/sql-sqlite-node` open a database on Deno.
 *
 * The driver calls `enableLoadExtension(false)` on every open. Deno's
 * `node:sqlite` `DatabaseSync` has no such method, so the open throws. This
 * adds a no-op method when it is missing and leaves a runtime that has it
 * alone. Run it before the first `SqliteClient.make`:
 *
 * ```ts
 * const client = Deno.nodeSqliteCompat.pipe(
 *   Effect.andThen(SqliteClient.make({ filename })),
 * );
 * ```
 */
export const nodeSqliteCompat: Effect.Effect<void> = Effect.promise(
  () => import("node:sqlite"),
).pipe(
  Effect.flatMap(({ DatabaseSync }) =>
    Effect.sync(() => {
      const prototype: LoadExtensionPrototype = DatabaseSync.prototype;

      if (prototype.enableLoadExtension === undefined) {
        prototype.enableLoadExtension = () => {};
      }
    }),
  ),
);
