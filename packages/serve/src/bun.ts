import type { Duration } from "effect";
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

/** The Bun listener that keeps a parked long poll alive. */
export const listener = (options: ListenerOptions = {}) =>
  BunHttpServer.layer(listenerOptions(options));
