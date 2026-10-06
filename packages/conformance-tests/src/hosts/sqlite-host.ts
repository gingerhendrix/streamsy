/**
 * A Streamsy host over SQLite through `node:sqlite`, shared by the Node and
 * Deno entries. It serves the wildcard group with `Serve.layer`, so a stop
 * drains live reads, and prints one JSON line with the bound URL.
 */
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Http, Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { SqlStorage } from "@streamsy/storage";
import { Config, Effect, Layer, Option } from "effect";
import { HttpServer } from "effect/http";

export const HostConfig = Config.all({
  // Zero asks the runtime for a free port, which `Config.Port` does not accept.
  port: Config.Int("PORT").pipe(Config.withDefault(0)),
  filename: Config.String("SQLITE_PATH"),
  longPollTimeoutMs: Config.Int("LONG_POLL_TIMEOUT_MS").pipe(Config.withDefault(1500)),
  sseDeadlineMs: Config.Int("SSE_DEADLINE_MS").pipe(Config.option),
});

export type HostConfig = typeof HostConfig extends Config.Config<infer A> ? A : never;

const streams = Streamsy.group(Streamsy.Wildcard);

// `clientLayer` gives the client and the storage boundary one Reactivity.
const clientLayer = (filename: string, beforeOpen: Effect.Effect<void>) =>
  SqlStorage.clientLayer(beforeOpen.pipe(Effect.andThen(SqliteClient.make({ filename }))));

// Built after the serve layer, so the URL is printed once the routes answer.
const announce = Layer.effectDiscard(
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;
    const listening = HttpServer.formatAddress(server.address);
    yield* Effect.sync(() => console.log(JSON.stringify({ listening })));
  }),
);

export interface HostOptions<A, E> {
  readonly listener: (config: HostConfig) => Layer.Layer<A, E>;
  /** Runs before the SQLite client opens its database. */
  readonly beforeOpen?: Effect.Effect<void>;
}

export const hostLayer = <A, E>(options: HostOptions<A, E>) =>
  Layer.unwrap(
    Effect.map(HostConfig, (config) => {
      const routes = Http.routes(streams, {
        access: "full",
        ...Option.match(config.sseDeadlineMs, {
          onNone: () => ({}),
          onSome: (sseDeadlineMs) => ({ sseDeadlineMs }),
        }),
      });

      const serve = Serve.layer(routes, { disableLogger: true, disableListenLog: true }).pipe(
        Layer.provide(SqlStorage.layer(streams, { longPollTimeoutMs: config.longPollTimeoutMs })),
        Layer.provide(clientLayer(config.filename, options.beforeOpen ?? Effect.void)),
      );

      return announce.pipe(Layer.provide(serve), Layer.provideMerge(options.listener(config)));
    }),
  );
