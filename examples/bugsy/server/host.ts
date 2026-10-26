import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, Transaction, type ProjectionKey } from "@streamsy/projection";
import { SqlStorage } from "@streamsy/storage";
import { BunStorage } from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Layer, Record, Stream, type Scope } from "effect";
import type { Reactivity } from "effect/reactivity";
import { SqlClient } from "effect/sql";
import { tracker, outputStreams } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { routes } from "./streams.ts";

export const streams = Streamsy.group(...Record.values(routes), ...outputStreams);

/** Root Streamsy storage for `streams` over a SQL client: Postgres, MySQL or SQLite. */
export const sqlStorage = <E>(
  makeClient: Effect.Effect<SqlClient.SqlClient, E, Scope.Scope | Reactivity.Reactivity>,
) => SqlStorage.layer(streams).pipe(Layer.provideMerge(SqlStorage.clientLayer(makeClient)));

/** The storage services the host needs, as `sqlStorage` and `sqliteStorage` give them. */
export type HostStorage = Layer.Success<ReturnType<typeof sqlStorage>>;

/** Bun SQLite storage for `streams` in `filename`. `:memory:` gives a private database. */
export const sqliteStorage = (filename: string) =>
  BunStorage.layer(streams, { client: { filename } });

export const hostLayer = <E>(storage: Layer.Layer<HostStorage, E>) => {
  const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(storage),
  );

  return Layer.effectContext(
    Effect.context<
      CheckpointStore | Transaction | SqlClient.SqlClient | Streamsy.Services<typeof streams>
    >().pipe(
      Effect.map(
        Context.pick(CheckpointStore, Transaction, SqlClient.SqlClient, ...streams.streams),
      ),
    ),
  ).pipe(Layer.provide(host));
};

/** Log a startup failure with its projection key. Interruption still stops startup. */
const startupFailed =
  (key: ProjectionKey) =>
  <E>(cause: Cause.Cause<E>) =>
    Cause.hasInterruptsOnly(cause)
      ? Effect.failCause(cause)
      : Effect.logError("projection startup failed", key, cause);

/** Run a watcher in a fiber that the layer scope owns, and log how it ended. */
const watchEnded =
  (key: ProjectionKey) =>
  <A, E, R>(follower: Stream.Stream<A, E, R>) =>
    Stream.runLast(follower).pipe(
      Effect.exit,
      Effect.flatMap((exit) =>
        Exit.isSuccess(exit)
          ? Effect.logInfo("projection watcher ended successfully", key, exit)
          : !Cause.hasInterruptsOnly(exit.cause)
            ? Effect.logError("projection watcher ended", key, exit)
            : Effect.void,
      ),
      Effect.forkScoped,
    );

export const applicationLayer = <E>(
  storage: Layer.Layer<HostStorage, E>,
  workspaces: ReadonlyArray<string> = [],
) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      yield* prepareSchema;
      yield* Effect.forEach(workspaces, createInputs, { discard: true });

      for (const workspaceId of workspaces) {
        const rows = issueRows.at({ workspaceId });
        const outputs = tracker.at({ workspaceId });
        // Startup and background failures are isolated to their address.
        yield* Projection.run(rows).pipe(Effect.catchCause(startupFailed(Projection.key(rows))));
        yield* Projection.run(outputs).pipe(
          Effect.catchCause(startupFailed(Projection.key(outputs))),
        );
        yield* Projection.follow(rows).pipe(watchEnded(Projection.key(rows)));
        yield* Projection.follow(outputs).pipe(watchEnded(Projection.key(outputs)));
      }
    }),
  ).pipe(Layer.provideMerge(hostLayer(storage)));

/** Create declared inputs; a mismatched existing config stops startup. */
export const createInputs = (workspaceId: string) =>
  Effect.gen(function* () {
    const inputs = yield* Effect.all(Record.values(routes));

    for (const stream of inputs) yield* stream.create({ workspaceId });
  });
