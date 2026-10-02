import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, Transaction, type ProjectionKey } from "@streamsy/projection";
import * as BunStorage from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Fiber, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { tracker, outputStreams } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { routes } from "./streams.ts";

export const streams = Streamsy.group(...Object.values(routes), ...outputStreams);
export const hostLayer = (filename: string) => {
  const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(BunStorage.layer(streams, { client: { filename } })),
  );
  return Layer.effectContext(
    Effect.context<
      | CheckpointStore
      | Transaction
      | SqlClient.SqlClient
      | Streamsy.Services<typeof streams>
      | Streamsy.ChangeFeed<Streamsy.Services<typeof streams>>
    >().pipe(
      Effect.map(
        Context.pick(
          CheckpointStore,
          Transaction,
          SqlClient.SqlClient,
          ...streams.streams,
          ...streams.streams.map((stream) =>
            Streamsy.ChangeFeed<Streamsy.Services<typeof streams>>(stream),
          ),
        ),
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

/** Log how a watcher ended, in a fiber that the layer scope owns. */
const watchEnded =
  (key: ProjectionKey) =>
  <A, E>(fiber: Fiber.Fiber<A, E>) =>
    Fiber.await(fiber).pipe(
      Effect.flatMap((exit) =>
        Exit.isSuccess(exit)
          ? Effect.logInfo("projection watcher ended successfully", key, exit)
          : !Cause.hasInterruptsOnly(exit.cause)
            ? Effect.logError("projection watcher ended", key, exit)
            : Effect.void,
      ),
      Effect.forkScoped,
    );

export const applicationLayer = (filename: string, workspaces: ReadonlyArray<string> = []) =>
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
        yield* Projection.watch(rows).pipe(Effect.flatMap(watchEnded(Projection.key(rows))));
        yield* Projection.watch(outputs).pipe(Effect.flatMap(watchEnded(Projection.key(outputs))));
      }
    }),
  ).pipe(Layer.provideMerge(hostLayer(filename)));

/** Create declared inputs; a mismatched existing config stops startup. */
export const createInputs = (workspaceId: string) =>
  Effect.gen(function* () {
    const inputs = yield* Effect.all(Object.values(routes));
    for (const stream of inputs) yield* stream.create({ workspaceId });
  });
