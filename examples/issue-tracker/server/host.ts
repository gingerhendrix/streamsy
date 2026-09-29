import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import * as ProjectionSql from "@streamsy/projection/sql";
import * as BunStorage from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Fiber, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Projection } from "@streamsy/projection";
import { tracker, outputStreams, Board, LabelCounts, Transitions } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { routes } from "./streams.ts";

export const streams = Streamsy.group(...Object.values(routes), ...outputStreams);
export const hostLayer = (filename: string) => {
  const host = ProjectionSql.layerStorage.pipe(
    Layer.provideMerge(BunStorage.layer({ client: { filename } }, streams)),
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

export class ApplicationReady extends Context.Service<ApplicationReady, true>()(
  "IssueTracker/ApplicationReady",
) {}

export const applicationLayer = (filename: string, workspaces: ReadonlyArray<string> = []) => {
  const base = hostLayer(filename);
  const ready = Layer.effect(
    ApplicationReady,
    Effect.gen(function* () {
      yield* prepareSchema;
      yield* Effect.forEach(workspaces, createInputs, { discard: true });
      if (workspaces.length > 0) {
        const addresses = workspaces.map((workspaceId) => ({ workspaceId }));
        for (const params of addresses) {
          const rows = issueRows.at(params);
          const outputs = tracker.at(params);
          // Startup and background failures are isolated to their address.
          yield* Projection.run(rows).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("projection startup failed", Projection.key(rows), cause),
            ),
          );
          yield* Projection.run(outputs).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("projection startup failed", Projection.key(outputs), cause),
            ),
          );
          const watched = [
            { key: Projection.key(rows), fiber: yield* Projection.watch(rows) },
            { key: Projection.key(outputs), fiber: yield* Projection.watch(outputs) },
          ];
          yield* Effect.forEach(
            watched,
            ({ key, fiber }) =>
              Fiber.await(fiber).pipe(
                Effect.flatMap((exit) =>
                  Exit.isSuccess(exit)
                    ? Effect.logInfo("projection watcher ended successfully", key, exit)
                    : !Cause.hasInterruptsOnly(exit.cause)
                      ? Effect.logError("projection watcher ended", key, exit)
                      : Effect.void,
                ),
              ),
            { concurrency: "unbounded", discard: true },
          ).pipe(Effect.forkScoped);
        }
      }
      return true as const;
    }),
  ).pipe(Layer.provide(base));
  return Layer.effectContext(
    Effect.context<
      | Transaction
      | CheckpointStore
      | SqlClient.SqlClient
      | ApplicationReady
      | import("./streams.ts").Events
      | import("./streams.ts").LabelEvents
      | import("./streams.ts").Projects
      | import("./streams.ts").Users
      | import("./streams.ts").Labels
      | Board
      | LabelCounts
      | Transitions
    >(),
  ).pipe(Layer.provide(ready.pipe(Layer.provideMerge(base))));
};

/** Create declared inputs; a mismatched existing config stops startup. */
export const createInputs = (workspaceId: string) =>
  Effect.gen(function* () {
    const inputs = yield* Effect.all(Object.values(routes));
    for (const stream of inputs) yield* stream.create({ workspaceId });
  });
