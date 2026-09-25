import { Streamsy } from "@streamsy/core";
import { Checkpoints, State } from "@streamsy/projection";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Fiber, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Projection } from "@streamsy/projection";
import { tracker, outputFamilies, Board, LabelCounts, Transitions } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { routes } from "./streams.ts";

export const streams = Streamsy.group(...Object.values(routes), ...outputFamilies);
export const hostLayer = (filename: string) => {
  const host = ProjectionSqlite.layer.pipe(
    Layer.provideMerge(BunStorage.layer({ client: { filename } }, streams)),
  );
  return Layer.effectContext(
    Effect.context<
      | Checkpoints
      | State
      | SqlClient.SqlClient
      | Streamsy.Services<typeof streams>
      | Streamsy.ChangeFeed<Streamsy.Services<typeof streams>>
    >().pipe(
      Effect.map(
        Context.pick(
          Checkpoints,
          State,
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
          const sqlMember = issueRows.member(params);
          const outputMember = tracker.member(params);
          // Startup and background failures are isolated to their address.
          yield* Projection.run(sqlMember).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("projection startup failed", Projection.key(sqlMember), cause),
            ),
          );
          yield* Projection.run(outputMember).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("projection startup failed", Projection.key(outputMember), cause),
            ),
          );
          const watched = [
            { key: Projection.key(sqlMember), fiber: yield* Projection.watch(sqlMember) },
            { key: Projection.key(outputMember), fiber: yield* Projection.watch(outputMember) },
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
      | State
      | Checkpoints
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
