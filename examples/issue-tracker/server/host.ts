import { StreamGroup, Changes } from "@streamsy/core";
import { Checkpoints, State } from "@streamsy/projection";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Fiber, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Projection } from "@streamsy/projection";
import { tracker, outputFamilies, Board, LabelCounts, Transitions } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { refs, routes } from "./streams.ts";

export const families = StreamGroup.make(...Object.values(routes), ...outputFamilies);
export const hostLayer = (filename: string) => {
  const host = ProjectionSqlite.layer.pipe(
    Layer.provideMerge(BunStorage.layer({ client: { filename } }, families)),
  );
  return Layer.effectContext(
    Effect.context<
      | Checkpoints
      | State
      | SqlClient.SqlClient
      | StreamGroup.Services<typeof families>
      | Changes<StreamGroup.Services<typeof families>>
    >().pipe(
      Effect.map(
        Context.pick(
          Checkpoints,
          State,
          SqlClient.SqlClient,
          ...families.families,
          ...families.families.map((family) =>
            Changes<StreamGroup.Services<typeof families>>(family),
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
      yield* createInputs(workspaces.flatMap(refs));
      if (workspaces.length > 0) {
        const members = workspaces.map((workspaceId) => ({ workspaceId }));
        for (const params of members) {
          const sqlMember = issueRows.member(params);
          const outputMember = tracker.member(params);
          // Startup and background failures are isolated to their member.
          yield* Projection.serialized(sqlMember).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("projection startup failed", Projection.key(sqlMember), cause),
            ),
          );
          yield* Projection.serialized(outputMember).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logError("projection startup failed", Projection.key(outputMember), cause),
            ),
          );
          const watched = [
            { key: Projection.key(sqlMember), fiber: yield* Projection.onChange(sqlMember) },
            { key: Projection.key(outputMember), fiber: yield* Projection.onChange(outputMember) },
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
export const createInputs = (inputs: ReturnType<typeof refs>) =>
  Effect.gen(function* () {
    const families = yield* Effect.all(Object.values(routes));
    for (const ref of inputs) {
      const family = families.find((family) => family.route.match(ref.id));
      if (family === undefined) return yield* Effect.die(new RangeError(`Unknown input ${ref.id}`));
      yield* family.wire.create(ref.id, { contentType: ref.contentType });
    }
  });
