import { Streams } from "@streamsy/core";
import type { Host, State } from "@streamsy/projection";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Fiber, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Projection } from "@streamsy/projection";
import { tracker } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { refs } from "./streams.ts";

export const hostLayer = (filename: string) =>
  ProjectionSqlite.layer.pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })),
  );

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
    Effect.context<State | Host | SqlClient.SqlClient | ApplicationReady>(),
  ).pipe(Layer.provide(ready.pipe(Layer.provideMerge(base))));
};

/**
 * `Streams.create` already treats an existing stream with the same config as
 * success, so a `CreateConflict` means the stored stream disagrees with the
 * declared ref; that stops the host at startup, where the message names it.
 */
export const createInputs = (inputs: ReadonlyArray<Parameters<typeof Streams.create>[0]>) =>
  Effect.forEach(inputs, (ref) => Streams.create(ref), { discard: true });
