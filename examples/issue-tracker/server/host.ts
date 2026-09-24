import { provides, StreamsReader } from "@streamsy/core";
import type { Host, State } from "@streamsy/projection";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Fiber, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Projection } from "@streamsy/projection";
import { tracker, outputFamilies, Board, LabelCounts, Transitions } from "./outputs.ts";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { refs, routes } from "./streams.ts";

export const hostLayer = (filename: string) => {
  const protocol = ProjectionSqlite.layer.pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })),
  );
  return provides(protocol, ...Object.values(routes), ...outputFamilies).pipe(
    Layer.provideMerge(protocol),
  );
};

/** Stage 1 adapter: projection runners still use the protocol Host internally. */
export class ProjectionRunner extends Context.Service<
  ProjectionRunner,
  {
    readonly run: (
      workspaceId: string,
    ) => Effect.Effect<
      void,
      | import("@streamsy/projection").ProjectionFault
      | import("effect/unstable/sql/SqlError").SqlError
      | import("./catalog.ts").CatalogWorkspaceMismatch
    >;
  }
>()("IssueTracker/ProjectionRunner") {}

const runnerLayer = Layer.effect(
  ProjectionRunner,
  Effect.gen(function* () {
    const context = yield* Effect.context<Host | State | SqlClient.SqlClient>();
    return {
      run: (workspaceId: string) =>
        Effect.gen(function* () {
          yield* Projection.serialized(issueRows.member({ workspaceId }));
          yield* Projection.serialized(tracker.member({ workspaceId }));
        }).pipe(Effect.provide(context)),
    };
  }),
);

/** Serve still requires a protocol reader until stage 2; each adapter supplies only its family wire. */
export const boardReader = Layer.effect(
  StreamsReader,
  Effect.map(Board, (family) => family.wire),
);
export const labelCountsReader = Layer.effect(
  StreamsReader,
  Effect.map(LabelCounts, (family) => family.wire),
);
export const transitionsReader = Layer.effect(
  StreamsReader,
  Effect.map(Transitions, (family) => family.wire),
);

export class ApplicationReady extends Context.Service<ApplicationReady, true>()(
  "IssueTracker/ApplicationReady",
) {}

export const applicationLayer = (filename: string, workspaces: ReadonlyArray<string> = []) => {
  const protocol = hostLayer(filename);
  const base = runnerLayer.pipe(Layer.provideMerge(protocol));
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
      | Host
      | SqlClient.SqlClient
      | ApplicationReady
      | ProjectionRunner
      | import("./streams.ts").events
      | import("./streams.ts").labelEvents
      | import("./streams.ts").projects
      | import("./streams.ts").users
      | import("./streams.ts").labels
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
