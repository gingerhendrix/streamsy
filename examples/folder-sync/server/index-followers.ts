import { ZERO_OFFSET } from "@streamsy/core";
import { Streams, StreamsReader } from "@streamsy/core/protocol";
import { Projection, ProjectionFault } from "@streamsy/projection";
import { Cause, Duration, Effect, Layer, Schedule, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { afterLastReset, isUpsert } from "./messages.ts";
import { clearIndexFolder, initialiseSearch, search } from "./search-index.ts";
import { Folders } from "./streams.ts";

class FollowerFailed extends Schema.TaggedError<
  FollowerFailed,
  { readonly brand: unique symbol }
>()("FollowerFailed", {}) {}

/** A stopped binding is logged and restarted, including defects from its handler. */
export const supervise = <A, E, R>(name: string, work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      (cause) =>
        Effect.logError(`Index follower ${name} failed; restarting in one second`, cause).pipe(
          Effect.andThen(Effect.fail(new FollowerFailed())),
        ),
    ),
    Effect.andThen(
      Effect.logWarning(`Index follower ${name} stopped; restarting in one second`).pipe(
        Effect.andThen(Effect.fail(new FollowerFailed())),
      ),
    ),
    Effect.retry({ schedule: Schedule.spaced("1 second") }),
  );

export class IndexBootstrapUnavailable extends Schema.TaggedError<
  IndexBootstrapUnavailable,
  { readonly brand: unique symbol }
>()("IndexBootstrapUnavailable", { folderId: Schema.String }) {}

/** Bootstrap failures stay local to one folder and emit one warning during its lifetime. */
export const superviseFolder = <A, E, R>(folderId: string, work: Effect.Effect<A, E, R>) =>
  Effect.suspend(() => {
    let warned = false;

    return supervise(
      folderId,
      work.pipe(
        Effect.tapError((error) => {
          if (!Schema.is(IndexBootstrapUnavailable)(error) || warned) return Effect.void;
          warned = true;

          return Effect.logWarning(
            `Index folder ${folderId} history is unavailable at bootstrap; retrying with backoff up to 60 seconds`,
          );
        }),
        Effect.retry({
          while: Schema.is(IndexBootstrapUnavailable),
          schedule: Schedule.exponential("1 second").pipe(
            Schedule.modifyDelay(({ duration }) =>
              Effect.succeed(Duration.min(duration, Duration.seconds(60))),
            ),
          ),
        }),
      ),
    );
  });

/** A gone cursor rebuilds only this folder. An unavailable bootstrap fails instead of spinning. */
export const runIndex = Effect.fn("Index.run")(function* (folderId: string) {
  const bound = search.at({ folderId });

  return yield* Projection.run(bound).pipe(
    Effect.catchTag("ProjectionFault", (error) =>
      Effect.gen(function* () {
        if (error.reason !== "history-unavailable") return yield* error;

        if ((yield* Projection.read(bound)).offsets.input === ZERO_OFFSET)
          return yield* new IndexBootstrapUnavailable({ folderId });
        const sql = yield* SqlClient.SqlClient;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* clearIndexFolder(folderId);
            yield* Projection.forget(bound);
          }),
        );

        return yield* Projection.run(bound).pipe(
          Effect.mapError((failure) =>
            Schema.is(ProjectionFault)(failure) && failure.reason === "history-unavailable"
              ? new IndexBootstrapUnavailable({ folderId })
              : failure,
          ),
        );
      }),
    ),
  );
});

/** Folder discovery owns one direct source follower per folder. No derived feed. */
export const IndexLive = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    const folders = yield* Folders;
    const scope = yield* Effect.scope;
    const started = new Set<string>();

    const start = Effect.fn("Index.start")(function* (folderId: string) {
      if (started.has(folderId)) return;
      const bound = search.at({ folderId });
      yield* superviseFolder(
        folderId,
        Effect.gen(function* () {
          yield* runIndex(folderId);

          return yield* Stream.runDrain(Projection.follow(bound));
        }),
      ).pipe(Effect.forkIn(scope));
      started.add(folderId);
    });

    // Replay the small registry on every runtime start to restore all folder bindings.
    yield* supervise(
      "folders",
      Streams.follow(Folders.at({})).pipe(
        Stream.runForEach((batch) =>
          Effect.forEach(
            afterLastReset(batch.items),
            (change) => (isUpsert(change) ? start(change.value.folderId) : Effect.void),
            { discard: true },
          ),
        ),
      ),
    ).pipe(Effect.provideService(StreamsReader, folders.wire), Effect.forkIn(scope));
  }),
);
