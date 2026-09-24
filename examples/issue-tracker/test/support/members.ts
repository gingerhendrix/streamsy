import { Board, LabelCounts, Transitions } from "../../server/outputs.ts";
import { Effect, Stream } from "effect";
import {
  Streams,
  StreamsReader,
  StreamsWriter,
  type StreamFamily,
  type AppendOptions,
} from "@streamsy/core";

/** Fixture I/O follows the member's family, including projection-owned codecs. */
export const append = <A, R>(
  member: StreamFamily.Member<A, R>,
  items: ReadonlyArray<A>,
  options?: Omit<AppendOptions, "data" | "contentType">,
) =>
  Effect.flatMap(member.family, ({ wire }) =>
    Streams.append(member, items, options).pipe(Effect.provideService(StreamsWriter, wire)),
  );
export const read = <A, R>(member: StreamFamily.Member<A, R>) =>
  Stream.unwrap(
    Effect.map(member.family, ({ wire }) =>
      Streams.read(member).pipe(Stream.provideService(StreamsReader, wire)),
    ),
  );
export const items = Streams.items;

export const patchOutputAppends =
  (
    patch: (
      append: import("@streamsy/core").Writer<import("@streamsy/core").StreamsFault>["append"],
    ) => import("@streamsy/core").Writer<import("@streamsy/core").StreamsFault>["append"],
  ) =>
  <A, E, R>(body: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const board = yield* Board;
      const counts = yield* LabelCounts;
      const transitions = yield* Transitions;
      return yield* body.pipe(
        Effect.provideService(Board, {
          ...board,
          wire: { ...board.wire, append: patch(board.wire.append) },
        }),
        Effect.provideService(LabelCounts, {
          ...counts,
          wire: { ...counts.wire, append: patch(counts.wire.append) },
        }),
        Effect.provideService(Transitions, {
          ...transitions,
          wire: { ...transitions.wire, append: patch(transitions.wire.append) },
        }),
      );
    });
