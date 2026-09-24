import { Board, LabelCounts, Transitions } from "../../server/outputs.ts";
import { Effect, Option, Stream } from "effect";
import { StreamFamily, type AppendOptions, type Batch } from "@streamsy/core";

/** Bind the output member's codec to its existing family wire, retaining its owner guard. */
const memberService = <A, R>(member: StreamFamily.Member<A, R>) =>
  Effect.map(member.family, ({ wire }) =>
    StreamFamily.bind(
      {
        _tag: "StreamRoute",
        template: "",
        match: (id: string) => id === member.id,
        parse: (id: string) => (id === member.id ? Option.some({}) : Option.none()),
        ref: () => member,
      },
      { reader: wire, writer: wire },
    ),
  );
export const append = <A, R>(
  member: StreamFamily.Member<A, R>,
  values: ReadonlyArray<A>,
  options?: Omit<AppendOptions, "data" | "contentType">,
) => Effect.flatMap(memberService(member), (service) => service.append({}, values, options));
export const read = <A, R>(member: StreamFamily.Member<A, R>) =>
  Stream.unwrap(Effect.map(memberService(member), (service) => service.read({})));
export const items = <A, E, R>(batches: Stream.Stream<Batch<A>, E, R>) =>
  batches.pipe(Stream.flatMap((batch) => Stream.fromIterable(batch.items)));

export const patchOutputAppends =
  (patch: (append: typeof Board.Service.wire.append) => typeof Board.Service.wire.append) =>
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
