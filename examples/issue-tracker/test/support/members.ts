import { Board, LabelCounts, Transitions } from "../../server/outputs.ts";
import { Effect } from "effect";

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
