import { Board, LabelCounts, Transitions } from "../../server/outputs.ts";
import { expect } from "bun:test";
import { Effect, Schema } from "effect";
import type { StreamFamily, StreamId } from "@streamsy/core";

/** Read one output member through its family wire and decode with the output codec. Fixtures fit one page. */
export const readOutput = <A, RD, RE, R>(
  output: {
    readonly family: StreamFamily.Member<unknown, R>["family"];
    readonly ref: (params: { readonly workspaceId: string }) => {
      readonly id: StreamId;
      readonly codec: Schema.Codec<A, string | Uint8Array, RD, RE>;
    };
  },
  workspaceId: string,
) =>
  Effect.gen(function* () {
    const ref = output.ref({ workspaceId });
    const result = yield* (yield* output.family).wire.read(ref.id);
    expect(result.upToDate).toBe(true);
    return yield* Effect.forEach(result.messages, (message) =>
      Schema.decodeEffect(ref.codec)(new TextDecoder().decode(message.data)),
    );
  });

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
