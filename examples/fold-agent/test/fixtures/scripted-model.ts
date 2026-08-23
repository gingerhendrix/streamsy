/**
 * A deterministic LanguageModel for the tests: a fixed script of turns replayed
 * through Fold's `customModel` seam. Provider-free CI is the point — every
 * durability claim in this example is provable without an API key.
 *
 * Modelled on Fold's own scripted test model, trimmed to what this example needs.
 */
import { customModel, type ActiveModel, type FoldModel } from "@humanlayer/fold-core";
import { Effect, Ref, Stream } from "effect";
import { LanguageModel, type Response } from "effect/unstable/ai";

/** One scripted model response: a sequence of encoded provider stream parts. */
export type ScriptedTurn = { readonly parts: ReadonlyArray<Response.StreamPartEncoded> };

const finishPart = (reason: Response.FinishReason): Response.StreamPartEncoded => ({
  type: "finish",
  reason,
  response: undefined,
  usage: {
    inputTokens: { uncached: undefined, total: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: undefined, reasoning: undefined },
  },
});

/** A turn where the model streams one block of text and stops. */
export const textTurn = (text: string): ScriptedTurn => ({
  parts: [
    { type: "text-start", id: "text-1" },
    { type: "text-delta", id: "text-1", delta: text },
    { type: "text-end", id: "text-1" },
    finishPart("stop"),
  ],
});

/** A turn where the model requests one tool call. */
export const toolCallTurn = (id: string, name: string, params: unknown): ScriptedTurn => ({
  parts: [
    { type: "tool-call", id, name, params, providerExecuted: false },
    finishPart("tool-calls"),
  ],
});

/** The `ActiveModel` snapshot the scripted model records in the durable log. */
export const scriptedActiveModel: ActiveModel = {
  providerId: "scripted",
  providerKind: "openai-compatible",
  modelId: "scripted-model",
  role: null,
  requestedReasoningLevel: "off",
  reasoning: { _tag: "disabled" },
};

export interface ScriptedModel {
  readonly model: FoldModel;
  /** Turns not yet consumed; assert the script was fully used. */
  readonly remainingTurns: Effect.Effect<number>;
}

/** Build a `FoldModel` that replays `turns` in order, one per model request. */
export const scriptedModel = (turns: ReadonlyArray<ScriptedTurn>): Effect.Effect<ScriptedModel> =>
  Effect.gen(function* () {
    const turnsRef = yield* Ref.make<ReadonlyArray<ScriptedTurn>>(turns);

    const make = LanguageModel.make({
      generateText: () => Effect.die(new Error("scripted model supports streamText only")),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            const remaining = yield* Ref.get(turnsRef);
            const turn = remaining[0];
            if (turn === undefined) {
              return yield* Effect.die(new Error("scripted model: script exhausted"));
            }
            yield* Ref.set(turnsRef, remaining.slice(1));
            return Stream.fromIterable(turn.parts);
          }),
        ),
    });

    return {
      model: customModel({ activeModel: scriptedActiveModel, make }),
      remainingTurns: Ref.get(turnsRef).pipe(Effect.map((remaining) => remaining.length)),
    };
  });
