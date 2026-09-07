/** Provider-free deterministic Fold model used by recovery tests and CLI smoke runs. */
import { customModel, type ActiveModel, type FoldModel } from "@humanlayer/fold-core";
import { Effect, Ref, Stream } from "effect";
import { LanguageModel, type Response } from "effect/unstable/ai";

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

export const textTurn = (text: string): ScriptedTurn => ({
  parts: [
    { type: "text-start", id: "text-1" },
    { type: "text-delta", id: "text-1", delta: text },
    { type: "text-end", id: "text-1" },
    finishPart("stop"),
  ],
});

export const toolCallTurn = (id: string, name: string, params: unknown): ScriptedTurn => ({
  parts: [
    { type: "tool-call", id, name, params, providerExecuted: false },
    finishPart("tool-calls"),
  ],
});

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
  readonly remainingTurns: Effect.Effect<number>;
}

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
            if (turn === undefined) return yield* Effect.die(new Error("scripted model exhausted"));
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
