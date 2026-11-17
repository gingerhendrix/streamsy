import { Array, Cause, Context, Effect, Exit, Layer, Ref, Schedule, Stream } from "effect";
import { LanguageModel, type Response } from "effect/ai";
import { finish, ResponseEntry } from "@streamsy/durable-response-contract";
import { Schema } from "effect";

const encodedFinish = Schema.encodeSync(ResponseEntry)(finish());

export const chunks = [
  "A durable ",
  "response keeps ",
  "the model running ",
  "when a reader ",
  "disconnects. ",
  "Reconnect from ",
  "the saved offset ",
  "to read each ",
  "record once. ",
  "Stop ends it for everyone.",
];

export class ModelStats extends Context.Service<
  ModelStats,
  {
    readonly read: Effect.Effect<{ calls: number; interruptions: number }>;
  }
>()("DurableResponse/ModelStats") {}

/** A native LanguageModel provider hook, paced so disconnect and stop are observable. */
export const scriptedModel = (paceMs = 250) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const interruptions = yield* Ref.make(0);

      const parts: ReadonlyArray<Response.StreamPartEncoded> = [
        { type: "text-start", id: "answer" },
        ...Array.map(chunks, (delta): Response.StreamPartEncoded => ({
          type: "text-delta",
          id: "answer",
          delta,
        })),
        { type: "text-end", id: "answer" },
        encodedFinish,
      ];

      const model = yield* LanguageModel.make({
        generateText: () => Effect.die("This demo uses streamText"),
        streamText: () =>
          Stream.unwrap(
            Ref.update(calls, (n) => n + 1).pipe(
              Effect.as(
                Stream.fromIterable(parts).pipe(
                  Stream.schedule(Schedule.spaced(`${paceMs} millis`)),
                  Stream.onExit((exit) =>
                    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
                      ? Ref.update(interruptions, (n) => n + 1)
                      : Effect.void,
                  ),
                ),
              ),
            ),
          ),
      });

      return Layer.mergeAll(
        Layer.succeed(LanguageModel.LanguageModel, model),
        Layer.succeed(ModelStats, {
          read: Effect.all({ calls: Ref.get(calls), interruptions: Ref.get(interruptions) }),
        }),
      );
    }),
  );
