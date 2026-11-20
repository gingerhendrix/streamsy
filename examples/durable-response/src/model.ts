import { Array, Cause, Context, Effect, Exit, Layer, Ref, Schedule, Stream } from "effect";
import { LanguageModel, type Response } from "effect/ai";
import { finish, ResponseEntry } from "@streamsy/durable-response-contract";
import { Schema } from "effect";

const encodedFinish = Schema.encodeSync(ResponseEntry)(finish());

export const chunks = [
  "Think of a durable response as a live transcript. ",
  "The server starts one model call ",
  "and appends each piece of text to a SQLite log ",
  "as the model produces it. ",
  "Your browser reads that log instead of holding ",
  "the only copy of the answer.\n\n",
  "Try dropping the connection after you see a few lines. ",
  "The text in this tab pauses, ",
  "but the model keeps writing on the server. ",
  "Reconnect after a moment and watch the missing text arrive. ",
  "The saved offset tells the server where to resume, ",
  "so the browser can append new text ",
  "without displaying the beginning twice.\n\n",
  "Now open this response in a second tab. ",
  "The new tab reads from the beginning ",
  "and then follows the same log. ",
  "Both readers can be at different offsets ",
  "while the model call count stays at one.\n\n",
  "Stop does something different from disconnecting. ",
  "It interrupts the model and records why the response ended. ",
  "Both tabs keep the text already written, ",
  "and a reader who joins later sees ",
  "the same partial answer and stop reason.",
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
