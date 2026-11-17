import { SecureRandom } from "effect/crypto";
import { Effect, Schema } from "effect";
import { Command, Navigation } from "foldkit";
import { persist, ResumeSnapshot } from "@streamsy/durable-response-contract";
import { startChat, stopResponse } from "@streamsy/durable-response-contract/client";
import { Message } from "./message.ts";

export const Start = Command.define("Start", {
  args: { prompt: Schema.String },
  messages: [Message.Navigated, Message.FailedWrite],
  execute: ({ prompt }) =>
    Effect.gen(function* () {
      const chatId = yield* (yield* SecureRandom.SecureRandom).uuidV4();
      const { id } = yield* startChat({ prompt, chatId });
      yield* Navigation.load(`/r/${id}`);

      return Message.Navigated();
    }).pipe(Effect.orElseSucceed(() => Message.FailedWrite({ error: "Unable to start response" }))),
});

export const Stop = Command.define("Stop", {
  args: { id: Schema.String, reader: Schema.String },
  messages: [Message.ReceivedStopReply, Message.FailedWrite],
  execute: ({ id, reader }) =>
    stopResponse(id, { by: reader, reason: "Stopped from browser" }).pipe(
      Effect.map(Message.ReceivedStopReply),
      Effect.orElseSucceed(() => Message.FailedWrite({ error: "Unable to stop response" })),
    ),
});

export const Save = Command.define("Save", {
  args: { id: Schema.String, snapshot: ResumeSnapshot },
  messages: [Message.SavedResume],
  execute: ({ id, snapshot }) => persist(id, snapshot).pipe(Effect.as(Message.SavedResume())),
});
