import { Schema } from "effect";
import { defineMessageUnion } from "foldkit/message";
import { ReadBatch } from "@streamsy/durable-response-contract";

export const Message = defineMessageUnion({
  ReceivedBatch: { batch: ReadBatch },
  FailedRead: { error: Schema.String },
  ChangedPrompt: { value: Schema.String },
  SubmittedPrompt: {},
  ClickedDrop: {},
  ClickedReconnect: {},
  ClickedStop: {},
  ReceivedStopReply: { alreadyClosed: Schema.Boolean },
  FailedWrite: { error: Schema.String },
  ReceivedModelCalls: { calls: Schema.Int },
  SavedResume: {},
  Navigated: {},
});

export type Message = typeof Message.Type;
