import { Effect, Schedule, Schema, Stream } from "effect";
import { Subscription } from "foldkit";
import { streamReader } from "@streamsy/durable-response-contract";
import { modelCalls } from "@streamsy/durable-response-contract/client";
import { Message } from "./message.ts";
import type { Resources } from "./resources.ts";
import type { Model } from "./model.ts";

/** Only explicit reconnect changes resumedAt; a batch never restarts its own subscription. */
export const responseMessages = (id: string, offset: string) =>
  streamReader(id, offset).pipe(
    Stream.map((batch) => Message.ReceivedBatch({ batch })),
    Stream.catch((error) => Stream.succeed(Message.FailedRead({ error }))),
  );

export const subscriptions = Subscription.make<Model, Message, Resources>()((entry) => ({
  response: entry(
    { id: Schema.String, active: Schema.Boolean, offset: Schema.String },
    {
      modelToDependencies: (model) => ({
        id: model.id,
        active: model.mode === "connected",
        offset: model.resumedAt,
      }),
      dependenciesToStream: ({ id, active, offset }) =>
        id && active ? responseMessages(id, offset) : Stream.empty,
    },
  ),
  calls: entry(
    {},
    {
      modelToDependencies: () => ({}),
      dependenciesToStream: () =>
        Stream.fromEffect(
          modelCalls.pipe(Effect.map(({ calls }) => Message.ReceivedModelCalls({ calls }))),
        ).pipe(
          Stream.repeat(Schedule.spaced("600 millis")),
          Stream.catch(() => Stream.empty),
        ),
    },
  ),
}));
