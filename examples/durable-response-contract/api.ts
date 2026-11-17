import { Schema } from "effect";

export const Id = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_-]{1,80}$/));

const NonBlank = Schema.String.check(Schema.isPattern(/\S/), Schema.isMaxLength(2000));

export const ChatRequest = Schema.Struct({ prompt: NonBlank, chatId: Id });

export type ChatRequest = typeof ChatRequest.Type;

export const ChatCreated = Schema.Struct({ id: Id, chatId: Id, prompt: Schema.String });

export const StopRequest = Schema.Struct({ by: Id, reason: NonBlank });

export type StopRequest = typeof StopRequest.Type;

export const StopReply = Schema.Struct({ alreadyClosed: Schema.Boolean });

export const ModelCalls = Schema.Struct({ calls: Schema.Int, interruptions: Schema.Int });

export const responsePath = (id: string) => `/responses/${encodeURIComponent(id)}`;

export const pageResponseId = (pathname: string) => {
  const match = /^\/r\/([a-zA-Z0-9_-]{1,80})$/.exec(pathname);

  return match?.[1] ?? "";
};
