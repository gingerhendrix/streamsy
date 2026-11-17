import { Schema } from "effect";

export const Id = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_-]{1,100}$/));

const NonBlank = Schema.String.check(Schema.isPattern(/\S/), Schema.isMaxLength(2000));

export const Stopped = Schema.Struct({ by: NonBlank, reason: NonBlank, at: Schema.String });

export type Stopped = typeof Stopped.Type;

export const Metadata = Schema.Struct({
  prompt: Schema.optionalKey(Schema.String),
  stopped: Schema.optionalKey(Stopped),
});

export const StopRequest = Schema.Struct({ by: NonBlank, reason: NonBlank });

export const ChatRequest = Schema.Struct({
  id: Id,
  messages: Schema.NonEmptyArray(
    Schema.Struct({
      role: Schema.Literals(["user", "assistant", "system"]),
      parts: Schema.Array(
        Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
      ),
    }),
  ),
});

export const Stats = Schema.Struct({ calls: Schema.Finite, interruptions: Schema.Finite });

export const Status = Schema.Struct({ exists: Schema.Boolean, closed: Schema.Boolean });

export const StopReply = Schema.Struct({ alreadyClosed: Schema.Boolean });

export const responsePath = (id: string) => `/responses/${id}`;
