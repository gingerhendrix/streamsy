import { Schema } from "effect";
import { Response } from "effect/ai";

/** The native Effect AI wire format; a stop is a finish with metadata.stopped. */
export const ResponseEntry = Response.StreamPart();

export type ResponseEntry = typeof ResponseEntry.Type;

export const Stopped = Schema.Struct({
  by: Schema.String,
  reason: Schema.String,
  at: Schema.String,
});

export type Stopped = typeof Stopped.Type;

export const usage = new Response.Usage({
  inputTokens: {
    uncached: undefined,
    total: undefined,
    cacheRead: undefined,
    cacheWrite: undefined,
  },
  outputTokens: { total: undefined, text: undefined, reasoning: undefined },
});

export const finish = (stopped?: Stopped): ResponseEntry =>
  Response.makePart("finish", {
    reason: stopped ? "other" : "stop",
    usage,
    response: undefined,
    metadata: stopped ? { stopped: { ...stopped } } : {},
  });

/** Preserve Effect AI's native omission of undefined fields at the JSON boundary. */
export const encodeEntry = (part: ResponseEntry): Schema.Json =>
  Schema.decodeUnknownSync(Schema.Json)(
    JSON.parse(JSON.stringify(Schema.encodeSync(ResponseEntry)(part))),
  );
