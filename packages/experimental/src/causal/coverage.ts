import { streamIdentity, streamIdentityEquals, type StreamIdentity } from "./identity.ts";
import { Schema } from "effect";

const FORBIDDEN_POSITION_CHARACTERS = /[,&=?/]/;

/** A real durable-stream position, excluding protocol read sentinels. */
export const StreamPosition = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(255),
  Schema.makeFilter((position) => position !== "-1" && position !== "now", {
    description: "a real stream position rather than a protocol read sentinel",
  }),
  Schema.makeFilter((position) => !FORBIDDEN_POSITION_CHARACTERS.test(position), {
    description: "a stream position without reserved URL characters",
  }),
).pipe(Schema.brand("StreamPosition"));
export type StreamPosition = typeof StreamPosition.Type;

export interface SourceAck {
  readonly identity: StreamIdentity;
  readonly position: StreamPosition;
}

export interface SourceWatermark {
  readonly identity: StreamIdentity;
  readonly position: StreamPosition;
}

export type Coverage =
  | { readonly status: "proven" }
  | { readonly status: "not-yet" }
  | { readonly status: "incomparable" };

/** Validate a real position without interpreting its opaque wire format. */
export function streamPosition(position: string): StreamPosition {
  return Schema.decodeUnknownSync(StreamPosition)(position);
}

export function compareStreamPositions(a: string, b: string): -1 | 0 | 1 {
  const left = streamPosition(a);
  const right = streamPosition(b);
  return left === right ? 0 : left < right ? -1 : 1;
}

export function sourceAck(identity: StreamIdentity, position: string): SourceAck {
  return Object.freeze({
    identity: streamIdentity(identity.name),
    position: streamPosition(position),
  });
}

export function sourceWatermark(identity: StreamIdentity, position: string): SourceWatermark {
  return Object.freeze({
    identity: streamIdentity(identity.name),
    position: streamPosition(position),
  });
}

/** Determine whether one source watermark incorporates an acknowledgement. */
export function coverage(watermark: SourceWatermark, ack: SourceAck): Coverage {
  if (!streamIdentityEquals(watermark.identity, ack.identity)) {
    return { status: "incomparable" };
  }
  return compareStreamPositions(watermark.position, ack.position) >= 0
    ? { status: "proven" }
    : { status: "not-yet" };
}
