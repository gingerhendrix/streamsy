import { streamIdentity, streamIdentityEquals, type StreamIdentity } from "./identity.ts";

declare const streamPositionBrand: unique symbol;

/** A real durable-stream position, excluding protocol read sentinels. */
export type StreamPosition = string & { readonly [streamPositionBrand]: true };

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

const FORBIDDEN_POSITION_CHARACTERS = /[,&=?/]/;

/** Validate a real position without interpreting its opaque wire format. */
export function streamPosition(position: string): StreamPosition {
  if (
    typeof position !== "string" ||
    position.length === 0 ||
    position.length >= 256 ||
    position === "-1" ||
    position === "now" ||
    FORBIDDEN_POSITION_CHARACTERS.test(position)
  ) {
    throw new TypeError(`Invalid real stream position: ${String(position)}`);
  }
  return position as StreamPosition;
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
