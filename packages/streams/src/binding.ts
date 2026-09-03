import type { StreamProtocolClient } from "@streamsy/core";
import { streamIdentity, type StreamIdentity } from "./identity.ts";

/** Explicit composition of a mesh identity, fixed client, and application stream id. */
export interface StreamBinding {
  readonly identity: StreamIdentity;
  readonly client: StreamProtocolClient;
  readonly streamId: string;
}

export interface BindStreamOptions {
  readonly identity: StreamIdentity;
  readonly client: StreamProtocolClient;
  readonly streamId: string;
}

/** Construct an inert binding. Transport operations remain owned by the fixed client handle. */
export function bindStream(options: BindStreamOptions): StreamBinding {
  return Object.freeze({
    identity: streamIdentity(options.identity.name),
    client: options.client,
    streamId: options.streamId,
  });
}
