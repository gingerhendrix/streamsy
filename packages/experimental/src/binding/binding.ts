import type {
  AppendStreamOptions,
  ClientAppendResult,
  ClientReadResult,
  JsonValue,
  ReadStreamOptions,
  StreamProtocolClient,
} from "@streamsy/core";
import { sourceAck, streamIdentity, type SourceAck, type StreamIdentity } from "../causal.ts";

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

type Appended = Extract<ClientAppendResult, { status: "appended" }>;

export type BoundAppendResult =
  | (Appended & { readonly ack: SourceAck })
  | Exclude<ClientAppendResult, Appended>;

/** Construct an inert binding. Transport operations remain owned by the fixed client handle. */
export function bindStream(options: BindStreamOptions): StreamBinding {
  return Object.freeze({
    identity: streamIdentity(options.identity.name),
    client: options.client,
    streamId: options.streamId,
  });
}

/** Delegate a read without translating canonical protocol offsets such as `-1` or `now`. */
export function readBoundStream<T extends JsonValue = JsonValue>(
  binding: StreamBinding,
  options?: ReadStreamOptions,
): Promise<ClientReadResult<T>> {
  return binding.client.stream(binding.streamId).read<T>(options);
}

/**
 * Delegate one append and mint an acknowledgement only for a newly appended
 * response carrying its exact offset. Producer duplicates remain explicit.
 */
// oxlint-disable-next-line effecttsgo/async-function -- This exported Promise helper is the documented lightweight binding compatibility API.
export async function appendBoundStream(
  binding: StreamBinding,
  data: Uint8Array | string,
  options?: AppendStreamOptions,
): Promise<BoundAppendResult> {
  const result = await binding.client.stream(binding.streamId).append(data, options);
  if (result.status !== "appended") return result;
  return { ...result, ack: sourceAck(binding.identity, result.offset) };
}
