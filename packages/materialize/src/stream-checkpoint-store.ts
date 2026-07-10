import { ZERO_OFFSET } from "@streamsy/core";
import type { Offset, StreamId, StreamProtocolFactory } from "@streamsy/core";

const CONTENT_TYPE = "application/json";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface Checkpoint<State> {
  cursors: Record<StreamId, Offset>;
  appliedThrough?: Offset;
  snapshot?: State;
}

export interface CheckpointStore<State> {
  load(viewId: string): Promise<Checkpoint<State> | null>;
  /** Persists the complete checkpoint as one atomic stream append. */
  save(viewId: string, checkpoint: Checkpoint<State>): Promise<void>;
}

export interface StreamCheckpointStoreOptions {
  protocol: StreamProtocolFactory;
  streamId?: (viewId: string) => StreamId;
}

function defaultStreamId(viewId: string): StreamId {
  return `__streamsy/views/${encodeURIComponent(viewId)}/checkpoint`;
}

/** A replay-tolerant checkpoint store where the latest complete record wins. */
export function streamCheckpointStore<State>(
  options: StreamCheckpointStoreOptions,
): CheckpointStore<State> {
  const idFor = options.streamId ?? defaultStreamId;

  return {
    async load(viewId) {
      const result = await options.protocol.get(idFor(viewId));
      if (result.status === "not-found") return null;
      if (result.status !== "ok") {
        throw new Error(`Cannot load checkpoint for ${viewId}: stream status is ${result.status}`);
      }

      const read = await result.stream.read({ offset: ZERO_OFFSET });
      if (read.status !== "ok") {
        throw new Error(`Cannot load checkpoint for ${viewId}: read status is ${read.status}`);
      }
      const latest = read.messages.at(-1);
      if (!latest) return null;
      return JSON.parse(decoder.decode(latest.data)) as Checkpoint<State>;
    },

    async save(viewId, checkpoint) {
      const streamId = idFor(viewId);
      const created = await options.protocol.create(streamId, { contentType: CONTENT_TYPE });
      if (created.status !== "created" && created.status !== "exists") {
        throw new Error(
          `Cannot create checkpoint stream for ${viewId}: status is ${created.status}`,
        );
      }

      const appended = await created.stream.append({
        contentType: CONTENT_TYPE,
        data: encoder.encode(JSON.stringify(checkpoint)),
      });
      if (appended.status !== "appended") {
        throw new Error(
          `Cannot save checkpoint for ${viewId}: append status is ${appended.status}`,
        );
      }
    },
  };
}
