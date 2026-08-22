import type { StreamBatch, StreamId, StreamOffset, StreamProtocolClient } from "@streamsy/core";

const CONTENT_TYPE = "application/json";
const decoder = new TextDecoder();

/** `snapshot`, when present, must be JSON-serializable. */
export interface Checkpoint<State> {
  cursors: Record<StreamId, StreamOffset>;
  appliedThrough?: StreamOffset;
  snapshot?: State;
}

export interface CheckpointStore<State> {
  load(viewId: string): Promise<Checkpoint<State> | null>;
  /** Persists the complete checkpoint as one atomic stream append. */
  save(viewId: string, checkpoint: Checkpoint<State>): Promise<void>;
}

export interface StreamCheckpointStoreOptions {
  client: StreamProtocolClient;
  streamId?: (viewId: string) => StreamId;
}

function defaultStreamId(viewId: string): StreamId {
  return `__streamsy/views/${encodeURIComponent(viewId)}/checkpoint`;
}

/**
 * A replay-tolerant checkpoint store where the last appended complete record wins.
 *
 * `State` must be JSON-serializable. Loading is fail-fast on malformed content
 * and costs O(saves); future stream compaction is the planned way to bound that
 * cost. Loading is last-write-wins, not max-cursor-wins.
 */
export function streamCheckpointStore<State>(
  options: StreamCheckpointStoreOptions,
): CheckpointStore<State> {
  const idFor = options.streamId ?? defaultStreamId;

  return {
    async load(viewId) {
      const handle = options.client.stream(idFor(viewId));
      const read = await handle.read({ live: false });
      if (read.status === "not-found" || read.status === "gone") return null;
      if (read.status !== "ok") {
        throw new Error(`Cannot load checkpoint for ${viewId}: read status is ${describe(read)}`);
      }

      let latest: Checkpoint<State> | null = null;
      try {
        for await (const batch of read.session) {
          for (const checkpoint of checkpointsFrom<State>(batch)) latest = checkpoint;
        }
      } catch (error) {
        read.session.cancel(error);
        throw new Error(`Cannot load checkpoint for ${viewId}: latest record is malformed`, {
          cause: error,
        });
      }

      const end = await read.session.done;
      if (end.status !== "done") {
        if (end.status === "error" && end.code === "parse-error") {
          throw new Error(`Cannot load checkpoint for ${viewId}: latest record is malformed`, {
            cause: end.cause,
          });
        }
        throw new Error(`Cannot load checkpoint for ${viewId}: session status is ${describe(end)}`);
      }
      return latest;
    },

    async save(viewId, checkpoint) {
      const handle = options.client.stream(idFor(viewId));
      const created = await handle.create({ contentType: CONTENT_TYPE });
      if (created.status !== "created" && created.status !== "conflict") {
        throw new Error(
          `Cannot create checkpoint stream for ${viewId}: status is ${describe(created)}`,
        );
      }

      const appended = await handle.append(JSON.stringify(checkpoint), {
        contentType: CONTENT_TYPE,
      });
      if (appended.status !== "appended") {
        throw new Error(
          `Cannot save checkpoint for ${viewId}: append status is ${describe(appended)}`,
        );
      }
    },
  };
}

function* checkpointsFrom<State>(batch: StreamBatch): Iterable<Checkpoint<State>> {
  if (batch.kind === "json") {
    for (const item of batch.items) yield decodeCheckpoint<State>(item);
    return;
  }
  const text = batch.kind === "text" ? batch.text : decoder.decode(batch.data);
  const parsed: unknown = JSON.parse(text);
  yield decodeCheckpoint<State>(parsed);
}

function decodeCheckpoint<State>(value: unknown): Checkpoint<State> {
  if (!isCheckpoint(value)) throw new TypeError("Checkpoint record has an invalid shape");
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- State is caller-owned; the persisted checkpoint structure is validated and snapshot remains the documented generic JSON trust boundary.
  return value as Checkpoint<State>;
}

function isCheckpoint(value: unknown): value is Checkpoint<unknown> {
  if (!isRecord(value) || !isRecord(value.cursors)) return false;
  if (!Object.values(value.cursors).every((cursor) => typeof cursor === "string")) return false;
  if (value.appliedThrough !== undefined && typeof value.appliedThrough !== "string") return false;
  return true;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function describe(result: { status: string; code?: string }): string {
  return result.code === undefined ? result.status : `${result.status} (${result.code})`;
}
