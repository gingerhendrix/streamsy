import { compare } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { Config, Context, Effect, Layer, Schema } from "effect";
import * as decoding from "lib0/decoding";
import * as Y from "yjs";
import { concat, Snapshots, Updates } from "./streams.ts";

/** Bytes of new updates that publish the next snapshot. */
export class CompactionThreshold extends Context.Service<CompactionThreshold, number>()(
  "yjs-editor/CompactionThreshold",
) {
  // A bad threshold is a deploy mistake, so it stops the object.
  static readonly layer = Layer.effect(
    CompactionThreshold,
    Config.Number("COMPACTION_THRESHOLD").pipe(Config.withDefault(1024 * 1024), Effect.orDie),
  );
}

/** Durable Object SQLite rejects a value over 2 MB, so a snapshot is stored as 1 MiB messages. */
const SNAPSHOT_CHUNK = 1024 * 1024;

/**
 * Merge the previous snapshot with every update after it, up to the log's tail,
 * and publish the result. Runs inside the fold's transaction, so no append
 * interleaves and the snapshot and the fold state commit together. Returns
 * the new snapshot's offset.
 */
const publishSnapshot = Effect.fn("Yjs.publishSnapshot")(function* (
  document: string,
  previous: string | undefined,
) {
  const snapshots = yield* Snapshots;
  const updates = yield* Updates;

  const merged = yield* Effect.acquireUseRelease(
    Effect.sync(() => new Y.Doc()),
    (doc) =>
      Effect.gen(function* () {
        if (previous !== undefined)
          Y.applyUpdate(
            doc,
            concat((yield* snapshots.collect({ document, offset: previous })).items),
          );

        const log = yield* updates.collect({ document }, { offset: previous ?? "-1" });

        // Each update message is a lib0 frame of one or more Yjs updates.
        for (const item of log.items) {
          const decoder = decoding.createDecoder(item);

          while (decoding.hasContent(decoder))
            Y.applyUpdate(doc, decoding.readVarUint8Array(decoder));
        }

        return { tail: log.offset, update: Y.encodeStateAsUpdate(doc) };
      }),
    (doc) => Effect.sync(() => doc.destroy()),
  );

  const snapshot = { document, offset: merged.tail };
  yield* snapshots.create(snapshot);

  for (let at = 0; at < merged.update.length; at += SNAPSHOT_CHUNK)
    yield* snapshots.append(snapshot, [merged.update.subarray(at, at + SNAPSHOT_CHUNK)]);

  // Keep only the latest snapshot. Original updates remain available to stale readers.
  if (previous !== undefined)
    yield* snapshots
      .remove({ document, offset: previous })
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

  return merged.tail;
});

/**
 * `bytes` counts every folded update. `snapshot` is the latest snapshot's
 * offset, and `pending` counts the update bytes after it.
 */
const FoldState = Schema.Struct({
  bytes: Schema.Finite,
  pending: Schema.Finite,
  snapshot: Schema.optionalKey(Schema.String),
});

/**
 * Count update bytes for each document and publish a snapshot every
 * `COMPACTION_THRESHOLD` bytes. The checkpoint holds two numbers and an
 * offset, so its size does not grow with the document. `ProjectionWakeup.attach`
 * runs it from the object's alarm after each commit to `.updates`, off the
 * request path.
 *
 * The step reads the log to its tail outside the fold's cursor: the fold is a
 * durable, exactly-once trigger with a counter, and the merge is a side read in
 * the same transaction.
 */
export const compaction = Projection.fold({
  id: "yjs-compaction",
  // Version 2 keeps the snapshot offset in the state instead of an `.index` stream.
  version: 2,
  params: { document: Schema.String },
  inputs: { updates: Updates },
  state: { schema: FoldState, initial: { bytes: 0, pending: 0 } },
  commit: Commit.transaction,
  retry: { maxAttempts: 5 },
  fold: (state, { item, offset }, unit) =>
    Effect.gen(function* () {
      const bytes = state.bytes + item.length;

      // The last snapshot read to the log's tail, so it already covers this update.
      if (
        state.snapshot !== undefined &&
        offset !== undefined &&
        compare(offset, state.snapshot) <= 0
      )
        return { ...state, bytes };
      const pending = state.pending + item.length;

      if (pending < (yield* CompactionThreshold)) return { ...state, bytes, pending };

      return {
        bytes,
        pending: 0,
        snapshot: yield* publishSnapshot(unit.params.document, state.snapshot),
      };
    }),
});
