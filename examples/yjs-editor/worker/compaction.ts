import { StreamAddress, StreamId, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { Commit, Projection } from "@streamsy/projection";
import { Clock, Config, Context, Effect, Layer, Option, Schema } from "effect";
import * as decoding from "lib0/decoding";
import * as Y from "yjs";
import { binary, concat, readToTail } from "./streams.ts";

/** Each document's update log. The id grammar puts every document under `yjs/`. */
export class Updates extends Streamsy.custom<Updates>()("YjsEditor/Updates", {
  parse: (id: string) => {
    const match = /^(yjs\/[^/]+\/docs\/.+)\/\.updates$/.exec(id);

    return match?.[1] === undefined ? Option.none() : Option.some({ document: match[1] });
  },
  address: ({ document }: { readonly document: string }) =>
    StreamAddress.bytes(`${document}/.updates`),
}) {}

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

const IndexEntry = Schema.Struct({
  snapshotOffset: Schema.String,
  createdAt: Schema.Finite,
  foldedBytes: Schema.optionalKey(Schema.Finite),
});

const IndexEntryJson = Schema.fromJsonString(IndexEntry);

export const snapshotStream = (document: string, offset: string) =>
  StreamId.make(`${document}/.snapshots/${offset}_snapshot`);

const indexStream = (document: string) => StreamId.make(`${document}/.index`);

export const readIndex = Effect.fn("Yjs.readIndex")(function* (document: string) {
  const reader = yield* StreamsReader;

  const result = yield* reader
    .read(indexStream(document), { offset: "-1" })
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

  if (result === undefined) return [];

  return yield* Effect.forEach(result.messages, (message) =>
    Schema.decodeEffect(IndexEntryJson)(new TextDecoder().decode(message.data)),
  );
});

/**
 * Merge the previous snapshot with every update after it, up to the log's tail,
 * and publish the result. Runs inside the fold's transaction, so no append
 * interleaves and the snapshot, index entry and fold counter commit together.
 */
const publishSnapshot = Effect.fn("Yjs.publishSnapshot")(function* (
  document: string,
  foldedBytes: number,
) {
  const writer = yield* StreamsWriter;
  const previous = (yield* readIndex(document)).at(-1);

  const merged = yield* Effect.acquireUseRelease(
    Effect.sync(() => new Y.Doc()),
    (doc) =>
      Effect.gen(function* () {
        if (previous !== undefined) {
          const parts: Array<Uint8Array> = [];
          yield* readToTail(snapshotStream(document, previous.snapshotOffset), "-1", (data) =>
            parts.push(data),
          );
          Y.applyUpdate(doc, concat(parts));
        }

        // Each update message is a lib0 frame of one or more Yjs updates.
        const tail = yield* readToTail(
          StreamId.make(`${document}/.updates`),
          previous?.snapshotOffset ?? "-1",
          (data) => {
            const decoder = decoding.createDecoder(data);

            while (decoding.hasContent(decoder))
              Y.applyUpdate(doc, decoding.readVarUint8Array(decoder));
          },
        );

        return { tail, update: Y.encodeStateAsUpdate(doc) };
      }),
    (doc) => Effect.sync(() => doc.destroy()),
  );

  if (merged.tail === previous?.snapshotOffset) return;
  const snapshot = snapshotStream(document, merged.tail);
  yield* writer.create(snapshot, { contentType: binary });

  for (let at = 0; at < merged.update.length; at += SNAPSHOT_CHUNK)
    yield* writer.append(snapshot, {
      contentType: binary,
      data: merged.update.subarray(at, at + SNAPSHOT_CHUNK),
    });

  yield* writer.create(indexStream(document), { contentType: "application/json" });
  const createdAt = yield* Clock.currentTimeMillis;
  yield* writer.append(indexStream(document), {
    contentType: "application/json",
    data: new TextEncoder().encode(
      yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(IndexEntry)))([
        { snapshotOffset: merged.tail, createdAt, foldedBytes },
      ]),
    ),
  });

  // Keep only the latest snapshot. Original updates remain available to stale readers.
  if (previous !== undefined)
    yield* writer
      .remove(snapshotStream(document, previous.snapshotOffset))
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));
});

/** `bytes` counts every folded update; `published` is its value at the last snapshot. */
const FoldState = Schema.Struct({ bytes: Schema.Finite, published: Schema.Finite });

/**
 * Count update bytes for each document and publish a snapshot every
 * `COMPACTION_THRESHOLD` bytes. The checkpoint holds two numbers, so its size
 * does not grow with the document. `ProjectionWakeup.attach` runs it from the
 * object's alarm after each commit to `.updates`, off the request path.
 */
export const compaction = Projection.fold({
  id: "yjs-compaction",
  params: { document: Schema.String },
  inputs: { updates: Updates },
  state: { schema: FoldState, initial: { bytes: 0, published: 0 } },
  commit: Commit.transaction,
  retry: { maxAttempts: 5 },
  fold: (state, { item }, unit) =>
    Effect.gen(function* () {
      const bytes = state.bytes + item.length;
      const threshold = yield* CompactionThreshold;

      if (bytes - state.published < threshold) return { bytes, published: state.published };
      yield* publishSnapshot(unit.params.document, bytes);

      return { bytes, published: bytes };
    }),
});
