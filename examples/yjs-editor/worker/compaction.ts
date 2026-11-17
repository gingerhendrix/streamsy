import { StreamId, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { Projection, Transaction } from "@streamsy/projection";
import { Clock, Effect, Schema } from "effect";
import * as buffer from "lib0/buffer";
import * as decoding from "lib0/decoding";
import * as Y from "yjs";

const FoldState = Schema.Struct({ update: Schema.String, bytes: Schema.Finite });

const IndexEntry = Schema.Struct({
  snapshotOffset: Schema.String,
  createdAt: Schema.Finite,
  foldedBytes: Schema.optionalKey(Schema.Finite),
});

const IndexEntryJson = Schema.fromJsonString(IndexEntry);

/** Every saved value is a full Yjs update; the mutable Y.Doc never survives a step. */
export const documentFold = (document: string) =>
  Projection.fold({
    id: `yjs-compaction:${document}`,
    params: {},
    inputs: { updates: Streamsy.Wildcard.at({ id: `${document}/.updates` }) },
    state: { schema: FoldState, initial: { update: "", bytes: 0 } },
    fold: (state, { item }) => {
      const doc = new Y.Doc();

      try {
        if (state.update) Y.applyUpdate(doc, buffer.fromBase64(state.update));
        const decoder = decoding.createDecoder(item);

        while (decoding.hasContent(decoder))
          Y.applyUpdate(doc, decoding.readVarUint8Array(decoder));

        return {
          update: buffer.toBase64(Y.encodeStateAsUpdate(doc)),
          bytes: state.bytes + item.length,
        };
      } finally {
        doc.destroy();
      }
    },
  }).at({});

export const readIndex = (document: string) =>
  Effect.gen(function* () {
    const reader = yield* StreamsReader;

    const result = yield* reader
      .read(StreamId.make(`${document}/.index`), { offset: "-1" })
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

    if (result === undefined) return [];
    const entries: Array<typeof IndexEntry.Type> = [];

    for (const message of result.messages) {
      entries.push(
        yield* Schema.decodeEffect(IndexEntryJson)(new TextDecoder().decode(message.data)),
      );
    }

    return entries;
  });

/** The fold checkpoint, snapshot and discovery index commit on the same SQLite owner. */
export const compact = (document: string, threshold: number) =>
  Effect.gen(function* () {
    const tx = yield* Transaction;

    return yield* tx.withTransaction(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const progress = yield* Projection.run(documentFold(document));
        const entries = yield* readIndex(document);
        const previous = entries.at(-1);

        if (progress.state.bytes - (previous?.foldedBytes ?? 0) < threshold) return;
        const offset = progress.offsets.updates;

        if (offset === undefined || previous?.snapshotOffset === offset) return;
        const snapshot = `${document}/.snapshots/${offset}_snapshot`;
        yield* writer.create(StreamId.make(snapshot), { contentType: "application/octet-stream" });
        yield* writer.append(StreamId.make(snapshot), {
          contentType: "application/octet-stream",
          data: buffer.fromBase64(progress.state.update),
        });
        yield* writer.create(StreamId.make(`${document}/.index`), {
          contentType: "application/json",
        });
        const createdAt = yield* Clock.currentTimeMillis;
        yield* writer.append(StreamId.make(`${document}/.index`), {
          contentType: "application/json",
          data: new TextEncoder().encode(
            yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(IndexEntry)))([
              { snapshotOffset: offset, createdAt, foldedBytes: progress.state.bytes },
            ]),
          ),
        });

        // Keep only the latest snapshot. Original updates remain available to stale readers.
        if (previous !== undefined)
          yield* writer
            .remove(StreamId.make(`${document}/.snapshots/${previous.snapshotOffset}_snapshot`))
            .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));
      }),
    );
  });
