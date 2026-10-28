import { DurableState } from "@streamsy/core";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Record, Schedule, Schema } from "effect";
import { FileIndexLive } from "../server/file-index.ts";
import { FolderStore, FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { sqliteHost } from "../server/storage.ts";
import { Files, FolderWake } from "../server/streams.ts";
import { WakeLive } from "../server/wake.ts";
import { storedText } from "./content.ts";

class NotReady extends Schema.TaggedError<NotReady, { readonly brand: unique symbol }>()(
  "NotReady",
  {},
) {}

const until = <A, E, R>(read: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  read.pipe(
    Effect.filterOrFail(ready, () => new NotReady()),
    Effect.retry({
      while: Schema.is(NotReady),
      schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 300 })),
    }),
  );

const graph = Layer.mergeAll(Initialise, FolderStoreLive, FileIndexLive, WakeLive).pipe(
  Layer.provideMerge(sqliteHost(":memory:")),
);

/** The wake rows by source stream id, as `stream -> offset`. */
const rows = Effect.gen(function* () {
  const { items } = yield* (yield* FolderWake).collect({});
  const state = DurableState.materialize(DurableState.materialized(FolderWake), items);

  return Record.map(Record.fromEntries(state.rows.stream), (row) => row.offset);
});

const write = Effect.fn("WakeTest.write")(function* (folderId: string, path: string) {
  yield* (yield* FolderStore).commit(folderId, path, {
    ...(yield* storedText(`${folderId}/${path}`)),
    txid: `${folderId}/${path}`,
    author: "test",
    parent: null,
  });

  return (yield* (yield* Files).head({ folderId })).nextOffset;
});

layer(graph, { excludeTestServices: true })((it) => {
  it.effect("the host creates the empty wake stream at start, before any folder write", () =>
    Effect.gen(function* () {
      expect(yield* rows).toEqual({});
    }),
  );

  it.effect("a file write gives a wake row with the new tail; a second folder a second row", () =>
    Effect.gen(function* () {
      const store = yield* FolderStore;
      yield* store.create("wake/a", "A", "create-a");
      const first = yield* write("wake/a", "one.md");
      const a = Files.at({ folderId: "wake/a" }).id;

      yield* until(rows, (current) => current[a] === first);

      const second = yield* write("wake/a", "two.md");
      yield* until(rows, (current) => current[a] === second);

      yield* store.create("wake/b", "B", "create-b");
      const other = yield* write("wake/b", "one.md");
      const b = Files.at({ folderId: "wake/b" }).id;
      const both = yield* until(rows, (current) => current[b] === other);

      expect(both).toEqual({ [a]: second, [b]: other });
    }),
  );

  it.effect("a folder id at the 249-byte limit gets its wake row", () =>
    Effect.gen(function* () {
      // 83 code units and 249 UTF-8 bytes: the wake producer id has 527 characters.
      const folderId = "雪".repeat(83);
      yield* (yield* FolderStore).create(folderId, "Long", "create-long");
      const tail = yield* write(folderId, "one.md");
      const id = Files.at({ folderId }).id;

      yield* until(rows, (current) => current[id] === tail);
    }),
  );
});
