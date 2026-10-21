import { expect, it as baseIt, layer } from "@effect/vitest";
import { Array, Effect, Exit, Schema, Stream } from "effect";
import * as DurableState from "../../src/toolkit/durable-state.ts";
import * as StreamAddress from "../../src/toolkit/address.ts";
import { MemoryStorage } from "../../src/index.ts";
import * as Streams from "../../src/toolkit/streams.ts";

const Story = Schema.Struct({ id: Schema.Finite, title: Schema.String });

const stories = StreamAddress.state("stories", {
  collections: { story: { schema: Story, key: "id" } },
});

const Wire = Schema.fromJsonString(Schema.Unknown);

const wire = (item: StreamAddress.CollectionsEvent<typeof stories.collections>) =>
  Schema.decodeSync(Wire)(
    Schema.decodeUnknownSync(Schema.String)(Schema.encodeSync(stories.codec)(item)),
  );

const controls = ["snapshot-start", "snapshot-end", "reset"] as const;

baseIt("decodes and encodes each State control with and without an offset", () => {
  for (const control of controls) {
    for (const headers of [{ control }, { control, offset: "0000000000000004_0000000000000012" }]) {
      const message = { headers };

      expect(Schema.decodeSync(stories.codec)(JSON.stringify(message))).toEqual(message);
      expect(wire(message)).toEqual(message);
    }
  }
});

baseIt("builds each control with the constructors", () => {
  expect(StreamAddress.reset()).toEqual({ headers: { control: "reset" } });
  expect(StreamAddress.snapshotStart({ offset: "9" })).toEqual({
    headers: { control: "snapshot-start", offset: "9" },
  });
  expect(StreamAddress.snapshotEnd()).toEqual({ headers: { control: "snapshot-end" } });
  // An explicit `undefined` offset is omitted, so the wire has no `offset` key.
  expect(StreamAddress.reset({ offset: undefined })).toStrictEqual({
    headers: { control: "reset" },
  });
});

baseIt("rejects a message with an unknown or missing control name", () => {
  for (const message of [
    { headers: { control: "truncate" } },
    { headers: {} },
    { headers: { control: 1 } },
  ]) {
    expect(Exit.isFailure(Schema.decodeExit(stories.codec)(JSON.stringify(message)))).toBe(true);
  }
});

baseIt("tells controls and changes apart", () => {
  const items: ReadonlyArray<StreamAddress.CollectionsEvent<typeof stories.collections>> = [
    StreamAddress.reset(),
    { type: "story", key: "1", value: { id: 1, title: "a" }, headers: { operation: "upsert" } },
    { type: "story", key: "1", headers: { operation: "delete" } },
    StreamAddress.snapshotEnd(),
  ];

  expect(Array.map(items, StreamAddress.isControl)).toEqual([true, false, false, true]);
  expect(Array.map(items, StreamAddress.isChange)).toEqual([false, true, true, false]);
});

baseIt("passes controls through DurableState.changes and stamps a missing offset", () => {
  const events = DurableState.changes(stories, { offset: "source:3" }, [
    StreamAddress.reset(),
    DurableState.upsert("story", { id: 1, title: "a" }),
    StreamAddress.snapshotStart({ offset: "own" }),
    DurableState.remove("story", 2),
  ]);

  expect(events).toEqual([
    { headers: { control: "reset", offset: "source:3" } },
    {
      type: "story",
      key: "1",
      value: { id: 1, title: "a" },
      headers: { operation: "upsert", offset: "source:3", txid: "source:3:1" },
    },
    { headers: { control: "snapshot-start", offset: "own" } },
    {
      type: "story",
      key: "2",
      headers: { operation: "delete", offset: "source:3", txid: "source:3:3" },
    },
  ]);

  const reset = StreamAddress.reset();
  expect(DurableState.changes(stories, {}, [reset])[0]).toBe(reset);
});

layer(MemoryStorage.layerProtocol())((it) => {
  it.effect("a typed append writes a mixed batch of changes and controls", () =>
    Effect.gen(function* () {
      const batch = DurableState.changes(stories, {}, [
        StreamAddress.reset(),
        StreamAddress.snapshotStart(),
        DurableState.upsert("story", { id: 1, title: "a" }),
        StreamAddress.snapshotEnd(),
        DurableState.remove("story", 1),
      ]);

      yield* Streams.create(stories);
      yield* Streams.append(stories, batch);

      expect(yield* Streams.read(stories).pipe(Streams.items, Stream.runCollect)).toEqual([
        ...batch,
      ]);

      const raw = yield* Streams.read(StreamAddress.bytes("stories")).pipe(
        Streams.items,
        Stream.runCollect,
      );

      expect(Array.map(raw, (item) => new TextDecoder().decode(item))).toEqual([
        '{"headers":{"control":"reset"}}',
        '{"headers":{"control":"snapshot-start"}}',
        '{"type":"story","key":"1","value":{"id":1,"title":"a"},"headers":{"operation":"upsert"}}',
        '{"headers":{"control":"snapshot-end"}}',
        '{"type":"story","key":"1","headers":{"operation":"delete"}}',
      ]);
    }),
  );

  it.effect("a follow reads past a reset that a raw writer stored", () =>
    Effect.gen(function* () {
      const followed = StreamAddress.state("stored-reset", { collections: stories.collections });
      const raw = StreamAddress.json("stored-reset", { schema: Schema.Unknown });
      yield* Streams.create(followed);

      yield* Streams.append(raw, [
        { type: "story", key: "1", value: { id: 1, title: "a" }, headers: { operation: "insert" } },
        { headers: { control: "reset", offset: "0" } },
        { type: "story", key: "2", value: { id: 2, title: "b" }, headers: { operation: "insert" } },
      ]);

      const items = yield* Streams.follow(followed).pipe(
        Streams.items,
        Stream.take(3),
        Stream.runCollect,
      );

      expect(items).toEqual([
        { type: "story", key: "1", value: { id: 1, title: "a" }, headers: { operation: "upsert" } },
        { headers: { control: "reset", offset: "0" } },
        { type: "story", key: "2", value: { id: 2, title: "b" }, headers: { operation: "upsert" } },
      ]);

      // A reader that folds the stream clears its rows at the reset.
      const rows = Array.reduce(items, new Map<string, string>(), (acc, item) => {
        if (StreamAddress.isControl(item)) {
          if (item.headers.control === "reset") acc.clear();
        } else if ("value" in item) acc.set(item.key, item.value.title);
        else acc.delete(item.key);

        return acc;
      });

      expect([...rows]).toEqual([["2", "b"]]);
    }),
  );
});
