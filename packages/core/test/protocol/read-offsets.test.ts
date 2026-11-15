import { layer } from "@effect/vitest";
import { expect, test } from "vitest";
import { Array, Effect, Record } from "effect";
import { MemoryStorage, Storage, StreamId, ZERO_OFFSET } from "@streamsy/core";
import { attachOffsets, offsetsOf } from "@streamsy/core/internal";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";

layer(MemoryStorage.layerProtocol({ readLimit: 2 }))((it) => {
  it.effect("Protocol.read retains storage offsets in every bounded window", () =>
    Effect.gen(function* () {
      const id = StreamId.make("read-offsets");
      const writer = yield* StreamsWriter;
      const reader = yield* StreamsReader;
      const storage = yield* Storage;
      yield* writer.create(id, { contentType: "application/json" });
      yield* writer.append(id, {
        contentType: "application/json",
        data: new TextEncoder().encode("[1,2,3,4,5]"),
      });
      const stored = yield* storage.messages(id, {});
      let cursor: string = ZERO_OFFSET;
      const offsets: Array<string> = [];

      for (let i = 0; i < 3; i++) {
        const result = yield* reader.read(id, { offset: cursor });
        expect(offsetsOf(result)).toEqual(
          Array.map(stored.slice(i * 2, i * 2 + 2), (message) => message.offset),
        );
        expect(Record.keys(result)).toEqual([
          "contentType",
          "messages",
          "nextOffset",
          "upToDate",
          "closed",
        ]);
        expect(result.messages).toEqual(
          Array.map(stored.slice(i * 2, i * 2 + 2), (message) => ({ data: message.data })),
        );
        offsets.push(...(offsetsOf(result) ?? []));
        cursor = result.nextOffset;
      }

      expect(offsets).toEqual(Array.map(stored, (message) => message.offset));
      expect(offsetsOf(yield* reader.read(id, { offset: cursor }))).toEqual([]);
    }),
  );
});

test("offset metadata is private and absent from a hand-built or copied result", () => {
  const result = { messages: [] };
  expect(offsetsOf(result)).toBeUndefined();
  expect(attachOffsets(result, ["cursor"])).toBe(result);
  expect(offsetsOf(result)).toEqual(["cursor"]);
  expect(offsetsOf({ ...result })).toBeUndefined();
  expect(JSON.stringify(result)).toBe('{"messages":[]}');
});
