import { expect, it } from "bun:test";
import { Effect, Schema } from "effect";
import * as StreamAddress from "../../src/toolkit/address.ts";
import * as Streams from "../../src/toolkit/streams.ts";
import { ZERO_OFFSET } from "../../src/offset/index.ts";
import { StreamNotFound } from "../../src/protocol/errors.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/tags.ts";

const address = StreamAddress.json("events", { schema: Schema.Struct({ n: Schema.Finite }) });
/** One message per page, so every fold below reads several pages. */
const run = <A, E>(program: Effect.Effect<A, E, StreamsReader | StreamsWriter>) =>
  Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory({ readLimit: 1 }))));

it("fold reduces every page and returns the tail of the last page", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }, { n: 2 }, { n: 3 }]);
      const { nextOffset } = yield* Streams.head(address);
      const folded = yield* Streams.fold(
        address,
        () => 0,
        (sum, item) => sum + item.n,
      );
      expect(folded).toEqual({ state: 6, offset: nextOffset });
      const appended = yield* Streams.append(address, [{ n: 4 }], {
        expectedOffset: folded.offset,
      });
      expect(appended.offset).toBe((yield* Streams.head(address)).nextOffset);
    }),
  ));

it("fold continues from a tail that an earlier fold returned", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }, { n: 2 }]);
      const first = yield* Streams.collect(address);
      yield* Streams.append(address, [{ n: 3 }, { n: 4 }]);
      const next = yield* Streams.fold(
        address,
        () => first.items.map((item) => item.n),
        (seen, item) => [...seen, item.n],
        { offset: first.offset },
      );
      expect(next).toEqual({
        state: [1, 2, 3, 4],
        offset: (yield* Streams.head(address)).nextOffset,
      });
    }),
  ));

it("fold calls the initial value once for each fold", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }]);
      let calls = 0;
      const fold = Streams.fold(
        address,
        () => {
          calls += 1;
          return new Set<number>();
        },
        (seen, item) => seen.add(item.n),
      );
      const first = yield* fold;
      const second = yield* fold;
      expect(calls).toBe(2);
      expect(first.state).not.toBe(second.state);
    }),
  ));

it("collect returns the items and the tail of an empty stream", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      expect(yield* Streams.collect(address)).toEqual({ items: [], offset: ZERO_OFFSET });
    }),
  ));

it("collect on a fork returns the fork offset, not the zero offset", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }, { n: 2 }]);
      const child = StreamAddress.json("child", { schema: Schema.Struct({ n: Schema.Finite }) });
      yield* (yield* StreamsWriter).fork(child.id, address.id, {
        contentType: child.contentType,
      });
      const collected = yield* Streams.collect(child);
      expect(collected.items).toEqual([{ n: 1 }, { n: 2 }]);
      expect(collected.offset).not.toBe(ZERO_OFFSET);
      expect(collected.offset).toBe((yield* Streams.head(child)).nextOffset);
      yield* Streams.append(child, [{ n: 3 }], { expectedOffset: collected.offset });
    }),
  ));

it("collect fails with the read error of a missing stream", () =>
  run(
    Effect.gen(function* () {
      expect(yield* Effect.flip(Streams.collect(address))).toBeInstanceOf(StreamNotFound);
    }),
  ));
