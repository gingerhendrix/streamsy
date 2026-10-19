import { layer } from "@effect/vitest";
import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { expect } from "vitest";
import { Array, Effect, Option, Record, Schema } from "effect";
import { ZERO_OFFSET } from "@streamsy/core";
import { Streams } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Projection,
  ProjectionFault,
  type Entry,
  type Unit,
} from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";

const a = FamilyFixture.json("multi-a", { schema: Schema.Finite });

const b = FamilyFixture.json("multi-b", { schema: Schema.String });

type Inputs = { readonly a: typeof a; readonly b: typeof b };

const setup = Effect.gen(function* () {
  yield* Streams.create(a);
  yield* Streams.create(b);
  yield* Streams.append(a, [1, 2, 3]);
  yield* Streams.append(b, ["x"]);
});

/** Records every tagged item the handler saw, in order, with the unit it arrived in. */
const recorder = (inputs: Inputs, id = "multi") => {
  const seen: globalThis.Array<{ readonly entry: Entry<Inputs>; readonly unit: Unit }> = [];

  const projection = Projection.process({
    commit: Commit.atLeastOnce,
    params: {},
    id,
    inputs,
    process: Projection.each<Inputs, never, never>((entry, unit) =>
      Effect.sync(() => {
        seen.push({ entry, unit });
      }),
    ),
  }).at({});

  return { projection, seen };
};

const tags = (seen: ReadonlyArray<{ readonly entry: Entry<Inputs> }>) =>
  Array.map(seen, ({ entry }) => `${entry.input}:${String(entry.item)}#${entry.index}`);

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 1000 })))((it) => {
  it.effect("two inputs interleave in declaration order", () =>
    Effect.gen(function* () {
      yield* setup;
      const ab = recorder({ a, b }, "ab");
      const first = yield* Projection.run(ab.projection);
      expect(first.status).toBe("caught-up");
      expect(first.items).toBe(4);
      expect(tags(ab.seen)).toEqual(["a:1#0", "a:2#1", "a:3#2", "b:x#3"]);
      const ba = recorder({ b, a }, "ba");
      yield* Projection.run(ba.projection);
      expect(tags(ba.seen)).toEqual(["b:x#0", "a:1#1", "a:2#2", "a:3#3"]);
      expect(Record.keys(ab.seen[0]?.unit.ranges ?? {})).toEqual(["a", "b"]);
      expect(ab.seen[0]?.unit.key).toBe(ba.seen[0]?.unit.key.replace('"ba"', '"ab"'));
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 2 })))((it) => {
  it.effect("each input contributes one server page per pass", () =>
    Effect.gen(function* () {
      yield* setup;
      const { projection, seen } = recorder({ a, b });
      const first = yield* Projection.pass(projection);
      expect(first.items).toBe(3);
      expect(tags(seen)).toEqual(["a:1#0", "a:2#1", "b:x#2"]);
      expect(first.offsets.b).not.toBe(ZERO_OFFSET);
      expect(Record.keys(seen[0]?.unit.ranges ?? {})).toEqual(["a", "b"]);
      const second = yield* Projection.pass(projection);
      expect(second.items).toBe(1);
      expect(tags(seen).slice(3)).toEqual(["a:3#0"]);
      expect((yield* Projection.pass(projection)).status).toBe("caught-up");
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 1000 })))((it) => {
  it.effect("one closed input stays in the batch while the other continues", () =>
    Effect.gen(function* () {
      yield* setup;
      yield* Streams.append(a, [], { close: true });

      const batches: globalThis.Array<
        Record<string, { readonly closed: boolean; readonly items: number }>
      > = [];

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "closing",
        params: {},
        inputs: { a, b },
        process: (batch) =>
          Effect.sync(() => {
            batches.push({
              a: { closed: batch.a.closed, items: batch.a.items.length },
              b: { closed: batch.b.closed, items: batch.b.items.length },
            });
          }),
      }).at({});

      const first = yield* Projection.run(projection);
      expect(first.status).toBe("caught-up");
      expect(batches).toEqual([{ a: { closed: true, items: 3 }, b: { closed: false, items: 1 } }]);
      yield* Streams.append(b, ["y"]);
      const second = yield* Projection.run(projection);
      expect(second.status).toBe("caught-up");
      expect(second.items).toBe(1);
      expect(batches[1]).toEqual({ a: { closed: true, items: 0 }, b: { closed: false, items: 1 } });
      yield* Streams.append(b, [], { close: true });
      expect((yield* Projection.run(projection)).status).toBe("source-closed");
      expect(batches).toHaveLength(2);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 1000 })))((it) => {
  it.effect("a missing second input fails the read naming that input", () =>
    Effect.gen(function* () {
      yield* Streams.create(a);
      yield* Streams.append(a, [1]);
      const { projection, seen } = recorder({ a, b });
      const result = yield* Projection.run(projection).pipe(Effect.result);
      expect(result._tag).toBe("Failure");

      if (result._tag === "Failure") {
        expect(result.failure).toBeInstanceOf(ProjectionFault);
        expect(result.failure.phase).toBe("read");
        expect(result.failure.reason).toBe("history-unavailable");
        expect(result.failure.input).toBe("b");
      }

      expect(seen).toHaveLength(0);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.isNone(loaded.record)).toBe(true);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 1000 })))((it) => {
  it.effect("items keeps declaration order and each runs one entry at a time", () =>
    Effect.gen(function* () {
      yield* setup;
      const order: globalThis.Array<string> = [];
      let active = 0;
      let overlapped = false;

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "sequenced",
        params: {},
        inputs: { b, a },
        process: (batch, unit) =>
          Effect.gen(function* () {
            expect(Array.map(Projection.items(batch), (entry) => entry.input)).toEqual([
              "b",
              "a",
              "a",
              "a",
            ]);
            yield* Projection.each<Inputs, never, never>((entry) =>
              Effect.gen(function* () {
                active += 1;

                if (active > 1) overlapped = true;
                yield* Effect.yieldNow;
                order.push(`${entry.input}:${String(entry.item)}`);
                active -= 1;
              }),
            )(batch, unit);
          }),
      }).at({});

      yield* Projection.run(projection);
      expect(order).toEqual(["b:x", "a:1", "a:2", "a:3"]);
      expect(overlapped).toBe(false);
    }),
  );
});
