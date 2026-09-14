import { expect, test } from "bun:test";
import { Effect, Option, Schema } from "effect";
import { StreamRef, Streams, ZERO_OFFSET } from "@streamsy/core";
import { Checkpoints, Projection, ProjectionFault, type Entry, type Unit } from "../src/index.ts";
import { layerMemory } from "../src/memory.ts";
import { positives, initialize, type Services } from "./scenarios.ts";

const a = StreamRef.json("multi-a", { schema: Schema.Finite });
const b = StreamRef.json("multi-b", { schema: Schema.String });
type Inputs = { readonly a: typeof a; readonly b: typeof b };

const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(body.pipe(Effect.provide(layerMemory())));
const setup = Effect.gen(function* () {
  yield* Streams.create(a);
  yield* Streams.create(b);
  yield* Streams.append(a, [1, 2, 3]);
  yield* Streams.append(b, ["x"]);
});
/** Records every tagged item the handler saw, in order, with the unit it arrived in. */
const recorder = (inputs: Inputs, id = "multi") => {
  const seen: Array<{ readonly entry: Entry<Inputs>; readonly unit: Unit }> = [];
  const projection = Projection.make({
    id,
    inputs,
    process: Projection.each<Inputs, never, never>((entry, unit) =>
      Effect.sync(() => {
        seen.push({ entry, unit });
      }),
    ),
  });
  return { projection, seen };
};
const tags = (seen: ReadonlyArray<{ readonly entry: Entry<Inputs> }>) =>
  seen.map(({ entry }) => `${entry.input}:${String(entry.item)}#${entry.index}`);

test("two inputs interleave in declaration order", () =>
  run(
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
      expect(Object.keys(ab.seen[0]?.unit.ranges ?? {})).toEqual(["a", "b"]);
      expect(ab.seen[0]?.unit.key).toBe(ba.seen[0]?.unit.key.replace('"ba"', '"ab"'));
    }),
  ));

test("budget carry-forward drains the first input first", () =>
  run(
    Effect.gen(function* () {
      yield* setup;
      const { projection, seen } = recorder({ a, b });
      const first = yield* Projection.pass(projection, { items: 2 });
      expect(first.items).toBe(2);
      expect(first.status).toBe("progress");
      expect(tags(seen)).toEqual(["a:1#0", "a:2#1"]);
      expect(first.record.inputs.b).toBe(ZERO_OFFSET);
      expect(seen[0]?.unit.ranges.b).toBeUndefined();
      const second = yield* Projection.pass(projection, { items: 2 });
      expect(second.items).toBe(2);
      expect(second.status).toBe("progress");
      expect((yield* Projection.pass(projection)).status).toBe("caught-up");
      expect(tags(seen).slice(2)).toEqual(["a:3#0", "b:x#1"]);
      expect(Object.keys(seen[2]?.unit.ranges ?? {})).toEqual(["a", "b"]);
    }),
  ));

test("one closed input stays in the batch while the other continues", () =>
  run(
    Effect.gen(function* () {
      yield* setup;
      yield* Streams.append(a, [], { close: true });
      const batches: Array<Record<string, { readonly closed: boolean; readonly items: number }>> =
        [];
      const projection = Projection.make({
        id: "closing",
        inputs: { a, b },
        process: (batch) =>
          Effect.sync(() => {
            batches.push({
              a: { closed: batch.a.closed, items: batch.a.items.length },
              b: { closed: batch.b.closed, items: batch.b.items.length },
            });
          }),
      });
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
  ));

test("a single-input projection stores a one-key inputs map", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const result = yield* Projection.run(positives);
      const loaded = yield* (yield* Checkpoints).load(Projection.key(positives));
      const record = Option.getOrThrow(loaded.record);
      expect(Object.keys(record.inputs)).toEqual(["input"]);
      expect(record.inputs).toEqual(result.record.inputs);
      expect(Object.keys(record.identity.inputs)).toEqual(["input"]);
      expect(record.pending).toBeUndefined();
      expect(record.adapters).toEqual({});
    }),
  ));

test("a missing second input fails the read naming that input", () =>
  run(
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
      const loaded = yield* (yield* Checkpoints).load(Projection.key(projection));
      expect(Option.isNone(loaded.record)).toBe(true);
    }),
  ));

test("items keeps declaration order and each runs one entry at a time", () =>
  run(
    Effect.gen(function* () {
      yield* setup;
      const order: Array<string> = [];
      let active = 0;
      let overlapped = false;
      const projection = Projection.make({
        id: "sequenced",
        inputs: { b, a },
        process: (batch, unit) =>
          Effect.gen(function* () {
            expect(Projection.items(batch).map((entry) => entry.input)).toEqual([
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
      });
      yield* Projection.run(projection);
      expect(order).toEqual(["b:x", "a:1", "a:2", "a:3"]);
      expect(overlapped).toBe(false);
    }),
  ));
