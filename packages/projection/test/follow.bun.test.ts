/* oxlint-disable effecttsgo/strict-effect-provide -- Bun owns the runtime boundary; each test assembles the complete host graph with a test clock. */
import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Layer, Schema, type Scope } from "effect";
import { TestClock } from "effect/testing";
import {
  StreamRef,
  Streams,
  StreamsReader,
  type StreamsWriter,
  type Reader,
  type StreamsFault,
} from "@streamsy/core";
import { Checkpoints, Projection } from "../src/index.ts";
import { layerMemory } from "../src/memory.ts";
import { input, output, positives, readAll } from "./scenarios.ts";

type Services = Checkpoints | StreamsReader | StreamsWriter | Scope.Scope;
const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(
    body.pipe(Effect.scoped, Effect.provide(Layer.merge(layerMemory(), TestClock.layer()))),
  );
const withReader = <A, E, R>(
  body: Effect.Effect<A, E, R | StreamsReader>,
  patch: (reader: Reader<StreamsFault>) => Partial<Reader<StreamsFault>>,
) =>
  Effect.gen(function* () {
    const reader = yield* StreamsReader;
    return yield* body.pipe(
      Effect.provideService(StreamsReader, StreamsReader.of({ ...reader, ...patch(reader) })),
    );
  });
/** A wake hint that parks forever and reports when it is parked and when it is released. */
const parkedHint = Effect.gen(function* () {
  const parked = yield* Deferred.make<void>();
  const released = yield* Deferred.make<void>();
  const patch = (): Partial<Reader<StreamsFault>> => ({
    readNext: () =>
      Deferred.succeed(parked, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Deferred.succeed(released, undefined)),
      ),
  });
  return { parked, released, patch };
});

test("follow repairs a missed wake and closes", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const hint = yield* parkedHint;
      const fiber = yield* withReader(
        Projection.follow(positives, { repairIntervalMs: 100 }),
        hint.patch,
      );
      yield* Deferred.await(hint.parked);
      yield* Streams.append(input, [7], { close: true });
      yield* TestClock.adjust(100);
      const result = yield* Fiber.join(fiber);
      expect(result.status).toBe("source-closed");
      expect(yield* readAll(output)).toEqual([7]);
      yield* Deferred.await(hint.released);
    }),
  ));

test("follow cancellation releases parked waits", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const hint = yield* parkedHint;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withReader(Projection.follow(positives), hint.patch);
          yield* Deferred.await(hint.parked);
        }),
      );
      yield* Deferred.await(hint.released);
      expect((yield* (yield* Checkpoints).load(Projection.key(positives))).token).toBe("0");
    }),
  ));

test("follow paces an oversized unit", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      yield* Streams.append(input, [], { close: true });
      const read = yield* Deferred.make<void>();
      let reads = 0;
      const fiber = yield* withReader(
        Projection.follow(positives, { repairIntervalMs: 100, bytes: 1 }),
        (reader) => ({
          read: (id, options) =>
            Effect.gen(function* () {
              reads += 1;
              yield* Deferred.succeed(read, undefined);
              // The first read answers an oversized message; later reads see the closed, empty stream.
              return reads === 1
                ? {
                    messages: [{ data: new TextEncoder().encode("1000") }],
                    nextOffset: `${"0".repeat(15)}1_${"0".repeat(16)}`,
                    upToDate: true,
                    closed: false,
                  }
                : yield* reader.read(id, options);
            }),
        }),
      );
      yield* Deferred.await(read);
      yield* Effect.yieldNow;
      expect(reads).toBe(1);
      yield* TestClock.adjust(100);
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
      expect(reads).toBe(2);
      expect(yield* readAll(output)).toEqual([]);
    }),
  ));

test("a write to the second input wakes a two-input follower", () =>
  run(
    Effect.gen(function* () {
      const a = StreamRef.json("wake-a", { schema: Schema.Finite });
      const b = StreamRef.json("wake-b", { schema: Schema.Finite });
      const target = StreamRef.json("wake-out", { schema: Schema.String });
      yield* Streams.create(a);
      yield* Streams.create(b);
      yield* Streams.create(target);
      yield* Streams.append(a, [1]);
      const woke = yield* Deferred.make<void>();
      const projection = Projection.make({
        id: "wake",
        inputs: { a, b },
        process: (batch) =>
          Effect.gen(function* () {
            yield* Streams.append(
              target,
              Projection.items(batch).map((entry) => `${entry.input}:${entry.item}`),
            );
            if (batch.b.items.length > 0) yield* Deferred.succeed(woke, undefined);
          }),
      });
      const parked = yield* Deferred.make<void>();
      let hints = 0;
      const fiber = yield* withReader(
        Projection.follow(projection, { repairIntervalMs: 10_000 }),
        (reader) => ({
          readNext: (id, options) =>
            Effect.gen(function* () {
              hints += 1;
              if (hints === 2) yield* Deferred.succeed(parked, undefined);
              return yield* reader.readNext(id, options);
            }),
        }),
      );
      // Both inputs are parked on their hints after the first run drains `a`.
      yield* Deferred.await(parked);
      expect(yield* readAll(target)).toEqual(["a:1"]);
      yield* Streams.append(b, [2]);
      yield* Deferred.await(woke);
      // The handler signalled inside its transaction; the commit is visible once the token moves.
      const owner = yield* Checkpoints;
      for (let spins = 0; spins < 1000; spins += 1) {
        if ((yield* owner.load(Projection.key(projection))).token === "2") break;
        yield* Effect.yieldNow;
      }
      expect((yield* owner.load(Projection.key(projection))).token).toBe("2");
      expect(yield* readAll(target)).toEqual(["a:1", "b:2"]);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("a closed and drained input beside an open one does not wake every cycle", () =>
  run(
    Effect.gen(function* () {
      const a = StreamRef.json("closed-a", { schema: Schema.Finite });
      const b = StreamRef.json("open-b", { schema: Schema.Finite });
      const target = StreamRef.json("closed-out", { schema: Schema.String });
      yield* Streams.create(a);
      yield* Streams.create(b);
      yield* Streams.create(target);
      yield* Streams.append(a, [1], { close: true });
      yield* Streams.append(b, [2]);
      const projection = Projection.make({
        id: "closed-open",
        inputs: { a, b },
        process: (batch) =>
          Streams.append(
            target,
            Projection.items(batch).map((entry) => `${entry.input}:${entry.item}`),
          ),
      });
      let reads = 0;
      let hints = 0;
      const fiber = yield* withReader(
        Projection.follow(projection, { repairIntervalMs: 100 }),
        (reader) => ({
          read: (id, options) =>
            Effect.suspend(() => {
              reads += 1;
              return reader.read(id, options);
            }),
          readNext: (id, options) =>
            Effect.suspend(() => {
              hints += 1;
              return reader.readNext(id, options);
            }),
        }),
      );
      // The first run is one non-empty pass and one empty pass, then one hint per input.
      const settle = (expectedHints: number) =>
        Effect.gen(function* () {
          for (let spins = 0; spins < 1000; spins += 1) {
            if (hints >= expectedHints) break;
            yield* Effect.yieldNow;
          }
          for (let spins = 0; spins < 50; spins += 1) yield* Effect.yieldNow;
        });
      yield* settle(2);
      expect(yield* readAll(target)).toEqual(["a:1", "b:2"]);
      expect(reads).toBe(4);
      expect(hints).toBe(2);
      // The closed input answered at once; without the clock moving nothing else runs.
      yield* TestClock.adjust(100);
      yield* settle(4);
      expect(reads).toBe(6);
      expect(hints).toBe(4);
      yield* TestClock.adjust(100);
      yield* settle(6);
      expect(reads).toBe(8);
      expect(hints).toBe(6);
      // A write to the open input still wakes the follower before the interval.
      yield* Streams.append(b, [3]);
      yield* settle(8);
      expect(yield* readAll(target)).toEqual(["a:1", "b:2", "b:3"]);
      expect(reads).toBe(12);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("follow rejects a non-positive repair interval before reading", () =>
  run(
    Effect.gen(function* () {
      const result = yield* Projection.follow(positives, { repairIntervalMs: 0 }).pipe(
        Effect.result,
      );
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure.phase).toBe("load");
        expect(result.failure.reason).toBe("invalid-budget");
      }
    }),
  ));
