import { expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as StreamRef from "./ref.ts";
import * as Streams from "./streams.ts";
import * as Producer from "./producer.ts";
import * as Fold from "./fold.ts";
import { DecodeFault, EncodeFault } from "../fault.ts";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { StreamsTest, layerTest } from "../testing/streams-test.ts";
import { ZERO_OFFSET } from "../offset/index.ts";

function provideTest<R>(layer: Layer.Layer<R>) {
  return <A, E>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(layer);
        return yield* program.pipe(Effect.provide(context));
      }),
    );
}
function check<E>(program: Effect.Effect<void, E, StreamsReader | StreamsWriter | StreamsTest>) {
  return expect(
    Effect.runPromiseExit(
      program.pipe(
        provideTest(Layer.mergeAll(layerTest({ longPollTimeoutMs: 100 }), TestClock.layer())),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined));
}
const ref = StreamRef.json("events", { schema: Schema.Struct({ n: Schema.Finite }) });

it("refs are inert and layerMemory supplies real tags", () =>
  expect(
    Effect.runPromiseExit(
      Effect.gen(function* () {
        expect(String(ref.id)).toBe("events");
        expect(yield* Streams.create(ref)).toMatchObject({ _tag: "Created" });
        expect(yield* Streams.append(ref, [{ n: 1 }, { n: 2 }])).toMatchObject({
          _tag: "Appended",
        });
        const batches = yield* Streams.read(ref).pipe(Stream.runCollect);
        expect(batches.map((batch) => batch.items)).toEqual([[{ n: 1 }], [{ n: 2 }]]);
        expect(yield* Streams.read(ref).pipe(Streams.items, Stream.runCollect)).toEqual([
          { n: 1 },
          { n: 2 },
        ]);
        expect(yield* Streams.head(ref)).toMatchObject({
          contentType: "application/json",
        });
        expect(yield* Streams.remove(ref)).toBeUndefined();
      }).pipe(provideTest(Streams.layerMemory({ readLimit: 1 }))),
    ),
  ).resolves.toEqual(Exit.succeed(undefined)));

it("bytes ref round trips batches and zero-byte close", () =>
  check(
    Effect.gen(function* () {
      const bytes = StreamRef.bytes("bytes", { contentType: "text/plain" });
      yield* Streams.create(bytes);
      yield* Streams.append(bytes, [new Uint8Array([1]), new Uint8Array([2])]);
      yield* Streams.append(bytes, [], { close: true });
      expect(yield* Streams.follow(bytes).pipe(Streams.items, Stream.runCollect)).toEqual([
        new Uint8Array([1, 2]),
      ]);
    }),
  ));

it("Schema encode failure is typed and performs no append", () =>
  check(
    Effect.gen(function* () {
      const restricted = StreamRef.json("restricted", {
        schema: Schema.Finite.pipe(Schema.check(Schema.isGreaterThan(0))),
      });
      yield* Streams.create(restricted);
      const exit = yield* Streams.append(restricted, [-1]).pipe(Effect.exit);
      expect(Exit.isFailure(exit) && Schema.is(EncodeFault)(Cause.squash(exit.cause))).toBe(true);
      expect(yield* Streams.head(restricted)).toMatchObject({ nextOffset: ZERO_OFFSET });
    }),
  ));

for (const body of ['{"n":"wrong"}', "{bad"]) {
  it(`Schema decode failure is typed for ${body}`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const control = yield* StreamsTest;
        yield* Streams.create(ref);
        if (body === "{bad") {
          const record = yield* control.storage.record(ref.id);
          expect(Option.isSome(record)).toBe(true);
          yield* control.storage.mutate({
            operations: [
              {
                _tag: "Append",
                streamId: ref.id,
                messages: [
                  { data: new TextEncoder().encode(body), offset: ZERO_OFFSET, timestamp: 0 },
                ],
                patch: {},
              },
            ],
          });
        } else
          yield* writer.append(ref.id, {
            contentType: "application/json",
            data: new TextEncoder().encode(body),
          });
        const exit = yield* Streams.read(ref).pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit) && Schema.is(DecodeFault)(Cause.squash(exit.cause))).toBe(true);
      }),
    ));
}

for (const operation of [Streams.read, Streams.follow]) {
  it(`${operation.name} and session fail with StreamNotFound`, () =>
    check(
      Effect.gen(function* () {
        const exit = yield* operation(ref).pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({
          id: ref.id,
          _tag: "StreamNotFound",
        });
        expect((yield* Effect.flip(Streams.session(ref, { offset: ZERO_OFFSET })))._tag).toBe(
          "StreamNotFound",
        );
      }),
    ));
}

it("follow's parked subscription and fiber are interrupted by scope close", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(ref);
      const control = yield* StreamsTest;
      let completed = false;
      const stoppedFiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const fiber = yield* Streams.follow(ref).pipe(
            Stream.runDrain,
            Effect.ensuring(
              Effect.sync(() => {
                completed = true;
              }),
            ),
            Effect.forkScoped,
          );
          yield* control.snapshot;
          expect(yield* control.subscribers).toBe(1);
          return fiber;
        }),
      );
      const exit = yield* Fiber.await(stoppedFiber);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(completed).toBe(true);
      expect(yield* control.subscribers).toBe(0);
      yield* Streams.append(ref, [{ n: 1 }]);
      yield* TestClock.adjust(200);
      expect(yield* control.subscribers).toBe(0);
    }),
  ));

it("Fold reduces existing items, waits for growth, and completes on close", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(ref);
      yield* Streams.append(ref, [{ n: 1 }, { n: 2 }]);
      const control = yield* StreamsTest;
      const seen = yield* Deferred.make<void>();
      const fiber = yield* Fold.run(ref, { initial: 0, step: (sum, item) => sum + item.n }).pipe(
        Effect.tap(() => Deferred.succeed(seen, undefined)),
        Effect.forkScoped,
      );
      yield* control.snapshot;
      expect(yield* Deferred.isDone(seen)).toBe(false);
      yield* Streams.append(ref, [{ n: 3 }], { close: true });
      expect(yield* Fiber.join(fiber)).toBe(6);
      expect(yield* control.subscribers).toBe(0);
    }).pipe(Effect.scoped),
  ));

it("Producer replay keeps one item and next advances only its inert tuple", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(ref);
      const position = { producerId: "p", producerEpoch: 0, producerSeq: 0 };
      expect((yield* Producer.append(ref, [{ n: 1 }], position))._tag).toBe("Appended");
      expect(
        (yield* Producer.append(ref, [{ n: 1 }], position, { expectedOffset: "bad" }))._tag,
      ).toBe("Duplicate");
      expect(Producer.next(position)).toEqual({
        producerId: "p",
        producerEpoch: 0,
        producerSeq: 1,
      });
      expect(position.producerSeq).toBe(0);
      expect(yield* Streams.read(ref).pipe(Streams.items, Stream.runCollect)).toEqual([{ n: 1 }]);
    }),
  ));

it("read and follow fail with gone while session preserves the classification", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(ref);
      const child = StreamRef.json("child", { schema: Schema.Finite });
      yield* (yield* StreamsWriter).fork(child.id, ref.id);
      yield* Streams.remove(ref);
      for (const source of [Streams.read(ref), Streams.follow(ref)]) {
        const exit = yield* source.pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({
          _tag: "StreamGone",
        });
      }
      expect((yield* Effect.flip(Streams.session(ref, { offset: ZERO_OFFSET })))._tag).toBe(
        "StreamGone",
      );
    }),
  ));

it("JSON empty close can be retried without a closed conflict", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(ref);
      expect((yield* Streams.append(ref, [], { close: true }))._tag).toBe("Appended");
      expect((yield* Streams.append(ref, [], { close: true }))._tag).toBe("Appended");
    }),
  ));

it("an append without items fails the same way as an empty body on the wire", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(ref);
      expect(yield* Effect.flip(Streams.append(ref, []))).toMatchObject({
        _tag: "InvalidAppendRequest",
        id: ref.id,
        message: "Empty append",
      });
      expect(yield* Streams.head(ref)).toMatchObject({ nextOffset: ZERO_OFFSET, closed: false });
    }),
  ));
