import { expect, it } from "bun:test";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import * as StreamAddress from "../../src/toolkit/address.ts";
import { MemoryStorage } from "../../src/index.ts";
import * as Streams from "../../src/toolkit/streams.ts";
import * as Producer from "../../src/toolkit/producer.ts";
import { DecodeFault, EncodeFault, TransportFault } from "../../src/fault.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/tags.ts";
import { StreamsTest, layerTest } from "../../src/testing/streams-test.ts";
import { ZERO_OFFSET } from "../../src/offset/index.ts";

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
const address = StreamAddress.json("events", { schema: Schema.Struct({ n: Schema.Finite }) });

it("refs are inert and layerMemory supplies real tags", () =>
  expect(
    Effect.runPromiseExit(
      Effect.gen(function* () {
        expect(String(address.id)).toBe("events");
        expect(yield* Streams.create(address)).toMatchObject({ _tag: "Created" });
        expect(yield* Streams.append(address, [{ n: 1 }, { n: 2 }])).toMatchObject({
          _tag: "Appended",
        });
        const batches = yield* Streams.read(address).pipe(Stream.runCollect);
        expect(batches.map((batch) => batch.items)).toEqual([[{ n: 1 }], [{ n: 2 }]]);
        expect(yield* Streams.read(address).pipe(Streams.items, Stream.runCollect)).toEqual([
          { n: 1 },
          { n: 2 },
        ]);
        expect(yield* Streams.head(address)).toMatchObject({
          contentType: "application/json",
        });
        expect(yield* Streams.remove(address)).toBeUndefined();
      }).pipe(provideTest(MemoryStorage.layerProtocol({ readLimit: 1 }))),
    ),
  ).resolves.toEqual(Exit.succeed(undefined)));

it("bytes address round trips batches and zero-byte close", () =>
  check(
    Effect.gen(function* () {
      const bytes = StreamAddress.bytes("bytes", { contentType: "text/plain" });
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
      const restricted = StreamAddress.json("restricted", {
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
        yield* Streams.create(address);
        if (body === "{bad") {
          const record = yield* control.storage.record(address.id);
          expect(Option.isSome(record)).toBe(true);
          yield* control.storage.mutate({
            operations: [
              {
                _tag: "Append",
                streamId: address.id,
                messages: [
                  { data: new TextEncoder().encode(body), offset: ZERO_OFFSET, timestamp: 0 },
                ],
                patch: {},
              },
            ],
          });
        } else
          yield* writer.append(address.id, {
            contentType: "application/json",
            data: new TextEncoder().encode(body),
          });
        const exit = yield* Streams.read(address).pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit) && Schema.is(DecodeFault)(Cause.squash(exit.cause))).toBe(true);
      }),
    ));
}

for (const operation of [Streams.read, Streams.follow]) {
  it(`${operation.name} and session fail with StreamNotFound`, () =>
    check(
      Effect.gen(function* () {
        const exit = yield* operation(address).pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({
          id: address.id,
          _tag: "StreamNotFound",
        });
        expect((yield* Effect.flip(Streams.session(address, { offset: ZERO_OFFSET })))._tag).toBe(
          "StreamNotFound",
        );
      }),
    ));
}

it("follow's parked subscription and fiber are interrupted by scope close", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      const control = yield* StreamsTest;
      let completed = false;
      const stoppedFiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const fiber = yield* Streams.follow(address).pipe(
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
      yield* Streams.append(address, [{ n: 1 }]);
      yield* TestClock.adjust(200);
      expect(yield* control.subscribers).toBe(0);
    }),
  ));

it("follow filters a timed-out long poll before yielding the next message", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      const control = yield* StreamsTest;
      const fiber = yield* Streams.follow(address).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );

      yield* control.snapshot;
      yield* TestClock.adjust(100);
      yield* control.snapshot;
      yield* Streams.append(address, [{ n: 1 }]);

      const batches = yield* Fiber.join(fiber);
      expect(batches).toHaveLength(1);
      expect(batches[0]?.items).toEqual([{ n: 1 }]);
    }).pipe(Effect.scoped),
  ));

it("follow echoes the cursor from each live read on the next live read", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }]);
      const control = yield* StreamsTest;
      const inner = yield* StreamsReader;
      const sent: Array<string | undefined> = [];
      const returned: Array<string> = [];
      const recording = StreamsReader.of({
        ...inner,
        readNext: (id, options) => {
          sent.push(options.cursor);
          return inner
            .readNext(id, options)
            .pipe(Effect.tap((result) => Effect.sync(() => returned.push(result.cursor))));
        },
      });
      const fiber = yield* Streams.follow(address).pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.provideService(StreamsReader, recording),
        Effect.forkScoped,
      );
      yield* control.snapshot;
      yield* Streams.append(address, [{ n: 2 }]);
      yield* control.snapshot;
      yield* Streams.append(address, [{ n: 3 }]);
      const batches = yield* Fiber.join(fiber);
      expect(batches.flatMap((batch) => batch.items)).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
      // The catch-up read has no cursor, so the first live read sends none.
      expect(sent[0]).toBeUndefined();
      expect(sent.length).toBeGreaterThanOrEqual(2);
      expect(sent[1]).toBeString();
      expect(sent.slice(1)).toEqual(returned.slice(0, sent.length - 1));
    }).pipe(Effect.scoped),
  ));

const lost = (operation: string) =>
  new TransportFault({ operation, reason: "request", message: "lost reply" });

for (const point of ["read", "readNext"] as const) {
  it(`follow retries a TransportFault on ${point} from the same offset`, () =>
    check(
      Effect.gen(function* () {
        yield* Streams.create(address);
        yield* Streams.append(address, [{ n: 1 }]);
        const control = yield* StreamsTest;
        const inner = yield* StreamsReader;
        let failures = 0;
        const failOnce = <A, E>(call: Effect.Effect<A, E>): Effect.Effect<A, E | TransportFault> =>
          Effect.suspend(() => (failures++ === 0 ? Effect.fail(lost(point)) : Effect.void)).pipe(
            Effect.andThen(call),
          );
        const flaky = StreamsReader.of({
          ...inner,
          read: (id, options) =>
            point === "read" ? failOnce(inner.read(id, options)) : inner.read(id, options),
          readNext: (id, options) =>
            point === "readNext"
              ? failOnce(inner.readNext(id, options))
              : inner.readNext(id, options),
        });
        const fiber = yield* Streams.follow(address, { retry: Schedule.recurs(3) }).pipe(
          Streams.items,
          Stream.take(2),
          Stream.runCollect,
          Effect.provideService(StreamsReader, flaky),
          Effect.forkScoped,
        );
        yield* control.snapshot;
        yield* Streams.append(address, [{ n: 2 }]);
        expect(yield* Fiber.join(fiber)).toEqual([{ n: 1 }, { n: 2 }]);
        expect(failures).toBeGreaterThan(1);
      }).pipe(Effect.scoped),
    ));
}

it("follow's default retry waits on the clock and then reads again", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }]);
      const inner = yield* StreamsReader;
      let reads = 0;
      const flaky = StreamsReader.of({
        ...inner,
        read: (id, options) =>
          Effect.suspend(() =>
            reads++ === 0 ? Effect.fail(lost("read")) : inner.read(id, options),
          ),
      });
      const fiber = yield* Streams.follow(address).pipe(
        Streams.items,
        Stream.take(1),
        Stream.runCollect,
        Effect.provideService(StreamsReader, flaky),
        Effect.forkScoped,
      );
      yield* TestClock.adjust(0);
      expect(reads).toBe(1);
      expect(fiber.pollUnsafe()).toBeUndefined();
      // The first delay is 200 ms, jittered to at most 240 ms.
      yield* TestClock.adjust(240);
      expect(yield* Fiber.join(fiber)).toEqual([{ n: 1 }]);
      expect(reads).toBe(2);
    }).pipe(Effect.scoped),
  ));

it("follow does not retry a missing stream or a decode failure", () =>
  check(
    Effect.gen(function* () {
      const inner = yield* StreamsReader;
      let reads = 0;
      const counting = StreamsReader.of({
        ...inner,
        read: (id, options) => Effect.suspend(() => (reads++, inner.read(id, options))),
      });
      const retry = Schedule.recurs(3);
      const missing = yield* Streams.follow(address, { retry }).pipe(
        Stream.runCollect,
        Effect.provideService(StreamsReader, counting),
        Effect.flip,
      );
      expect(missing._tag).toBe("StreamNotFound");
      expect(reads).toBe(1);

      yield* Streams.create(address);
      yield* (yield* StreamsWriter).append(address.id, {
        contentType: "application/json",
        data: new TextEncoder().encode('{"n":"wrong"}'),
      });
      reads = 0;
      const decode = yield* Streams.follow(address, { retry }).pipe(
        Stream.runCollect,
        Effect.provideService(StreamsReader, counting),
        Effect.flip,
      );
      expect(Schema.is(DecodeFault)(decode)).toBe(true);
      expect(reads).toBe(1);
    }),
  ));

it("follow yields an empty close that arrives while its live read is parked", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      const control = yield* StreamsTest;
      const fiber = yield* Streams.follow(address).pipe(Stream.runCollect, Effect.forkScoped);

      yield* control.snapshot;
      yield* Streams.append(address, [], { close: true });

      const batches = yield* Fiber.join(fiber);
      expect(batches).toHaveLength(1);
      expect(batches[0]?.items).toEqual([]);
      expect(batches[0]?.closed).toBe(true);
    }).pipe(Effect.scoped),
  ));

it("Fold reduces existing items, waits for growth, and completes on close", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }, { n: 2 }]);
      const control = yield* StreamsTest;
      const seen = yield* Deferred.make<void>();
      const fiber = yield* Streams.follow(address)
        .pipe(
          Streams.items,
          Stream.runFold(
            () => 0,
            (sum, item) => sum + item.n,
          ),
        )
        .pipe(
          Effect.tap(() => Deferred.succeed(seen, undefined)),
          Effect.forkScoped,
        );
      yield* control.snapshot;
      expect(yield* Deferred.isDone(seen)).toBe(false);
      yield* Streams.append(address, [{ n: 3 }], { close: true });
      expect(yield* Fiber.join(fiber)).toBe(6);
      expect(yield* control.subscribers).toBe(0);
    }).pipe(Effect.scoped),
  ));

it("Producer replay keeps one item and next advances only its inert tuple", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      const position = { producerId: "p", producerEpoch: 0, producerSeq: 0 };
      expect((yield* Streams.append(address, [{ n: 1 }], { producer: position }))._tag).toBe(
        "Appended",
      );
      expect(
        (yield* Streams.append(address, [{ n: 1 }], { producer: position, expectedOffset: "bad" }))
          ._tag,
      ).toBe("Duplicate");
      expect(Producer.next(position)).toEqual({
        producerId: "p",
        producerEpoch: 0,
        producerSeq: 1,
      });
      expect(position.producerSeq).toBe(0);
      expect(yield* Streams.read(address).pipe(Streams.items, Stream.runCollect)).toEqual([
        { n: 1 },
      ]);
    }),
  ));

it("read and follow fail with gone while session preserves the classification", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      const child = StreamAddress.json("child", { schema: Schema.Finite });
      yield* (yield* StreamsWriter).fork(child.id, address.id);
      yield* Streams.remove(address);
      for (const source of [Streams.read(address), Streams.follow(address)]) {
        const exit = yield* source.pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({
          _tag: "StreamGone",
        });
      }
      expect((yield* Effect.flip(Streams.session(address, { offset: ZERO_OFFSET })))._tag).toBe(
        "StreamGone",
      );
    }),
  ));

it("JSON empty close can be retried without a closed conflict", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      expect((yield* Streams.append(address, [], { close: true }))._tag).toBe("Appended");
      expect((yield* Streams.append(address, [], { close: true }))._tag).toBe("Appended");
    }),
  ));

it("an append without items fails the same way as an empty body on the wire", () =>
  check(
    Effect.gen(function* () {
      yield* Streams.create(address);
      // @ts-expect-error an append needs items unless it closes
      expect(yield* Effect.flip(Streams.append(address, []))).toMatchObject({
        _tag: "InvalidAppendRequest",
        id: address.id,
        message: "Empty append",
      });
      expect(yield* Streams.head(address)).toMatchObject({
        nextOffset: ZERO_OFFSET,
        closed: false,
      });
    }),
  ));

for (const point of ["read", "readNext"] as const) {
  it(`follow stops with a typed configuration fault on ${point}`, () =>
    check(
      Effect.gen(function* () {
        yield* Streams.create(address);
        const reader = yield* StreamsReader;
        let calls = 0;
        const fault = new TransportFault({
          operation: point,
          reason: "configuration",
          message: "bad URL",
        });
        const fail = () =>
          Effect.suspend(() => {
            calls += 1;
            return Effect.fail(fault);
          });
        const failure = yield* Streams.follow(address, { retry: Schedule.recurs(3) }).pipe(
          Stream.runCollect,
          Effect.provideService(StreamsReader, { ...reader, [point]: fail }),
          Effect.flip,
        );
        expect(failure).toBe(fault);
        expect(calls).toBe(1);
      }),
    ));
}
