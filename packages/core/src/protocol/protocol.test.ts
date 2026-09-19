import { create } from "./create.ts";
import { MutationRejected } from "../storage/mutation.ts";
import { expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { StreamsReader, StreamsWriter } from "./tags.ts";
import * as Protocol from "./layer.ts";
import { touch } from "./expiry.ts";
import { StreamId, ProducerId } from "../schema/index.ts";
import { ZERO_OFFSET, next } from "../offset/index.ts";
import { Storage } from "../storage/storage.ts";
import { StorageFault } from "../fault.ts";
import { layerTest, StreamsTest } from "../testing/streams-test.ts";
import { faultyStorage } from "../testing/fault-injection.ts";
import * as Memory from "../storage/memory/layer.ts";

const id = StreamId.make("s");
const data = new TextEncoder().encode("a");
const appendOptions = { data, contentType: "text/plain" };
const producer = (epoch: number, seq: number) => ({
  producerId: "p",
  producerEpoch: epoch,
  producerSeq: seq,
});
function provideTest<R>(layer: Layer.Layer<R>) {
  return <A, E>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(layer);
        return yield* program.pipe(Effect.provide(context));
      }),
    );
}
function check<E>(
  program: Effect.Effect<void, E, StreamsReader | StreamsWriter | StreamsTest>,
  constrained = false,
  readLimit = 1000,
) {
  return expect(
    Effect.runPromiseExit(
      program.pipe(
        provideTest(
          Layer.mergeAll(
            layerTest({ constrained, longPollTimeoutMs: 100, readLimit }),
            TestClock.layer(),
          ),
        ),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined));
}

for (const constrained of [false, true]) {
  const mode = constrained ? "copy/poll" : "chain/push";
  it(`${mode}: create idempotency and every config conflict`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        expect((yield* writer.create(id, { contentType: "text/plain" }))._tag).toBe("Created");
        expect(
          (yield* writer.create(id, { contentType: "text/plain", initialData: data }))._tag,
        ).toBe("Exists");
        for (const options of [
          { contentType: "application/json" },
          { ttlSeconds: 2 },
          { expiresAt: "2030-01-01" },
          { closed: true },
          { forkedFrom: "other" },
          { forkOffset: ZERO_OFFSET },
          { forkSubOffset: 1 },
        ]) {
          expect(yield* Effect.flip(writer.create(id, options))).toMatchObject({
            _tag: "CreateConflict",
            reason: "config-mismatch",
          });
        }
        const closed = yield* writer.create(StreamId.make("closed"), {
          closed: true,
          initialData: data,
        });
        expect(closed).toMatchObject({
          _tag: "Created",
          closed: true,
          nextOffset: next(ZERO_OFFSET),
        });
      }),
      constrained,
    ));

  it(`${mode}: CAS malformed, stale, success, and conflicts do not write`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        expect(yield* Effect.flip(writer.append(id, appendOptions))).toMatchObject({
          _tag: "StreamNotFound",
        });
        yield* writer.create(id, { contentType: "text/plain" });
        expect(
          yield* Effect.flip(writer.append(id, { ...appendOptions, expectedOffset: "broken" })),
        ).toMatchObject({
          _tag: "OffsetMismatch",
          actual: ZERO_OFFSET,
        });
        expect(
          yield* writer.append(id, { ...appendOptions, expectedOffset: ZERO_OFFSET, seq: "b" }),
        ).toMatchObject({ _tag: "Appended", offset: next(ZERO_OFFSET) });
        expect(
          yield* Effect.flip(
            writer.append(id, { ...appendOptions, expectedOffset: ZERO_OFFSET, close: true }),
          ),
        ).toMatchObject({ _tag: "OffsetMismatch" });
        expect(
          yield* Effect.flip(
            writer.append(id, { ...appendOptions, contentType: "application/json" }),
          ),
        ).toMatchObject({ _tag: "AppendConflict", message: "Content-Type mismatch" });
        expect(yield* Effect.flip(writer.append(id, { ...appendOptions, seq: "a" }))).toMatchObject(
          {
            _tag: "AppendConflict",
            message: "Sequence conflict",
          },
        );
        const result = yield* reader.read(id);
        expect(result).toMatchObject({ closed: false });
        expect(result.messages.length).toBe(1);
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            data: new Uint8Array(),
            close: true,
            expectedOffset: next(ZERO_OFFSET),
          }),
        ).toMatchObject({ _tag: "Appended", closed: true });
        expect(yield* Effect.flip(writer.append(id, appendOptions))).toMatchObject({
          _tag: "StreamClosed",
        });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            data: new Uint8Array(),
            close: true,
            expectedOffset: "stale",
          }),
        ).toMatchObject({ _tag: "Appended", closed: true });
        expect(yield* writer.remove(id)).toBeUndefined();
        expect(yield* Effect.flip(writer.remove(id))).toMatchObject({ _tag: "StreamNotFound" });
        expect(yield* Effect.flip(reader.head(id))).toMatchObject({ _tag: "StreamNotFound" });
      }),
      constrained,
    ));

  it(`${mode}: an append carries a message or it closes the stream`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        const json = { contentType: "application/json" };
        const emptyArray = new TextEncoder().encode("[]");
        yield* writer.create(id, json);
        for (const options of [
          { data: emptyArray, ...json },
          { data: emptyArray, ...json, close: true },
          { data: new Uint8Array(), ...json },
        ]) {
          expect(yield* Effect.flip(writer.append(id, options))).toMatchObject({
            _tag: "InvalidAppendRequest",
            id,
          });
        }
        expect(yield* reader.read(id)).toMatchObject({ nextOffset: ZERO_OFFSET, closed: false });
        expect(
          yield* writer.append(id, { data: new Uint8Array(), ...json, close: true }),
        ).toMatchObject({ _tag: "Appended", closed: true });
        expect(yield* reader.read(id)).toMatchObject({ nextOffset: ZERO_OFFSET, closed: true });
      }),
      constrained,
    ));

  it(`${mode}: producer epochs, gaps and duplicate validation precede malformed CAS`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const control = yield* StreamsTest;
        yield* writer.create(id, { contentType: "text/plain" });
        expect(
          yield* Effect.flip(
            writer.append(id, {
              ...appendOptions,
              producer: producer(0, 2),
              expectedOffset: "bad",
            }),
          ),
        ).toMatchObject({ _tag: "ProducerGap", expectedSeq: 0, receivedSeq: 2 });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(0, 0),
            expectedOffset: ZERO_OFFSET,
          }),
        ).toMatchObject({ _tag: "Appended", producerSeq: 0 });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(0, 0),
            expectedOffset: "bad",
            contentType: "wrong",
          }),
        ).toMatchObject({ _tag: "Duplicate", producerSeq: 0 });
        expect(
          yield* Effect.flip(
            writer.append(id, {
              ...appendOptions,
              producer: producer(1, 1),
              expectedOffset: "bad",
            }),
          ),
        ).toMatchObject({ _tag: "InvalidEpochSeq" });
        expect(
          yield* Effect.flip(writer.append(id, { ...appendOptions, producer: producer(0, 2) })),
        ).toMatchObject({
          _tag: "ProducerGap",
          expectedSeq: 1,
          receivedSeq: 2,
        });
        expect(
          yield* writer.append(id, { ...appendOptions, producer: producer(0, 1) }),
        ).toMatchObject({ _tag: "Appended", producerSeq: 1 });
        expect(
          yield* writer.append(id, { ...appendOptions, producer: producer(0, 0) }),
        ).toMatchObject({ _tag: "Duplicate", producerSeq: 1 });
        expect(
          yield* writer.append(id, { ...appendOptions, producer: producer(1, 0) }),
        ).toMatchObject({ _tag: "Appended", producerEpoch: 1 });
        expect(
          yield* Effect.flip(
            writer.append(id, {
              ...appendOptions,
              producer: producer(0, 1),
              expectedOffset: "bad",
            }),
          ),
        ).toMatchObject({ _tag: "StaleEpoch", currentEpoch: 1 });
        expect(yield* control.storage.producer(id, ProducerId.make("p"))).toEqual(
          Option.some({ epoch: 1, lastSeq: 0 }),
        );
        expect((yield* control.storage.messages(id, {})).length).toBe(3);
      }),
      constrained,
    ));

  it(`${mode}: fork prefix, sub-offset, inherited expiry and source classifications`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        const child = StreamId.make("child");
        expect((yield* Effect.flip(writer.fork(child, id)))._tag).toBe("ForkSourceNotFound");
        yield* writer.create(id, {
          contentType: "text/plain",
          initialData: new TextEncoder().encode("abc"),
          ttlSeconds: 5,
        });
        expect(
          yield* Effect.flip(writer.fork(child, id, { contentType: "application/json" })),
        ).toMatchObject({
          _tag: "CreateConflict",
          reason: "fork-content-type",
        });
        expect((yield* Effect.flip(writer.fork(child, id, { forkOffset: "bad" })))._tag).toBe(
          "InvalidForkRequest",
        );
        expect(
          (yield* Effect.flip(writer.fork(child, id, { forkOffset: next(next(ZERO_OFFSET)) })))
            ._tag,
        ).toBe("InvalidForkRequest");
        expect(
          (yield* Effect.flip(
            writer.fork(child, id, { forkOffset: ZERO_OFFSET, forkSubOffset: 4 }),
          ))._tag,
        ).toBe("InvalidForkRequest");
        expect(
          (yield* writer.fork(child, id, { forkOffset: ZERO_OFFSET, forkSubOffset: 2 }))._tag,
        ).toBe("Created");
        expect(
          (yield* writer.fork(child, id, { forkOffset: ZERO_OFFSET, forkSubOffset: 2 }))._tag,
        ).toBe("Exists");
        const result = yield* reader.read(child);
        expect(new TextDecoder().decode(result.messages[0]?.data)).toBe("ab");
        expect(yield* reader.head(child)).toMatchObject({ ttlSeconds: 5 });
        yield* writer.fork(StreamId.make("full"), id);
        yield* writer.remove(id);
        expect(yield* reader.read(StreamId.make("full"))).toBeDefined();
        if (!constrained) {
          expect(yield* Effect.flip(writer.append(id, appendOptions))).toMatchObject({
            _tag: "StreamGone",
          });
          expect(yield* Effect.flip(reader.head(id))).toMatchObject({ _tag: "StreamGone" });
          expect(yield* Effect.flip(reader.read(id))).toMatchObject({ _tag: "StreamGone" });
          expect((yield* Effect.flip(reader.readNext(id, { offset: ZERO_OFFSET })))._tag).toBe(
            "StreamGone",
          );
          expect(yield* Effect.flip(writer.remove(id))).toMatchObject({ _tag: "StreamGone" });
          expect(yield* Effect.flip(writer.create(id))).toMatchObject({
            _tag: "CreateConflict",
            reason: "soft-deleted",
          });
          expect(yield* Effect.flip(writer.fork(StreamId.make("gone-source"), id))).toMatchObject({
            _tag: "CreateConflict",
            reason: "fork-source-soft-deleted",
          });
        }
      }),
      constrained,
    ));

  it(`${mode}: read pagination, sentinel offsets and created-closed readNext`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        yield* writer.create(id, {
          contentType: "application/json",
          initialData: new TextEncoder().encode("[1,2,3]"),
          closed: true,
        });
        const first = yield* reader.read(id, { offset: "-1" });
        expect(first).toMatchObject({
          nextOffset: next(ZERO_OFFSET),
          upToDate: false,
          closed: false,
        });
        expect(yield* reader.read(id, { offset: "now" })).toMatchObject({
          messages: [],
          closed: true,
        });
        expect(yield* reader.readNext(id, { offset: ZERO_OFFSET })).toMatchObject({
          closed: true,
        });
        expect(yield* reader.readNext(id, { offset: next(next(next(ZERO_OFFSET))) })).toMatchObject(
          { timedOut: true, closed: true },
        );
      }),
      constrained,
      1,
    ));

  for (const change of ["append", "close", "remove"] as const) {
    it(`${mode}: parked readNext wakes on ${change} and releases subscription`, () =>
      check(
        Effect.gen(function* () {
          const writer = yield* StreamsWriter;
          const reader = yield* StreamsReader;
          const control = yield* StreamsTest;
          yield* writer.create(id, { contentType: "text/plain" });
          const fiber = yield* reader.readNext(id, { offset: ZERO_OFFSET }).pipe(Effect.forkScoped);
          expect((yield* control.snapshot).value.currentOffset).toBe(ZERO_OFFSET);
          expect(yield* control.subscribers).toBe(1);
          if (change === "remove") yield* writer.remove(id);
          else
            yield* writer.append(id, {
              ...appendOptions,
              data: change === "close" ? new Uint8Array() : data,
              close: change === "close",
            });
          if (constrained) yield* TestClock.adjust(25);
          if (change === "remove")
            expect((yield* Effect.flip(Fiber.join(fiber)))._tag).toBe("StreamNotFound");
          else {
            const result = yield* Fiber.join(fiber);
            expect(result.timedOut).toBe(false);
            if (change === "close") expect(result.closed).toBe(true);
          }
          expect(yield* control.subscribers).toBe(0);
        }).pipe(Effect.scoped),
        constrained,
      ));
  }

  it(`${mode}: timeout ignores non-advancing wake and releases subscription`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        const control = yield* StreamsTest;
        yield* writer.create(id, { contentType: "text/plain" });
        const fiber = yield* reader.readNext(id, { offset: ZERO_OFFSET }).pipe(Effect.forkScoped);
        yield* control.snapshot;
        yield* control.storage.mutate({
          operations: [{ _tag: "Append", streamId: id, messages: [], patch: {} }],
        });
        if (constrained) yield* TestClock.adjust(25);
        yield* control.snapshot;
        expect(fiber.pollUnsafe()).toBeUndefined();
        yield* TestClock.adjust(100);
        expect(yield* Fiber.join(fiber)).toMatchObject({
          timedOut: true,
          nextOffset: ZERO_OFFSET,
        });
        expect(yield* control.subscribers).toBe(0);
      }).pipe(Effect.scoped),
      constrained,
    ));

  it(`${mode}: scope close interrupts parked readNext without a fault`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        const control = yield* StreamsTest;
        yield* writer.create(id);
        const stoppedFiber = yield* Effect.scoped(
          Effect.gen(function* () {
            const fiber = yield* reader
              .readNext(id, { offset: ZERO_OFFSET })
              .pipe(Effect.forkScoped);
            yield* control.snapshot;
            expect(yield* control.subscribers).toBe(1);
            return fiber;
          }),
        );
        const exit = yield* Fiber.await(stoppedFiber);
        expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
        expect(yield* control.subscribers).toBe(0);
      }),
      constrained,
    ));

  it(`${mode}: TTL read/append touches, fixed expiry, and lazy expiry`, () =>
    check(
      Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        const reader = yield* StreamsReader;
        const control = yield* StreamsTest;
        yield* writer.create(id, { contentType: "text/plain", ttlSeconds: 1 });
        yield* TestClock.adjust(500);
        yield* reader.read(id);
        expect(Option.getOrThrow(yield* control.storage.record(id)).lifecycle.expiresAtMs).toBe(
          1500,
        );
        yield* TestClock.adjust(500);
        yield* writer.append(id, appendOptions);
        expect(Option.getOrThrow(yield* control.storage.record(id)).lifecycle.expiresAtMs).toBe(
          2000,
        );
        yield* TestClock.adjust(1000);
        expect(yield* Effect.flip(reader.head(id))).toMatchObject({ _tag: "StreamNotFound" });
        yield* writer.create(id, { expiresAt: "1970-01-01T00:00:03.000Z" });
        yield* TestClock.adjust(500);
        yield* reader.read(id);
        expect(Option.getOrThrow(yield* control.storage.record(id)).lifecycle.expiresAtMs).toBe(
          3000,
        );
        yield* TestClock.adjust(500);
        expect(yield* Effect.flip(reader.read(id))).toMatchObject({ _tag: "StreamNotFound" });
      }),
      constrained,
    ));
}

it("fork none refuses before any storage operation, even for an existing target", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      const forbidden = Effect.die("unexpected storage call");
      const source = Storage.of({
        capabilities: {
          atomicScope: control.storage.capabilities.atomicScope,
          wake: control.storage.capabilities.wake,
          expiryIndex: control.storage.capabilities.expiryIndex,
          fork: "none",
        },
        record: () => forbidden,
        messages: () => forbidden,
        producer: () => forbidden,
        mutate: () => forbidden,
        changes: () => Stream.die("unexpected changes"),
        nextExpiry: forbidden,
      });
      yield* Effect.gen(function* () {
        const writer = yield* StreamsWriter;
        expect(yield* Effect.flip(writer.fork(id, StreamId.make("source")))).toMatchObject({
          _tag: "NotSupported",
          feature: "fork",
        });
        expect((yield* Effect.flip(writer.create(id, { forkedFrom: "source" })))._tag).toBe(
          "NotSupported",
        );
      }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, source)))));
    }),
  ));

for (const rejects of [7, 8]) {
  it(`append makes exactly eight semantic attempts with ${rejects} rejections`, () =>
    check(
      Effect.gen(function* () {
        const control = yield* StreamsTest;
        yield* (yield* StreamsWriter).create(id, { contentType: "text/plain" });
        let calls = 0;
        const source = Storage.of({
          ...control.storage,
          mutate: Effect.fn("Test.reject")(function* (mutation) {
            calls++;
            if (calls <= rejects)
              return yield* new MutationRejected({
                index: 0,
                reason: "offset",
                record: yield* control.storage.record(id),
              });
            return yield* control.storage.mutate(mutation);
          }),
        });
        const result = yield* Effect.gen(function* () {
          const attempt = (yield* StreamsWriter).append(id, appendOptions);
          if (rejects === 8) return yield* Effect.flip(attempt);
          return yield* attempt;
        }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, source)))));
        expect(result._tag).toBe(rejects === 7 ? "Appended" : "StreamBusy");
        expect(calls).toBe(8);
      }),
    ));
}

for (const when of ["before", "after"] as const) {
  it(`ambiguous ${when} fault stays typed and replay acknowledges one tuple`, () =>
    expect(
      Effect.runPromiseExit(
        Effect.gen(function* () {
          const writer = yield* StreamsWriter;
          const reader = yield* StreamsReader;
          yield* writer.create(id, { contentType: "text/plain" });
          const options = {
            ...appendOptions,
            producer: producer(0, 0),
            expectedOffset: ZERO_OFFSET,
          };
          const exit = yield* writer.append(id, options).pipe(Effect.exit);
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(StorageFault);
          expect((yield* writer.append(id, options))._tag).toBe(
            when === "before" ? "Appended" : "Duplicate",
          );
          const result = yield* reader.read(id);
          expect(result.messages.length).toBe(1);
        }).pipe(
          provideTest(
            Protocol.layer().pipe(
              Layer.provide(faultyStorage(Memory.layer(), { failOn: 2, when })),
            ),
          ),
        ),
      ),
    ).resolves.toEqual(Exit.succeed(undefined)));
}

it("indexed expireDue handles due streams and leaves future deadlines", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      const writer = yield* StreamsWriter;
      yield* writer.create(id, { ttlSeconds: 1 });
      yield* writer.create(StreamId.make("future"), { ttlSeconds: 2 });
      yield* TestClock.adjust(1000);
      yield* Protocol.expireDue().pipe(Effect.provideService(Storage, control.storage));
      expect(yield* control.storage.record(id)).toEqual(Option.none());
      expect(Option.isSome(yield* control.storage.record(StreamId.make("future")))).toBe(true);
    }),
  ));

it("readNext sees a commit in the read-to-subscribe window", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      const writer = yield* StreamsWriter;
      yield* writer.create(id, { contentType: "text/plain" });
      const storage = Storage.of({
        ...control.storage,
        changes: (target) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* writer.append(id, appendOptions);
              return control.storage.changes(target);
            }).pipe(Effect.catch(Effect.die)),
          ),
      });
      const result = yield* Effect.gen(function* () {
        return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
      }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))));
      expect(result).toMatchObject({ nextOffset: next(ZERO_OFFSET) });
      expect(yield* control.subscribers).toBe(0);
    }),
  ));

it("timeout re-reads a fresh snapshot even when storage emits no wake", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      yield* (yield* StreamsWriter).create(id, { contentType: "text/plain" });
      const ready = yield* Deferred.make<void>();
      const storage = Storage.of({
        ...control.storage,
        changes: () =>
          Stream.fromEffect(
            Effect.gen(function* () {
              yield* Deferred.succeed(ready, undefined);
              return {
                present: true,
                currentOffset: ZERO_OFFSET,
                closed: false,
                softDeleted: false,
              };
            }),
          ).pipe(Stream.concat(Stream.never)),
      });
      const fiber = yield* Effect.gen(function* () {
        return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
      }).pipe(
        provideTest(
          Protocol.layer({ longPollTimeoutMs: 100 }).pipe(
            Layer.provide(Layer.succeed(Storage, storage)),
          ),
        ),
        Effect.forkScoped,
      );
      yield* Deferred.await(ready);
      yield* (yield* StreamsWriter).append(id, {
        ...appendOptions,
        data: new Uint8Array(),
        close: true,
      });
      yield* TestClock.adjust(100);
      expect(yield* Fiber.join(fiber)).toMatchObject({ timedOut: true, closed: true });
    }).pipe(Effect.scoped),
  ));

it("expiry precondition protects a concurrently renewed deadline", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      yield* (yield* StreamsWriter).create(id, { ttlSeconds: 1 });
      yield* TestClock.adjust(1000);
      const storage = Storage.of({
        ...control.storage,
        mutate: Effect.fn("Test.renew")(function* (mutation) {
          yield* control.storage.mutate({
            operations: [
              {
                _tag: "Append",
                streamId: id,
                messages: [],
                patch: { lifecycle: { expiresAtMs: 2000 } },
              },
            ],
          });
          return yield* control.storage.mutate(mutation);
        }),
      });
      expect(
        yield* Effect.gen(function* () {
          return yield* (yield* StreamsReader).head(id);
        }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage))))),
      ).toMatchObject({});
      expect(Option.getOrThrow(yield* control.storage.record(id)).lifecycle.expiresAtMs).toBe(2000);
    }),
  ));

it("append record acquisition remains interruptible before mutate", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      const ready = yield* Deferred.make<void>();
      let mutations = 0;
      const storage = Storage.of({
        ...control.storage,
        record: () => Deferred.succeed(ready, undefined).pipe(Effect.andThen(Effect.never)),
        mutate: (mutation) => {
          mutations++;
          return control.storage.mutate(mutation);
        },
      });
      const stoppedFiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const fiber = yield* Effect.gen(function* () {
            return yield* (yield* StreamsWriter).append(id, appendOptions);
          }).pipe(
            provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))),
            Effect.forkScoped,
          );
          yield* Deferred.await(ready);
          return fiber;
        }),
      );
      const exit = yield* Fiber.await(stoppedFiber);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(mutations).toBe(0);
    }),
  ));

it("remove classifies an already expired stream as not-found", () =>
  check(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      yield* writer.create(id, { ttlSeconds: 1 });
      yield* TestClock.adjust(1000);
      expect(yield* Effect.flip(writer.remove(id))).toMatchObject({ _tag: "StreamNotFound" });
    }),
  ));

it("the mutate region completes before an append interruption is delivered", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      yield* (yield* StreamsWriter).create(id, { contentType: "text/plain" });
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const storage = Storage.of({
        ...control.storage,
        mutate: Effect.fn("Test.heldMutation")(function* (mutation) {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
          return yield* control.storage.mutate(mutation);
        }),
      });
      const fiber = yield* Effect.gen(function* () {
        return yield* (yield* StreamsWriter).append(id, appendOptions);
      }).pipe(
        provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))),
        Effect.forkScoped,
      );
      yield* Deferred.await(entered);
      fiber.interruptUnsafe();
      expect(fiber.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      const exit = yield* Fiber.await(fiber);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect((yield* control.storage.messages(id, {})).length).toBe(1);
    }).pipe(Effect.scoped),
  ));

it("parked readNext observes soft deletion as gone", () =>
  check(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      const reader = yield* StreamsReader;
      const control = yield* StreamsTest;
      yield* writer.create(id);
      yield* writer.fork(StreamId.make("child"), id);
      const fiber = yield* reader.readNext(id, { offset: ZERO_OFFSET }).pipe(Effect.forkScoped);
      yield* control.snapshot;
      yield* writer.remove(id);
      expect((yield* Effect.flip(Fiber.join(fiber)))._tag).toBe("StreamGone");
      expect(yield* control.subscribers).toBe(0);
    }).pipe(Effect.scoped),
  ));

it("purge/recreate with a lower tail is a change, including when its wake was coalesced", () =>
  check(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      const control = yield* StreamsTest;
      yield* writer.create(id, { contentType: "text/plain", initialData: data });
      const storage = Storage.of({
        ...control.storage,
        changes: (target) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* writer.remove(id);
              yield* writer.create(id, { contentType: "text/plain" });
              return control.storage.changes(target);
            }).pipe(Effect.catch(Effect.die)),
          ),
      });
      const result = yield* Effect.gen(function* () {
        return yield* (yield* StreamsReader).readNext(id, { offset: next(ZERO_OFFSET) });
      }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))));
      expect(result).toMatchObject({ nextOffset: ZERO_OFFSET, messages: [] });
      expect(yield* control.subscribers).toBe(0);
    }),
  ));

it("read and readNext reject noncanonical offsets without replaying history", () =>
  check(
    Effect.gen(function* () {
      yield* (yield* StreamsWriter).create(id, { contentType: "text/plain", initialData: data });
      const reader = yield* StreamsReader;
      for (const offset of ["zz", "0", ""]) {
        expect(yield* reader.read(id, { offset }).pipe(Effect.flip)).toMatchObject({
          _tag: "InvalidReadRequest",
        });
        expect(yield* reader.readNext(id, { offset }).pipe(Effect.flip)).toMatchObject({
          _tag: "InvalidReadRequest",
        });
      }
    }),
  ));

it("readNext holds its cursor behind a reported tail until messages become visible", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      yield* (yield* StreamsWriter).create(id, { contentType: "text/plain", initialData: data });
      const ready = yield* Deferred.make<void>();
      let visible = false;
      const storage = Storage.of({
        ...control.storage,
        messages: (target, window) =>
          visible ? control.storage.messages(target, window) : Effect.succeed([]),
        changes: (target) =>
          control.storage
            .changes(target)
            .pipe(Stream.tap(() => Deferred.succeed(ready, undefined))),
      });
      yield* Effect.gen(function* () {
        const reader = yield* StreamsReader;
        const fiber = yield* reader.readNext(id, { offset: ZERO_OFFSET }).pipe(Effect.forkScoped);
        yield* Deferred.await(ready);
        yield* TestClock.adjust(100);
        expect(yield* Fiber.join(fiber)).toMatchObject({
          timedOut: true,
          nextOffset: ZERO_OFFSET,
          closed: false,
        });
        visible = true;
        expect(yield* reader.readNext(id, { offset: ZERO_OFFSET })).toMatchObject({
          nextOffset: next(ZERO_OFFSET),
        });
      }).pipe(
        Effect.scoped,
        provideTest(
          Protocol.layer({ longPollTimeoutMs: 100 }).pipe(
            Layer.provide(Layer.succeed(Storage, storage)),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  ));

it("append replans a rejected CAS into a duplicate acknowledgement", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      yield* (yield* StreamsWriter).create(id, { contentType: "text/plain" });
      let calls = 0;
      const storage = Storage.of({
        ...control.storage,
        mutate: Effect.fn("Test.concurrentAppend")(function* (mutation) {
          calls++;
          // Another writer wins this exact tuple between planning and preflight.
          yield* control.storage.mutate(mutation);
          return yield* control.storage.mutate(mutation);
        }),
      });
      const appended = yield* Effect.gen(function* () {
        return yield* (yield* StreamsWriter).append(id, {
          ...appendOptions,
          producer: producer(0, 0),
        });
      }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))));
      expect(appended).toMatchObject({ _tag: "Duplicate", producerEpoch: 0, producerSeq: 0 });
      expect(calls).toBe(1);
      expect((yield* control.storage.messages(id, {})).length).toBe(1);
    }),
  ));

for (const race of ["matching-create", "conflicting-create", "source-gone"] as const) {
  it(`create maps mutation rejection after ${race}`, () =>
    check(
      Effect.gen(function* () {
        const control = yield* StreamsTest;
        const source = StreamId.make("source");
        if (race === "source-gone") yield* create(control.storage, source);
        const storage = Storage.of({
          ...control.storage,
          mutate: Effect.fn("Test.concurrentCreate")(function* (mutation) {
            if (race === "source-gone")
              yield* control.storage.mutate({
                operations: [{ _tag: "Delete", streamId: source, reason: "delete" }],
              });
            else
              yield* create(control.storage, id, {
                contentType: race === "matching-create" ? "text/plain" : "application/json",
              }).pipe(Effect.orDie);
            return yield* control.storage.mutate(mutation);
          }),
        });
        const attempt = create(
          storage,
          id,
          race === "source-gone" ? { forkedFrom: source } : { contentType: "text/plain" },
        );
        if (race === "matching-create") expect((yield* attempt)._tag).toBe("Exists");
        else
          expect(yield* Effect.flip(attempt)).toMatchObject(
            race === "source-gone"
              ? { _tag: "ForkSourceNotFound", source }
              : { _tag: "CreateConflict", reason: "config-mismatch" },
          );
      }),
    ));
}

it("a plain create treats an impossible rejection as a defect, not a fork answer", () =>
  check(
    Effect.gen(function* () {
      const control = yield* StreamsTest;
      const storage = Storage.of({
        ...control.storage,
        mutate: () =>
          Effect.fail(
            new MutationRejected({ index: 0, reason: "fork-source-gone", record: Option.none() }),
          ),
      });
      const exit = yield* create(storage, id, { contentType: "text/plain" }).pipe(Effect.exit);
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
      if (Exit.isFailure(exit))
        expect(String(Cause.squash(exit.cause))).toContain("Unexpected create rejection");
    }),
  ));

for (const operation of ["touch", "sweep"] as const) {
  it(`${operation} tolerates a rejected mutation and preserves concurrent renewal`, () =>
    check(
      Effect.gen(function* () {
        const control = yield* StreamsTest;
        yield* (yield* StreamsWriter).create(id, { ttlSeconds: 1 });
        const before = Option.getOrThrow(yield* control.storage.record(id));
        if (operation === "sweep") yield* TestClock.adjust(1000);
        let calls = 0;
        const storage = Storage.of({
          ...control.storage,
          mutate: Effect.fn("Test.concurrentRenewal")(function* (mutation) {
            calls++;
            yield* control.storage.mutate({
              operations: [
                {
                  _tag: "Append",
                  streamId: id,
                  messages: [],
                  patch: { currentOffset: next(ZERO_OFFSET), lifecycle: { expiresAtMs: 3000 } },
                },
              ],
            });
            return yield* control.storage.mutate(mutation);
          }),
        });
        if (operation === "touch") yield* touch(storage, before);
        else yield* Protocol.expireDue().pipe(Effect.provideService(Storage, storage));
        expect(calls).toBe(1);
        expect(Option.getOrThrow(yield* control.storage.record(id))).toMatchObject({
          currentOffset: next(ZERO_OFFSET),
          lifecycle: { expiresAtMs: 3000 },
        });
        expect(yield* control.storage.nextExpiry).toEqual(Option.some({ at: 3000, streamId: id }));
      }),
    ));
}
