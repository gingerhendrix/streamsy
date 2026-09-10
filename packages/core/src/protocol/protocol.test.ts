import { expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { StreamsReader, StreamsWriter } from "./tags.ts";
import * as Protocol from "./layer.ts";
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
) {
  return expect(
    Effect.runPromiseExit(
      program.pipe(
        provideTest(
          Layer.mergeAll(layerTest({ constrained, longPollTimeoutMs: 100 }), TestClock.layer()),
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
        expect((yield* writer.create(id, { contentType: "text/plain" })).status).toBe("created");
        expect(
          (yield* writer.create(id, { contentType: "text/plain", initialData: data })).status,
        ).toBe("exists");
        for (const options of [
          { contentType: "application/json" },
          { ttlSeconds: 2 },
          { expiresAt: "2030-01-01" },
          { closed: true },
          { forkedFrom: "other" },
          { forkOffset: ZERO_OFFSET },
          { forkSubOffset: 1 },
        ]) {
          expect(yield* writer.create(id, options)).toMatchObject({
            status: "conflict",
            conflictReason: "config-mismatch",
          });
        }
        const closed = yield* writer.create(StreamId.make("closed"), {
          closed: true,
          initialData: data,
        });
        expect(closed).toMatchObject({
          status: "created",
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
        expect(yield* writer.append(id, appendOptions)).toEqual({ status: "not-found" });
        yield* writer.create(id, { contentType: "text/plain" });
        expect(
          yield* writer.append(id, { ...appendOptions, expectedOffset: "broken" }),
        ).toMatchObject({
          status: "conflict",
          conflictReason: "expected-offset",
          offset: ZERO_OFFSET,
        });
        expect(
          yield* writer.append(id, { ...appendOptions, expectedOffset: ZERO_OFFSET, seq: "b" }),
        ).toMatchObject({ status: "appended", offset: next(ZERO_OFFSET) });
        expect(
          yield* writer.append(id, { ...appendOptions, expectedOffset: ZERO_OFFSET, close: true }),
        ).toMatchObject({ status: "conflict", conflictReason: "expected-offset" });
        expect(
          yield* writer.append(id, { ...appendOptions, contentType: "application/json" }),
        ).toEqual({ status: "conflict", conflictReason: "content-type" });
        expect(yield* writer.append(id, { ...appendOptions, seq: "a" })).toEqual({
          status: "conflict",
          conflictReason: "sequence",
        });
        const result = yield* reader.read(id);
        expect(result).toMatchObject({ status: "ok", closed: false });
        if (result.status === "ok") expect(result.messages.length).toBe(1);
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            data: new Uint8Array(),
            close: true,
            expectedOffset: next(ZERO_OFFSET),
          }),
        ).toMatchObject({ status: "appended", closed: true });
        expect(yield* writer.append(id, appendOptions)).toMatchObject({
          status: "conflict",
          conflictReason: "closed",
        });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            data: new Uint8Array(),
            close: true,
            expectedOffset: "stale",
          }),
        ).toMatchObject({ status: "appended", closed: true });
        expect(yield* writer.remove(id)).toEqual({ status: "ok" });
        expect(yield* writer.remove(id)).toEqual({ status: "not-found" });
        expect(yield* reader.head(id)).toEqual({ status: "not-found" });
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
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(0, 2),
            expectedOffset: "bad",
          }),
        ).toEqual({ status: "producer-gap", expectedSeq: 0, receivedSeq: 2 });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(0, 0),
            expectedOffset: ZERO_OFFSET,
          }),
        ).toMatchObject({ status: "appended", producerSeq: 0 });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(0, 0),
            expectedOffset: "bad",
            contentType: "wrong",
          }),
        ).toMatchObject({ status: "duplicate", producerSeq: 0 });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(1, 1),
            expectedOffset: "bad",
          }),
        ).toEqual({ status: "invalid-epoch-seq" });
        expect(yield* writer.append(id, { ...appendOptions, producer: producer(0, 2) })).toEqual({
          status: "producer-gap",
          expectedSeq: 1,
          receivedSeq: 2,
        });
        expect(
          yield* writer.append(id, { ...appendOptions, producer: producer(0, 1) }),
        ).toMatchObject({ status: "appended", producerSeq: 1 });
        expect(
          yield* writer.append(id, { ...appendOptions, producer: producer(0, 0) }),
        ).toMatchObject({ status: "duplicate", producerSeq: 1 });
        expect(
          yield* writer.append(id, { ...appendOptions, producer: producer(1, 0) }),
        ).toMatchObject({ status: "appended", producerEpoch: 1 });
        expect(
          yield* writer.append(id, {
            ...appendOptions,
            producer: producer(0, 1),
            expectedOffset: "bad",
          }),
        ).toEqual({ status: "stale-epoch", currentEpoch: 1 });
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
        expect((yield* writer.fork(child, id)).status).toBe("not-found");
        yield* writer.create(id, {
          contentType: "text/plain",
          initialData: new TextEncoder().encode("abc"),
          ttlSeconds: 5,
        });
        expect(yield* writer.fork(child, id, { contentType: "application/json" })).toMatchObject({
          status: "conflict",
          conflictReason: "fork-content-type",
        });
        expect((yield* writer.fork(child, id, { forkOffset: "bad" })).status).toBe("bad-request");
        expect(
          (yield* writer.fork(child, id, { forkOffset: next(next(ZERO_OFFSET)) })).status,
        ).toBe("bad-request");
        expect(
          (yield* writer.fork(child, id, { forkOffset: ZERO_OFFSET, forkSubOffset: 4 })).status,
        ).toBe("bad-request");
        expect(
          (yield* writer.fork(child, id, { forkOffset: ZERO_OFFSET, forkSubOffset: 2 })).status,
        ).toBe("created");
        expect(
          (yield* writer.fork(child, id, { forkOffset: ZERO_OFFSET, forkSubOffset: 2 })).status,
        ).toBe("exists");
        const result = yield* reader.read(child);
        if (result.status !== "ok") throw new Error("expected child");
        expect(new TextDecoder().decode(result.messages[0]?.data)).toBe("ab");
        expect(yield* reader.head(child)).toMatchObject({ ttlSeconds: 5 });
        yield* writer.fork(StreamId.make("full"), id);
        yield* writer.remove(id);
        expect((yield* reader.read(StreamId.make("full"))).status).toBe("ok");
        if (!constrained) {
          expect(yield* writer.append(id, appendOptions)).toEqual({ status: "gone" });
          expect(yield* reader.head(id)).toEqual({ status: "gone" });
          expect(yield* reader.read(id)).toEqual({ status: "gone" });
          expect((yield* reader.readNext(id, { offset: ZERO_OFFSET })).status).toBe("gone");
          expect(yield* writer.remove(id)).toEqual({ status: "gone" });
          expect(yield* writer.create(id)).toMatchObject({
            status: "conflict",
            conflictReason: "soft-deleted",
          });
          expect(yield* writer.fork(StreamId.make("gone-source"), id)).toMatchObject({
            status: "conflict",
            conflictReason: "fork-source-soft-deleted",
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
        const first = yield* reader.read(id, { offset: "-1", limit: 1 });
        expect(first).toMatchObject({
          status: "ok",
          nextOffset: next(ZERO_OFFSET),
          upToDate: false,
          closed: false,
        });
        expect(yield* reader.read(id, { offset: "now" })).toMatchObject({
          status: "ok",
          messages: [],
          closed: true,
        });
        expect(yield* reader.readNext(id, { offset: ZERO_OFFSET })).toMatchObject({
          status: "ok",
          closed: true,
        });
        expect(yield* reader.readNext(id, { offset: next(next(next(ZERO_OFFSET))) })).toMatchObject(
          { status: "timeout", closed: true },
        );
      }),
      constrained,
    ));

  for (const change of ["append", "close", "remove"] as const) {
    it(`${mode}: parked readNext wakes on ${change} and releases subscription`, () =>
      check(
        Effect.gen(function* () {
          const writer = yield* StreamsWriter;
          const reader = yield* StreamsReader;
          const control = yield* StreamsTest;
          yield* writer.create(id, { contentType: "text/plain" });
          const fiber = yield* reader.readNext(id, { offset: "0" }).pipe(Effect.forkScoped);
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
          const result = yield* Fiber.join(fiber);
          expect(result.status).toBe(change === "remove" ? "not-found" : "ok");
          if (change === "close") expect(result).toMatchObject({ closed: true });
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
        const fiber = yield* reader.readNext(id, { offset: "0" }).pipe(Effect.forkScoped);
        yield* control.snapshot;
        yield* control.storage.mutate({
          operations: [{ _tag: "Append", streamId: id, messages: [], patch: {} }],
        });
        if (constrained) yield* TestClock.adjust(25);
        yield* control.snapshot;
        expect(fiber.pollUnsafe()).toBeUndefined();
        yield* TestClock.adjust(100);
        expect(yield* Fiber.join(fiber)).toMatchObject({
          status: "timeout",
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
        expect(yield* reader.head(id)).toEqual({ status: "not-found" });
        yield* writer.create(id, { expiresAt: "1970-01-01T00:00:03.000Z" });
        yield* TestClock.adjust(500);
        yield* reader.read(id);
        expect(Option.getOrThrow(yield* control.storage.record(id)).lifecycle.expiresAtMs).toBe(
          3000,
        );
        yield* TestClock.adjust(500);
        expect(yield* reader.read(id)).toEqual({ status: "not-found" });
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
        expect(yield* writer.fork(id, StreamId.make("source"))).toEqual({
          status: "not-supported",
          feature: "fork",
        });
        expect((yield* writer.create(id, { forkedFrom: "source" })).status).toBe("not-supported");
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
              return {
                _tag: "Rejected" as const,
                index: 0,
                reason: "offset" as const,
                record: yield* control.storage.record(id),
              };
            return yield* control.storage.mutate(mutation);
          }),
        });
        const result = yield* Effect.gen(function* () {
          return yield* (yield* StreamsWriter).append(id, appendOptions);
        }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, source)))));
        expect(result.status).toBe(rejects === 7 ? "appended" : "busy");
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
          expect((yield* writer.append(id, options)).status).toBe(
            when === "before" ? "appended" : "duplicate",
          );
          const result = yield* reader.read(id);
          if (result.status === "ok") expect(result.messages.length).toBe(1);
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
            }).pipe(Effect.catchTag("TransportFault", Effect.die)),
          ),
      });
      const result = yield* Effect.gen(function* () {
        return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
      }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))));
      expect(result).toMatchObject({ status: "ok", nextOffset: next(ZERO_OFFSET) });
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
      expect(yield* Fiber.join(fiber)).toMatchObject({ status: "timeout", closed: true });
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
      ).toMatchObject({ status: "ok" });
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
      expect(yield* writer.remove(id)).toEqual({ status: "not-found" });
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
      expect((yield* Fiber.join(fiber)).status).toBe("gone");
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
            }).pipe(Effect.catchTag("TransportFault", Effect.die)),
          ),
      });
      const result = yield* Effect.gen(function* () {
        return yield* (yield* StreamsReader).readNext(id, { offset: next(ZERO_OFFSET) });
      }).pipe(provideTest(Protocol.layer().pipe(Layer.provide(Layer.succeed(Storage, storage)))));
      expect(result).toMatchObject({ status: "ok", nextOffset: ZERO_OFFSET, messages: [] });
      expect(yield* control.subscribers).toBe(0);
    }),
  ));

it("catch-up preserves lexical filtering of noncanonical offsets", () =>
  check(
    Effect.gen(function* () {
      yield* (yield* StreamsWriter).create(id, { contentType: "text/plain", initialData: data });
      const reader = yield* StreamsReader;
      expect(yield* reader.read(id, { offset: "zz" })).toMatchObject({
        status: "ok",
        messages: [],
      });
      const result = yield* reader.read(id, { offset: "0", limit: 1 });
      expect(result).toMatchObject({ status: "ok", nextOffset: next(ZERO_OFFSET) });
      if (result.status === "ok") expect(result.messages.length).toBe(1);
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
          status: "timeout",
          nextOffset: ZERO_OFFSET,
          closed: false,
        });
        visible = true;
        expect(yield* reader.readNext(id, { offset: ZERO_OFFSET })).toMatchObject({
          status: "ok",
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
