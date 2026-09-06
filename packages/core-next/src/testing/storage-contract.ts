/** Executable Bun edge: a fresh scoped Layer and virtual clock for every case. */
import { describe, expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Queue, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ProducerId, StreamId, type ChangeSnapshot, type StreamRecord } from "../schema/index.ts";
import { ZERO_OFFSET, next } from "../offset/index.ts";
import { Storage } from "../storage/storage.ts";
import type { StorageCapabilities } from "../storage/capabilities.ts";
import type { MutationOutcome, Operation } from "../storage/mutation.ts";

export interface StorageContractOptions {
  readonly name: string;
  readonly layer: () => Layer.Layer<Storage>;
  readonly expected: StorageCapabilities;
}
const id = StreamId.make("s");
const child = StreamId.make("child");
const producerId = ProducerId.make("p");
const one = next(ZERO_OFFSET);
const two = next(one);
const encode = (text: string) => new TextEncoder().encode(text);
const record = (streamId = id): StreamRecord => ({
  id: streamId,
  config: { contentType: "text/plain", createdAt: 0 },
  lifecycle: { closed: false, softDeleted: false },
  currentOffset: ZERO_OFFSET,
});
const create = (value = record()): Operation => ({
  _tag: "Create",
  record: value,
  initialMessages: [],
});
const append = (extra: Partial<Extract<Operation, { _tag: "Append" }>> = {}): Operation => ({
  _tag: "Append",
  streamId: id,
  messages: [{ offset: one, data: encode("a"), timestamp: 0 }],
  patch: { currentOffset: one },
  ...extra,
});
const remove = (streamId = id): Operation => ({ _tag: "Delete", streamId, reason: "delete" });
const mutate = Effect.fn("Contract.mutate")(function* (operation: Operation) {
  const storage = yield* Storage;
  return yield* storage.mutate({ operations: [operation] });
});
function applied(out: MutationOutcome, tag: string) {
  expect(out._tag).toBe("Applied");
  if (out._tag === "Applied") expect(String(out.results[0]._tag)).toBe(tag);
}
function rejected(out: MutationOutcome, reason: string) {
  expect(out._tag).toBe("Rejected");
  if (out._tag === "Rejected") expect(String(out.reason)).toBe(reason);
}
const current = Effect.gen(function* () {
  const storage = yield* Storage;
  return Option.getOrThrow(yield* storage.record(id));
});
const fork = Effect.fn("Contract.fork")(function* () {
  const storage = yield* Storage;
  const source = yield* current;
  const messages = storage.capabilities.fork === "copy" ? yield* storage.messages(id, {}) : [];
  const operation: Extract<Operation, { _tag: "Create" }> = {
    _tag: "Create",
    record: {
      ...record(child),
      currentOffset: source.currentOffset,
      lifecycle: {
        closed: false,
        softDeleted: false,
        forkedFrom: id,
        forkOffset: source.currentOffset,
      },
    },
    initialMessages: messages,
  };
  return yield* mutate(
    storage.capabilities.fork === "chain"
      ? { ...operation, forkSource: { id, liveAtOffset: source.currentOffset } }
      : operation,
  );
});
const observe = Effect.gen(function* () {
  const storage = yield* Storage;
  const queue = yield* Queue.unbounded<ChangeSnapshot>();
  const ready = yield* Deferred.make<void>();
  const fiber = yield* storage.changes(id).pipe(
    Stream.runForEach((value) =>
      Effect.gen(function* () {
        yield* Queue.offer(queue, value);
        yield* Deferred.succeed(ready, undefined);
      }),
    ),
    Effect.forkScoped,
  );
  yield* Deferred.await(ready);
  const initial = yield* Queue.take(queue);
  return { queue, fiber, initial };
});
const tick = Effect.gen(function* () {
  yield* TestClock.adjust(25);
});

export const StorageContract = {
  run(options: StorageContractOptions): void {
    describe(options.name, () => {
      const test = (
        name: string,
        body: Effect.Effect<
          unknown,
          unknown,
          Storage | TestClock.TestClock | import("effect").Scope.Scope
        >,
        enabled = true,
        reason = "capability unavailable",
      ) => {
        if (!enabled) {
          it.skip(`${name} — ${reason}`, () => {});
          return;
        }
        it(name, () =>
          expect(
            Effect.gen(function* () {
              const storage = yield* Storage;
              expect(storage.capabilities).toEqual(options.expected);
              yield* body;
            }).pipe(
              Effect.provide(options.layer()),
              Effect.provide(TestClock.layer()),
              Effect.scoped,
              Effect.runPromiseExit,
            ),
          ).resolves.toMatchObject({ _tag: "Success" }),
        );
      };
      // Runtime conversion is confined to this exported test runner.
      test(
        "01 Append advances tail and folds close atomically",
        Effect.gen(function* () {
          yield* mutate(create());
          applied(
            yield* mutate(
              append({ patch: { currentOffset: one, lifecycle: { closed: true, closedAt: 7 } } }),
            ),
            "Appended",
          );
          expect(yield* current).toMatchObject({
            currentOffset: one,
            lifecycle: { closed: true, closedAt: 7 },
          });
          const storage = yield* Storage;
          expect((yield* storage.messages(id, {})).length).toBe(1);
        }),
      );
      for (const [number, reason] of [
        ["02", "offset"],
        ["03", "closed"],
        ["04", "producer"],
        ["05", "absent producer"],
      ] as const) {
        test(
          `${number} rejects ${reason} precondition without writes`,
          Effect.gen(function* () {
            yield* mutate(create());
            yield* mutate(
              append({
                producer: { producerId, expected: Option.none(), next: { epoch: 1, lastSeq: 0 } },
              }),
            );
            const before = yield* current;
            const out = yield* mutate(
              append({
                messages: [{ offset: two, data: encode("b"), timestamp: 1 }],
                patch: { currentOffset: two, lifecycle: { closed: true } },
                expectedOffset: reason === "offset" ? ZERO_OFFSET : one,
                expectedClosed: reason === "closed",
                producer: {
                  producerId,
                  expected:
                    reason === "absent producer"
                      ? Option.none()
                      : Option.some({ epoch: 9, lastSeq: 9 }),
                  next: { epoch: 2, lastSeq: 0 },
                },
              }),
            );
            rejected(out, reason === "absent producer" ? "producer" : reason);
            expect(yield* current).toEqual(before);
            const storage = yield* Storage;
            expect((yield* storage.messages(id, {})).length).toBe(1);
            expect(yield* storage.producer(id, producerId)).toEqual(
              Option.some({ epoch: 1, lastSeq: 0 }),
            );
            if (out._tag === "Rejected") expect(out.record).toEqual(Option.some(before));
          }),
        );
      }
      test(
        "06 lifecycle-only TTL touch preserves tail",
        Effect.gen(function* () {
          yield* mutate(create());
          applied(
            yield* mutate(append({ messages: [], patch: { lifecycle: { expiresAtMs: 100 } } })),
            "Appended",
          );
          expect(yield* current).toMatchObject({
            currentOffset: ZERO_OFFSET,
            lifecycle: { expiresAtMs: 100 },
          });
        }),
      );
      test(
        "07 exclusive after, inclusive until, lexical order and limit",
        Effect.gen(function* () {
          yield* mutate(create());
          const three = next(two),
            four = next(three);
          yield* mutate(
            append({
              messages: [four, one, three, two].map((offset) => ({
                offset,
                data: encode(offset),
                timestamp: 0,
              })),
              patch: { currentOffset: four },
            }),
          );
          const storage = yield* Storage;
          expect((yield* storage.messages(id, {})).map((m) => m.offset)).toEqual([
            one,
            two,
            three,
            four,
          ]);
          expect(
            (yield* storage.messages(id, { after: one, until: three })).map((m) => m.offset),
          ).toEqual([two, three]);
          expect(
            (yield* storage.messages(id, { after: one, until: four, limit: 1 })).map(
              (m) => m.offset,
            ),
          ).toEqual([two]);
          expect(yield* storage.messages(id, { limit: 0 })).toEqual([]);
        }),
      );
      test(
        "08 first changes element differs from stale observed offset",
        Effect.gen(function* () {
          yield* mutate(create());
          yield* mutate(append());
          const { initial } = yield* observe;
          expect(initial.currentOffset).toBe(one);
        }),
      );
      test(
        "09 timeout returns no changed value and consumer re-reads fresh snapshot",
        Effect.gen(function* () {
          yield* mutate(create());
          const { queue, initial } = yield* observe;
          const pending = yield* Stream.fromQueue(queue).pipe(
            Stream.filter(
              (s) =>
                s.currentOffset !== initial.currentOffset ||
                s.closed !== initial.closed ||
                s.present !== initial.present,
            ),
            Stream.take(1),
            Stream.runCollect,
            Effect.timeoutOption(100),
            Effect.forkScoped,
          );
          yield* TestClock.adjust(100);
          expect(yield* Fiber.join(pending)).toEqual(Option.none());
          const storage = yield* Storage;
          expect(Option.getOrThrow(yield* storage.record(id)).currentOffset).toBe(
            initial.currentOffset,
          );
        }),
      );
      for (const [number, action] of [
        ["10", "append"],
        ["11", "close"],
        ["12", "soft delete"],
        ["13", "purge"],
      ] as const) {
        test(
          `${number} parked changes subscriber wakes on ${action}`,
          Effect.gen(function* () {
            yield* mutate(create());
            if (action === "soft delete") yield* fork();
            const { queue } = yield* observe;
            yield* mutate(
              action === "append"
                ? append()
                : action === "close"
                  ? append({ messages: [], patch: { lifecycle: { closed: true } } })
                  : remove(),
            );
            yield* tick;
            const value = yield* Queue.take(queue);
            if (action === "append") expect(value.currentOffset).toBe(one);
            if (action === "close") expect(value.closed).toBe(true);
            if (action === "soft delete") expect(value.softDeleted).toBe(true);
            if (action === "purge") expect(value.present).toBe(false);
          }),
          action !== "soft delete" || options.expected.fork === "chain",
          "soft deletion requires chain fork",
        );
      }
      test(
        "14 purge then re-create lower offset is a change",
        Effect.gen(function* () {
          yield* mutate(create());
          yield* mutate(append());
          const { queue } = yield* observe;
          yield* mutate(remove());
          yield* mutate(create());
          yield* tick;
          const storage = yield* Storage;
          const values = yield* storage.changes(id).pipe(Stream.take(1), Stream.runCollect);
          expect(values[0]).toMatchObject({ present: true, currentOffset: ZERO_OFFSET });
          expect((yield* Queue.take(queue)).currentOffset).not.toBe(one);
        }),
      );
      test(
        "15 commit before subscription survives lost-notify race",
        Effect.gen(function* () {
          yield* mutate(create());
          yield* mutate(append());
          const { initial } = yield* observe;
          expect(initial).toEqual({
            present: true,
            currentOffset: one,
            closed: false,
            softDeleted: false,
          });
        }),
      );
      test(
        "16 consumer ignores a non-advancing wake then observes advance",
        Effect.gen(function* () {
          yield* mutate(create());
          const { queue } = yield* observe;
          yield* mutate(append({ messages: [], patch: { lifecycle: { expiresAtMs: 100 } } }));
          yield* tick;
          expect((yield* Queue.take(queue)).currentOffset).toBe(ZERO_OFFSET);
          yield* mutate(append());
          yield* tick;
          const result = yield* Stream.fromQueue(queue).pipe(
            Stream.filter((s) => s.currentOffset !== ZERO_OFFSET),
            Stream.take(1),
            Stream.runCollect,
          );
          expect(result[0]?.currentOffset).toBe(one);
        }),
      );
      test(
        "17 repeated Create rejects exists with original record",
        Effect.gen(function* () {
          yield* mutate(create());
          const out = yield* mutate(create());
          rejected(out, "exists");
          if (out._tag === "Rejected") expect(out.record).toEqual(Option.some(record()));
        }),
      );
      test(
        "18 Create persists an already-closed record",
        Effect.gen(function* () {
          yield* mutate(
            create({ ...record(), lifecycle: { closed: true, closedAt: 9, softDeleted: false } }),
          );
          expect((yield* current).lifecycle.closedAt).toBe(9);
        }),
      );
      test(
        "19 fork capability is an explicit value",
        Effect.gen(function* () {
          const storage = yield* Storage;
          expect(["none", "copy", "chain"]).toContain(storage.capabilities.fork);
          expect(storage.capabilities.fork).toBe(options.expected.fork);
        }),
      );
      test(
        "20 fork Create is idempotent and missing source rejects",
        Effect.gen(function* () {
          yield* mutate(create());
          yield* mutate(append());
          applied(yield* fork(), "Created");
          rejected(yield* fork(), "exists");
          rejected(
            yield* mutate({
              _tag: "Create",
              record: record(StreamId.make("other")),
              initialMessages: [],
              forkSource: { id: StreamId.make("missing"), liveAtOffset: ZERO_OFFSET },
            }),
            "fork-source-gone",
          );
        }),
        options.expected.fork !== "none",
        "fork unavailable",
      );
      test(
        "21 delete purges record, messages and producers",
        Effect.gen(function* () {
          yield* mutate(create());
          yield* mutate(
            append({
              producer: { producerId, expected: Option.none(), next: { epoch: 0, lastSeq: 0 } },
            }),
          );
          applied(yield* mutate(remove()), "Purged");
          const storage = yield* Storage;
          expect(yield* storage.record(id)).toEqual(Option.none());
          expect(yield* storage.messages(id, {})).toEqual([]);
          expect(yield* storage.producer(id, producerId)).toEqual(Option.none());
        }),
      );
      test(
        "22 delete missing or soft-deleted rejects not-found or gone",
        Effect.gen(function* () {
          rejected(yield* mutate(remove()), "not-found");
          yield* mutate(create({ ...record(), lifecycle: { closed: false, softDeleted: true } }));
          rejected(yield* mutate(remove()), "gone");
          rejected(
            yield* mutate({
              _tag: "Delete",
              streamId: id,
              reason: "expiry",
              expectedExpiresAtMs: 3,
            }),
            "gone",
          );
        }),
      );
      test(
        "23 stale expiry deletion leaves renewed deadline intact",
        Effect.gen(function* () {
          yield* mutate(
            create({
              ...record(),
              lifecycle: { closed: false, softDeleted: false, expiresAtMs: 100 },
            }),
          );
          rejected(
            yield* mutate({
              _tag: "Delete",
              streamId: id,
              reason: "expiry",
              expectedExpiresAtMs: 99,
            }),
            "expiry-mismatch",
          );
          expect((yield* current).lifecycle.expiresAtMs).toBe(100);
          applied(
            yield* mutate({
              _tag: "Delete",
              streamId: id,
              reason: "expiry",
              expectedExpiresAtMs: 100,
            }),
            "Purged",
          );
        }),
      );
      test(
        `24 ${options.expected.fork === "chain" ? "soft-delete ancestor then cascade purge" : "purging copied source leaves child readable"}`,
        Effect.gen(function* () {
          yield* mutate(create());
          yield* mutate(append());
          yield* fork();
          applied(
            yield* mutate(remove()),
            options.expected.fork === "chain" ? "SoftDeleted" : "Purged",
          );
          const storage = yield* Storage;
          expect(
            (yield* storage.messages(child, {})).map((m) => new TextDecoder().decode(m.data)),
          ).toEqual(["a"]);
          yield* mutate(remove(child));
          expect(yield* storage.record(id)).toEqual(Option.none());
          expect(yield* storage.record(child)).toEqual(Option.none());
        }),
        options.expected.fork !== "none",
        "fork unavailable",
      );
      test(
        "25 interruption ends parked subscriber without fault and runs finalizer",
        Effect.gen(function* () {
          yield* mutate(create());
          const storage = yield* Storage;
          const ready = yield* Deferred.make<void>();
          const done = yield* Deferred.make<void>();
          const fiber = yield* storage.changes(id).pipe(
            Stream.tap(() => Deferred.succeed(ready, undefined)),
            Stream.runDrain,
            Effect.ensuring(Deferred.succeed(done, undefined)),
            Effect.forkScoped,
          );
          yield* Deferred.await(ready);
          yield* Fiber.interrupt(fiber);
          yield* Deferred.await(done);
          const exit = yield* Fiber.await(fiber);
          expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
          yield* mutate(append());
          expect((yield* observe).initial.currentOffset).toBe(one);
        }),
      );
      test(
        "26 store mutation is all or nothing with first failure index",
        Effect.gen(function* () {
          yield* mutate(create());
          const storage = yield* Storage;
          const out = yield* storage.mutate({
            operations: [create(record(child)), append({ expectedOffset: one })],
          });
          rejected(out, "offset");
          if (out._tag === "Rejected") expect(out.index).toBe(1);
          expect(yield* storage.record(child)).toEqual(Option.none());
          expect((yield* current).currentOffset).toBe(ZERO_OFFSET);
          const success = yield* storage.mutate({ operations: [create(record(child)), append()] });
          expect(success._tag).toBe("Applied");
          expect(Option.isSome(yield* storage.record(child))).toBe(true);
        }),
        options.expected.atomicScope === "store",
        "requires store atomic scope",
      );
      test(
        "27 stream mutation rejects multiple targets as a defect",
        Effect.gen(function* () {
          const storage = yield* Storage;
          const exit = yield* Effect.exit(
            storage.mutate({ operations: [create(), create(record(child))] }),
          );
          expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
          expect(yield* storage.record(id)).toEqual(Option.none());
        }),
        options.expected.atomicScope === "stream",
        "requires stream atomic scope",
      );
      test(
        "28 polling observes mutation within one virtual tick",
        Effect.gen(function* () {
          yield* mutate(create());
          const { queue } = yield* observe;
          yield* mutate(append());
          yield* tick;
          expect((yield* Queue.take(queue)).currentOffset).toBe(one);
        }),
        options.expected.wake === "poll",
        "requires polling wake",
      );
      test(
        "29 lazy index is None; indexed returns and updates earliest deadline",
        Effect.gen(function* () {
          const storage = yield* Storage;
          yield* mutate(
            create({
              ...record(),
              lifecycle: { closed: false, softDeleted: false, expiresAtMs: 200 },
            }),
          );
          yield* mutate(
            create({
              ...record(child),
              lifecycle: { closed: false, softDeleted: false, expiresAtMs: 100 },
            }),
          );
          expect(yield* storage.nextExpiry).toEqual(
            options.expected.expiryIndex === "indexed"
              ? Option.some({ at: 100, streamId: child })
              : Option.none(),
          );
          yield* mutate(remove(child));
          expect(yield* storage.nextExpiry).toEqual(
            options.expected.expiryIndex === "indexed"
              ? Option.some({ at: 200, streamId: id })
              : Option.none(),
          );
          yield* mutate(append({ messages: [], patch: { clear: ["expiresAtMs"] } }));
          expect(yield* storage.nextExpiry).toEqual(Option.none());
        }),
      );
      test(
        "30 records and byte arrays do not alias input or returned values",
        Effect.gen(function* () {
          const input = { ...record(), config: { contentType: "text/plain", createdAt: 0 } };
          const bytes = encode("a");
          const storage = yield* Storage;
          yield* mutate({
            _tag: "Create",
            record: input,
            initialMessages: [{ offset: one, data: bytes, timestamp: 0 }],
          });
          input.config.contentType = "changed";
          bytes[0] = 98;
          const messages = yield* storage.messages(id, {});
          messages[0]?.data.fill(99);
          expect((yield* current).config.contentType).toBe("text/plain");
          expect(new TextDecoder().decode((yield* storage.messages(id, {}))[0]?.data)).toBe("a");
          const first = yield* current,
            second = yield* current;
          expect(first).not.toBe(second);
          expect(first.config).not.toBe(second.config);
          expect(first.lifecycle).not.toBe(second.lifecycle);
        }),
      );
      test(
        "31 duplicate stream targets are a defect in every mode",
        Effect.gen(function* () {
          const storage = yield* Storage;
          const exit = yield* Effect.exit(storage.mutate({ operations: [create(), create()] }));
          expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
          expect(yield* storage.record(id)).toEqual(Option.none());
        }),
      );
      test(
        "32 patch clears optional fields and preserves unrelated values",
        Effect.gen(function* () {
          yield* mutate(
            create({
              ...record(),
              config: { contentType: "text/plain", createdAt: 0, ttlSeconds: 1, expiresAt: "date" },
              lifecycle: { closed: false, softDeleted: false, lastSeq: "x", expiresAtMs: 100 },
            }),
          );
          yield* mutate(
            append({
              messages: [],
              patch: { clear: ["ttlSeconds", "expiresAt", "lastSeq", "expiresAtMs"] },
            }),
          );
          expect(yield* current).toEqual(record());
        }),
      );
    });
  },
  /** The configured memory suite must exercise each shipped capability value. */
  assertModes(configurations: ReadonlyArray<StorageCapabilities>): void {
    it("configured contract runs cover both shipped capability modes", () => {
      for (const [key, expected] of Object.entries({
        fork: ["chain", "copy"],
        atomicScope: ["store", "stream"],
        wake: ["push", "poll"],
        expiryIndex: ["indexed", "lazy"],
      })) {
        expect(
          new Set(configurations.map((c) => Object.entries(c).find(([k]) => k === key)?.[1])),
        ).toEqual(new Set(expected));
      }
    });
  },
};
