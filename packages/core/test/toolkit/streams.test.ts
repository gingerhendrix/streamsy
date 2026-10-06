import { expect, it } from "bun:test";
import { Cause, Effect, Exit, Layer, Schedule, Schema } from "effect";
import * as StreamAddress from "../../src/toolkit/address.ts";
import { MemoryStorage } from "../../src/index.ts";
import * as Streams from "../../src/toolkit/streams.ts";
import { ZERO_OFFSET } from "../../src/offset/index.ts";
import {
  InvalidAppendRequest,
  InvalidEpochSeq,
  OffsetMismatch,
  StaleEpoch,
  StreamClosed,
  StreamNotFound,
} from "../../src/protocol/errors.ts";
import { StreamsReader, StreamsWriter, type Writer } from "../../src/protocol/tags.ts";
import { TransportFault, type StreamsFault } from "../../src/fault.ts";
import { StreamId } from "../../src/schema/index.ts";

const address = StreamAddress.json("events", { schema: Schema.Struct({ n: Schema.Finite }) });

/** One message per page, so every fold below reads several pages. */
const run = <A, E>(program: Effect.Effect<A, E, StreamsReader | StreamsWriter>) =>
  Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layerProtocol({ readLimit: 1 }))));

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

const unused = () => Effect.die(new Error("unused"));

/** A writer whose append always fails with one rejection. */
const rejecting = (
  error: OffsetMismatch | StaleEpoch | StreamClosed | InvalidAppendRequest | InvalidEpochSeq,
) => {
  const writer: Writer<StreamsFault> = {
    create: unused,
    fork: unused,
    remove: unused,
    append: () => Effect.fail(error),
  };

  return Layer.succeed(StreamsWriter, writer);
};

const id = StreamId.make("events");

const mismatch = new OffsetMismatch({ id, expected: "a", actual: "b" });

const stale = new StaleEpoch({ id, currentEpoch: 2 });

const closed = new StreamClosed({ id, offset: "a" });

const producer = { producerId: "p", producerEpoch: 1, producerSeq: 0 };

/** Run one append against a writer that answers with `error`. */
const answer = <A, E>(
  program: Effect.Effect<A, E, StreamsWriter>,
  error: OffsetMismatch | StaleEpoch | StreamClosed | InvalidAppendRequest | InvalidEpochSeq,
) => program.pipe(Effect.provide(rejecting(error)));

it("append keeps a rejection that its options make possible", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const conditional = Streams.append(address, [{ n: 1 }], { expectedOffset: "a" });
      expect(yield* Effect.flip(answer(conditional, mismatch))).toBe(mismatch);
      const fenced = Streams.append(address, [{ n: 1 }], { producer });
      expect(yield* Effect.flip(answer(fenced, stale))).toBe(stale);
      const plain = Streams.append(address, [{ n: 1 }]);
      expect(yield* Effect.flip(answer(plain, closed))).toBe(closed);
    }),
  ));

it("append dies on a rejection that its options rule out", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plain = Streams.append(address, [{ n: 1 }]);
      const conditional = Streams.append(address, [{ n: 1 }], { expectedOffset: "a" });

      for (const [exit, error] of [
        [yield* Effect.exit(answer(plain, mismatch)), mismatch],
        [yield* Effect.exit(answer(conditional, stale)), stale],
      ] as const) {
        expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(error);
      }
    }),
  ));

const sum = {
  initial: () => 0,
  fold: (total: number, event: { readonly n: number }) => total + event.n,
};

/** The memory writer, with every append counted. */
const countingAppends = Effect.gen(function* () {
  const writer = yield* StreamsWriter;
  const appends = { count: 0 };

  const counted: Writer<StreamsFault> = {
    ...writer,
    append: (streamId, options) =>
      Effect.suspend(() => {
        appends.count += 1;

        return writer.append(streamId, options);
      }),
  };

  return { appends, counted };
});

it("decide with no items makes no append and no tail check", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }, { n: 2 }]);
      const { appends, counted } = yield* countingAppends;

      const decided = yield* Streams.decide(address, {
        ...sum,
        decide: (total, { offset }) =>
          // A rival write after the read: a no-write answer does not check the tail.
          Streams.append(address, [{ n: 10 }]).pipe(
            Effect.as({ value: `${total}@${offset}`, items: [] }),
          ),
      }).pipe(Effect.provideService(StreamsWriter, counted));

      const tail = (yield* Streams.head(address)).nextOffset;
      expect(decided.appended).toBe(false);
      expect(decided.value).toBe(`3@${decided.offset}`);
      expect(decided.offset).not.toBe(tail);
      // The one append is the rival's.
      expect(appends.count).toBe(1);
    }),
  ));

it("decide retries a lost race and folds only the new items from the old tail", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 1 }, { n: 2 }, { n: 3 }]);
      let initials = 0;
      let folded = 0;
      const seen: Array<{ readonly total: number; readonly offset: string }> = [];

      const decided = yield* Streams.decide(address, {
        initial: () => {
          initials += 1;

          return 0;
        },
        fold: (total: number, event) => {
          folded += 1;

          return total + event.n;
        },
        decide: (total, { offset }) =>
          Effect.gen(function* () {
            seen.push({ total, offset });

            // The first decision loses to a rival write.
            if (seen.length === 1) yield* Streams.append(address, [{ n: 10 }]);

            return { value: total, items: [{ n: total }] };
          }),
      });

      expect(seen.map(({ total }) => total)).toEqual([6, 16]);
      expect(seen[1]?.offset).not.toBe(seen[0]?.offset);
      expect(initials).toBe(1);
      // Three items on the first read, then only the rival's item.
      expect(folded).toBe(4);
      expect(decided).toEqual({
        value: 16,
        offset: (yield* Streams.head(address)).nextOffset,
        appended: true,
      });
      expect((yield* Streams.collect(address)).items.map(({ n }) => n)).toEqual([1, 2, 3, 10, 16]);
    }),
  ));

it("decide fails with the last OffsetMismatch when the schedule ends", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      const offsets: Array<string> = [];

      const failure = yield* Effect.flip(
        Streams.decide(address, {
          ...sum,
          decide: (_total, { offset }) =>
            Effect.gen(function* () {
              offsets.push(offset);
              yield* Streams.append(address, [{ n: 1 }]);

              return { value: undefined, items: [{ n: 0 }] };
            }),
          retry: Schedule.recurs(2),
        }),
      );

      expect(offsets).toHaveLength(3);
      expect(failure).toBeInstanceOf(OffsetMismatch);
      expect(failure).toMatchObject({ expected: offsets[2] });
      expect((yield* Streams.collect(address)).items.map(({ n }) => n)).toEqual([1, 1, 1]);
    }),
  ));

it("decide does not retry other append failures", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [], { close: true });
      let closedDecisions = 0;

      const closedFailure = yield* Effect.flip(
        Streams.decide(address, {
          ...sum,
          decide: () => {
            closedDecisions += 1;

            return Effect.succeed({ value: undefined, items: [{ n: 1 }] });
          },
        }),
      );

      expect(closedFailure).toBeInstanceOf(StreamClosed);
      expect(closedDecisions).toBe(1);

      // A lost reply may hide a landed append, so a transport fault is not retried.
      const other = StreamAddress.json("other", { schema: Schema.Struct({ n: Schema.Finite }) });
      yield* Streams.create(other);

      const fault = new TransportFault({
        operation: "append",
        reason: "response",
        message: "lost",
      });

      const writer = yield* StreamsWriter;
      let faultDecisions = 0;

      const faultFailure = yield* Effect.flip(
        Streams.decide(other, {
          ...sum,
          decide: () => {
            faultDecisions += 1;

            return Effect.succeed({ value: undefined, items: [{ n: 1 }] });
          },
        }).pipe(
          Effect.provideService(StreamsWriter, { ...writer, append: () => Effect.fail(fault) }),
        ),
      );

      expect(faultFailure).toBe(fault);
      expect(faultDecisions).toBe(1);
    }),
  ));

class Refused extends Schema.TaggedError<Refused>()("Refused", { total: Schema.Finite }) {}

it("decide passes its own failure through without an append or a retry", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 4 }]);
      const { appends, counted } = yield* countingAppends;
      let decisions = 0;

      const failure = yield* Effect.flip(
        Streams.decide(address, {
          ...sum,
          decide: (total) => {
            decisions += 1;

            return Effect.fail(new Refused({ total }));
          },
        }).pipe(Effect.provideService(StreamsWriter, counted)),
      );

      expect(failure).toEqual(new Refused({ total: 4 }));
      expect(decisions).toBe(1);
      expect(appends.count).toBe(0);
    }),
  ));

it("decide does not retry an OffsetMismatch that its own decide raises", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(address);
      yield* Streams.append(address, [{ n: 4 }]);
      const { appends, counted } = yield* countingAppends;

      const own = new OffsetMismatch({
        id: StreamId.make("elsewhere"),
        expected: "a",
        actual: "b",
      });

      let decisions = 0;

      const failure = yield* Effect.flip(
        Streams.decide(address, {
          ...sum,
          decide: () => {
            decisions += 1;

            return Effect.fail(own);
          },
        }).pipe(Effect.provideService(StreamsWriter, counted)),
      );

      expect(failure).toBe(own);
      expect(decisions).toBe(1);
      expect(appends.count).toBe(0);
    }),
  ));

for (const error of [
  new InvalidAppendRequest({ id, message: "malformed request" }),
  new InvalidEpochSeq({ id }),
]) {
  it(`append dies on a host answer of ${error.name}, even with producer options`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        for (const options of [{}, { producer }] as const) {
          const exit = yield* Effect.exit(
            answer(Streams.append(address, [{ n: 1 }], options), error),
          );

          expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
          expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(error);
        }
      }),
    ));
}

it("append rejects each empty encoded bytes item before any write", () =>
  run(
    Effect.gen(function* () {
      const bytes = StreamAddress.bytes("bytes");
      yield* Streams.create(bytes);
      const { appends, counted } = yield* countingAppends;
      const empty = new Uint8Array();
      const nonempty = new Uint8Array([1]);

      for (const items of [[empty], [nonempty, empty], [empty, nonempty]] as const) {
        for (const options of [{}, { close: true }] as const) {
          const exit = yield* Streams.append(bytes, items, options).pipe(
            Effect.provideService(StreamsWriter, counted),
            Effect.exit,
          );

          expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
          const defect = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
          expect(defect).toBeInstanceOf(RangeError);
          expect(defect).toMatchObject({ message: "Cannot append an empty bytes item to bytes" });
        }
      }

      expect(appends.count).toBe(0);
      // A close-only append has no encoded item and remains valid.
      yield* Streams.append(bytes, [], { close: true }).pipe(
        Effect.provideService(StreamsWriter, counted),
      );
      expect(appends.count).toBe(1);
    }),
  ));
