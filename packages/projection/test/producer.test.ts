import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { Streamsy } from "@streamsy/core";
import { producerId } from "../src/commit/producer.ts";
/* oxlint-disable effecttsgo/strict-effect-provide -- Bun owns the runtime boundary; each test assembles the complete routed host graph. */
import { expect, test } from "bun:test";
import { Context, Deferred, Effect, Fiber, Layer, Option, Schema, Stream } from "effect";
import { StorageFault, StreamAddress, ZERO_OFFSET, type StreamsFault } from "@streamsy/core";
import {
  Protocol,
  Streams,
  StreamsReader,
  StreamsWriter,
  type Reader,
  type Writer,
} from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Projection,
  ProjectionFault,
  type Identity,
  type Transaction,
  type Unit,
} from "@streamsy/projection";
import { type CheckpointRecord } from "@streamsy/projection/checkpoint";
import * as Memory from "@streamsy/projection/memory";

/**
 * Inputs, outputs and checkpoints share one owner. The pinned runner still commits
 * appends and checkpoint saves separately, so crash/replay coverage remains essential.
 */
class In extends Streamsy.json<In>()("outputs-pass.test/In", "in/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}
class Out extends Streamsy.json<Out>()("outputs-pass.test/Out", "out/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
}) {}
const inputGraph = Streams.layerMemory();
class PagedReader extends Context.Service<PagedReader, Reader<StreamsFault>>()(
  "test/PagedReader",
) {}
const host = Layer.mergeAll(
  inputGraph,
  Memory.layer.pipe(Layer.provide(inputGraph)),
  Layer.effect(PagedReader, StreamsReader).pipe(
    Layer.provide(Protocol.layer({ readLimit: 1 }).pipe(Layer.provide(inputGraph))),
  ),
);
type Services =
  | In
  | Out
  | PagedReader
  | CheckpointStore
  | Transaction
  | import("@streamsy/core").Streamsy.Wildcard
  | StreamsReader
  | StreamsWriter;
const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(body.pipe(Effect.provide(FamilyFixture.withFamilies(host, In, Out))));

const fault = <A>(result: { readonly _tag: string; readonly failure?: A }): ProjectionFault => {
  if (result._tag !== "Failure") throw new Error("Expected a failure");
  if (!Schema.is(ProjectionFault)(result.failure)) throw new Error("Expected a ProjectionFault");
  return result.failure;
};
const readAll = <A>(address: StreamAddress.StreamAddress<A>) =>
  Streams.read(address).pipe(Streams.items, Stream.runCollect);
const stored = (projection: Identity) =>
  Effect.gen(function* () {
    const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
    return { token: loaded.token, record: Option.getOrUndefined(loaded.record) };
  });

/** Substitutes part of the writer for one effect; every other operation is untouched. */
const withWriter = <A, E, R>(
  body: Effect.Effect<A, E, R | StreamsWriter>,
  patch: (writer: Writer<StreamsFault>) => Partial<Writer<StreamsFault>>,
) =>
  Effect.gen(function* () {
    const writer = yield* StreamsWriter;
    return yield* body.pipe(provideWriter(StreamsWriter.of({ ...writer, ...patch(writer) })));
  });
const withReader = <A, E, R>(
  body: Effect.Effect<A, E, R | StreamsReader>,
  patch: (reader: Reader<StreamsFault>) => Partial<Reader<StreamsFault>>,
) =>
  Effect.gen(function* () {
    const reader = yield* StreamsReader;
    return yield* body.pipe(provideReader(StreamsReader.of({ ...reader, ...patch(reader) })));
  });
const withCheckpointStore = <A, E, R>(
  body: Effect.Effect<A, E, R | CheckpointStore>,
  patch: (owner: typeof CheckpointStore.Service) => Partial<typeof CheckpointStore.Service>,
) =>
  Effect.gen(function* () {
    const owner = yield* CheckpointStore;
    return yield* body.pipe(
      Effect.provideService(CheckpointStore, CheckpointStore.of({ ...owner, ...patch(owner) })),
    );
  });

/** Records the producer tuple of every append; the append itself goes through. */
const recordingAppends = () => {
  const positions: Array<{ readonly epoch?: number; readonly seq?: number }> = [];
  const patch = (writer: Writer<StreamsFault>): Partial<Writer<StreamsFault>> => ({
    append: (id, options) =>
      Effect.sync(() => {
        positions.push({
          epoch: options.producer?.producerEpoch,
          seq: options.producer?.producerSeq,
        });
      }).pipe(Effect.andThen(writer.append(id, options))),
  });
  return { positions, patch };
};

const appendFault = new StorageFault({ operation: "append", message: "lost", retryable: true });
/** Fails the first `count` appends; later appends go through and their outcomes are recorded. */
const failingAppends = (count: number) => {
  const outcomes: Array<string> = [];
  let failures = 0;
  const patch = (writer: Writer<StreamsFault>): Partial<Writer<StreamsFault>> => ({
    append: (id, options) =>
      failures < count
        ? Effect.sync(() => {
            failures += 1;
          }).pipe(Effect.andThen(Effect.fail(appendFault)))
        : writer.append(id, options).pipe(
            Effect.tap((r) =>
              Effect.sync(() => {
                outcomes.push(r._tag);
              }),
            ),
          ),
  });
  return { outcomes, patch };
};

/** An outputs projection that doubles numbers into strings; both refs are unique per scenario. */
const scenario = (name: string) => {
  const input = In.at({ name });
  const output = Out.at({ name });
  const seen: Array<{ readonly items: ReadonlyArray<number>; readonly unit: Unit }> = [];
  const definition = {
    id: `doubled-${name}`,
    params: {},
    inputs: { input },
    outputs: { output: output },
  };
  const doubled = Projection.mapAccumBatch({
    commit: Commit.producer,
    ...definition,
    state: { schema: Schema.Null, initial: null },
    mapAccumBatch: (_state, batch, unit) =>
      Effect.sync(() => {
        seen.push({ items: batch.input.items, unit });
        return [null, { output: batch.input.items.map((n) => String(n * 2)) }] as const;
      }),
  }).at({});
  const initialize = Effect.gen(function* () {
    yield* Streams.create(input);
    yield* Streams.create(output);
    yield* Streams.append(input, [1, 2, 3]);
  });
  return { input, output, definition, doubled, seen, initialize };
};
const pinnedRecord = (record: CheckpointRecord | undefined) => {
  if (record?.pending === undefined) throw new Error("Expected a pinned unit");
  return {
    pending: record.pending,
    output: record.adapters.outputs?.output,
    inputs: record.inputs,
  };
};

test("crash after append and before the checkpoint settles by Duplicate without duplicate output", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("settle-duplicate");
      yield* s.initialize;
      let saves = 0;
      const crashed = yield* withCheckpointStore(Projection.run(s.doubled), (owner) => ({
        compareAndSet: (key, token, record) => {
          saves += 1;
          return saves === 2
            ? Effect.fail(
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "storage-failure",
                  message: "after-append",
                }),
              )
            : owner.compareAndSet(key, token, record);
        },
      })).pipe(Effect.result);
      const failure = fault(crashed);
      expect(failure.phase).toBe("checkpoint");
      expect(failure.reason).toBe("storage-failure");
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      const before = yield* stored(s.doubled);
      const pinned = pinnedRecord(before.record);
      expect(pinned.inputs.input).toBe(ZERO_OFFSET);
      expect(pinned.pending.seqs?.output).toBe(0);
      expect(pinned.pending.ranges.input?.count).toBe(3);
      expect(pinned.output).toEqual({ epoch: 1, nextSeq: 0 });

      const appends = failingAppends(0);
      const settled = yield* withWriter(Projection.run(s.doubled), appends.patch);
      expect(appends.outcomes).toEqual(["Duplicate"]);
      expect(settled.status).toBe("caught-up");
      expect(settled.units).toBe(1);
      expect(settled.items).toBe(3);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      const after = yield* stored(s.doubled);
      expect(after.record?.pending).toBeUndefined();
      expect(after.record?.inputs.input).toBe(pinned.pending.ranges.input?.nextOffset ?? "");
      expect(after.record?.adapters.outputs?.output).toEqual({ epoch: 1, nextSeq: 1 });
      expect(after.token).toBe("2");
      // The retry processed exactly the pinned range under the same unit key.
      expect(s.seen.map((entry) => entry.items)).toEqual([
        [1, 2, 3],
        [1, 2, 3],
      ]);
      expect(s.seen[1]?.unit.key).toBe(s.seen[0]?.unit.key ?? "");
    }),
  ));

test("a competing save between the append and the settle fails in phase checkpoint, then settles by Duplicate", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("settle-conflict");
      yield* s.initialize;
      let saves = 0;
      let pinned: CheckpointRecord | undefined;
      // The competing writer re-saves the pinned record under the pin token, so the
      // kernel's settle save meets a real token conflict from the store.
      const crashed = yield* withCheckpointStore(Projection.run(s.doubled), (owner) => ({
        compareAndSet: (key, token, record) => {
          saves += 1;
          if (saves === 1) pinned = record;
          return saves === 2 && pinned !== undefined
            ? owner
                .compareAndSet(key, token, pinned)
                .pipe(Effect.andThen(owner.compareAndSet(key, token, record)))
            : owner.compareAndSet(key, token, record);
        },
      })).pipe(Effect.result);
      const failure = fault(crashed);
      expect(failure.phase).toBe("checkpoint");
      expect(failure.reason).toBe("token-conflict");
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      const before = yield* stored(s.doubled);
      expect(before.token).toBe("2");
      expect(pinnedRecord(before.record).pending.seqs?.output).toBe(0);

      const appends = failingAppends(0);
      const settled = yield* withWriter(Projection.run(s.doubled), appends.patch);
      expect(appends.outcomes).toEqual(["Duplicate"]);
      expect(settled.items).toBe(3);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      const after = yield* stored(s.doubled);
      expect(after.token).toBe("3");
      expect(after.record?.pending).toBeUndefined();
      expect(after.record?.adapters.outputs?.output).toEqual({ epoch: 1, nextSeq: 1 });
    }),
  ));

test("the first pin stores the stream epoch before the append; a crash there settles by Appended", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("settle-appended");
      yield* s.initialize;
      const failing = failingAppends(1);
      const crashed = yield* withWriter(Projection.run(s.doubled), failing.patch).pipe(
        Effect.result,
      );
      const failure = fault(crashed);
      expect(failure.phase).toBe("pin");
      expect(failure.reason).toBe("storage-failure");
      expect(yield* readAll(s.output)).toEqual([]);
      const before = yield* stored(s.doubled);
      expect(before.token).toBe("1");
      const pinned = pinnedRecord(before.record);
      expect(pinned.output).toEqual({ epoch: 1, nextSeq: 0 });
      expect(pinned.inputs.input).toBe(ZERO_OFFSET);
      expect(pinned.pending.seqs?.output).toBe(0);
      expect(pinned.pending.ranges.input?.from).toBe(ZERO_OFFSET);
      expect(pinned.pending.ranges.input?.count).toBe(3);
      expect(pinned.pending.ranges.input?.nextOffset).not.toBe(ZERO_OFFSET);

      const settled = yield* withWriter(Projection.run(s.doubled), failing.patch);
      expect(failing.outcomes).toEqual(["Appended"]);
      expect(settled.items).toBe(3);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      const after = yield* stored(s.doubled);
      expect(after.record?.pending).toBeUndefined();
      expect(after.record?.adapters.outputs?.output).toEqual({ epoch: 1, nextSeq: 1 });
    }),
  ));

test("an empty-output unit writes no pin and consumes no seq", () =>
  run(
    Effect.gen(function* () {
      const input = In.at({ name: "evens" });
      const output = Out.at({ name: "evens" });
      const evens = Projection.emit({
        commit: Commit.producer,
        id: "evens",
        params: {},
        inputs: { input },
        outputs: { output: output },
        emit: (entry) =>
          Effect.succeed({ output: entry.item % 2 === 0 ? [String(entry.item)] : [] }),
      }).at({});
      yield* Streams.create(input);
      yield* Streams.create(output);
      yield* Streams.append(input, [1, 3]);
      const recording = recordingAppends();
      const first = yield* withWriter(Projection.run(evens), recording.patch);
      expect(first.items).toBe(2);
      expect(recording.positions).toEqual([]);
      const afterEmpty = yield* stored(evens);
      expect(afterEmpty.token).toBe("1");
      expect(afterEmpty.record?.pending).toBeUndefined();
      expect(afterEmpty.record?.adapters.outputs?.output).toBeUndefined();
      expect(afterEmpty.record?.inputs.input).not.toBe(ZERO_OFFSET);

      yield* Streams.append(input, [4]);
      yield* withWriter(Projection.run(evens), recording.patch);
      expect(recording.positions).toEqual([{ epoch: 1, seq: 0 }]);
      expect(yield* readAll(output)).toEqual(["4"]);
      expect((yield* stored(evens)).record?.adapters.outputs?.output).toEqual({
        epoch: 1,
        nextSeq: 1,
      });
    }),
  ));

test("a retry reproduces a pinned range across several pages", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("paged");
      yield* s.initialize;
      const failing = failingAppends(1);
      fault(yield* withWriter(Projection.run(s.doubled), failing.patch).pipe(Effect.result));
      const paged = yield* PagedReader;
      let reads = 0;
      const settled = yield* withReader(
        withWriter(Projection.pass(s.doubled), failing.patch),
        () => ({
          read: (id, options) => {
            reads += 1;
            return paged.read(id, options);
          },
        }),
      );
      expect(failing.outcomes).toEqual(["Appended"]);
      expect(settled.items).toBe(3);
      // Three one-message server pages reproduce the pin.
      expect(reads).toBe(3);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      expect(s.seen[1]?.items).toEqual([1, 2, 3]);
    }),
  ));

test("a pinned range on a removed input fails pin/range-unreproducible naming the input", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("removed");
      yield* s.initialize;
      const failing = failingAppends(1);
      fault(yield* withWriter(Projection.run(s.doubled), failing.patch).pipe(Effect.result));
      yield* Streams.remove(s.input);
      const failure = fault(yield* Projection.run(s.doubled).pipe(Effect.result));
      expect(failure.phase).toBe("pin");
      expect(failure.reason).toBe("range-unreproducible");
      expect(failure.input).toBe("input");
      expect(yield* readAll(s.output)).toEqual([]);
      expect((yield* stored(s.doubled)).token).toBe("1");
    }),
  ));

test("a short reproduction fails pin/range-unreproducible", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("short");
      yield* s.initialize;
      const failing = failingAppends(1);
      fault(yield* withWriter(Projection.run(s.doubled), failing.patch).pipe(Effect.result));
      const failure = fault(
        yield* withReader(Projection.run(s.doubled), (reader) => ({
          read: (id, options) =>
            reader
              .read(id, options)
              .pipe(
                Effect.map((r) => ({ ...r, messages: r.messages.slice(0, 2), upToDate: true })),
              ),
        })).pipe(Effect.result),
      );
      expect(failure.phase).toBe("pin");
      expect(failure.reason).toBe("range-unreproducible");
      expect(failure.input).toBe("input");
    }),
  ));

test("two competing passes with one loaded token: the second pin fails before any append", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("token-race");
      yield* s.initialize;
      const gate = yield* Deferred.make<void>();
      const reading = yield* Deferred.make<void>();
      const late = Projection.emit({
        commit: Commit.producer,
        ...s.definition,
        emit: (entry) =>
          Deferred.succeed(reading, undefined).pipe(
            Effect.andThen(Deferred.await(gate)),
            Effect.as({ output: [String(entry.item * 2)] }),
          ),
      }).at({});
      const appends = failingAppends(0);
      const second = yield* withWriter(Projection.pass(late), appends.patch).pipe(
        Effect.result,
        Effect.forkChild,
      );
      yield* Deferred.await(reading);
      const first = yield* withWriter(Projection.pass(s.doubled), appends.patch);
      expect(first.items).toBe(3);
      yield* Deferred.succeed(gate, undefined);
      const failure = fault(yield* Fiber.join(second));
      expect(failure.phase).toBe("pin");
      expect(failure.reason).toBe("token-conflict");
      expect(appends.outcomes).toEqual(["Appended"]);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      expect((yield* stored(s.doubled)).token).toBe("2");
    }),
  ));

for (const order of ["first-then-second", "second-then-first"] as const) {
  test(`two competing passes settle the identical pinned unit (${order}): one Appended, one Duplicate, one checkpoint`, () =>
    run(
      Effect.gen(function* () {
        const s = scenario(`identical-${order}`);
        yield* s.initialize;
        const failing = failingAppends(1);
        fault(yield* withWriter(Projection.pass(s.doubled), failing.patch).pipe(Effect.result));
        const pinToken = (yield* stored(s.doubled)).token;

        // Both runners load the pin before either appends; the gate fixes the append order.
        const loaded = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        const outcomes: Array<string> = [];
        const runner = (name: string, role: "leads" | "follows") =>
          withWriter(
            Projection.pass(
              Projection.emit({
                commit: Commit.producer,
                ...s.definition,
                emit: (entry) =>
                  (role === "follows" ? Deferred.succeed(loaded, undefined) : Effect.void).pipe(
                    Effect.as({ output: [String(entry.item * 2)] }),
                  ),
              }).at({}),
            ),
            (writer) => ({
              append: (id, options) =>
                (role === "leads" ? Deferred.await(loaded) : Deferred.await(gate)).pipe(
                  Effect.andThen(writer.append(id, options)),
                  Effect.tap((r) =>
                    Effect.sync(() => {
                      outcomes.push(`${name}:${r._tag}`);
                    }),
                  ),
                  Effect.tap(() => Deferred.succeed(gate, undefined)),
                ),
            }),
          ).pipe(Effect.result);
        const leads = order === "first-then-second" ? "first" : "second";
        const follows = leads === "first" ? "second" : "first";
        const [a, b] = yield* Effect.all([runner(leads, "leads"), runner(follows, "follows")], {
          concurrency: 2,
        });
        expect(outcomes).toEqual([`${leads}:Appended`, `${follows}:Duplicate`]);
        const failed = [a, b].filter((r) => r._tag === "Failure");
        expect(failed.length).toBe(1);
        const failure = fault(failed[0] ?? a);
        expect(failure.phase).toBe("checkpoint");
        expect(failure.reason).toBe("token-conflict");
        expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
        const after = yield* stored(s.doubled);
        expect(after.token).toBe(String(Number(pinToken) + 1));
        expect(after.record?.pending).toBeUndefined();
        expect(after.record?.adapters.outputs?.output).toEqual({ epoch: 1, nextSeq: 1 });
      }),
    ));
}

test("generation 2 appends at seq 0 in epoch 2; a later generation 1 runner fails stale-epoch", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("generations");
      yield* s.initialize;
      const recording = recordingAppends();
      yield* withWriter(Projection.run(s.doubled), recording.patch);
      const second = Projection.emit({
        commit: Commit.producer,
        ...s.definition,
        generation: 2,
        emit: (entry) => Effect.succeed({ output: [String(entry.item * 2)] }),
      }).at({});
      const again = yield* withWriter(Projection.run(second), recording.patch);
      expect(again.items).toBe(3);
      expect(recording.positions).toEqual([
        { epoch: 1, seq: 0 },
        { epoch: 2, seq: 0 },
      ]);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6", "2", "4", "6"]);
      expect((yield* stored(second)).record?.adapters.outputs?.output).toEqual({
        epoch: 2,
        nextSeq: 1,
      });

      yield* Streams.append(s.input, [4]);
      const failure = fault(
        yield* withWriter(Projection.run(s.doubled), recording.patch).pipe(Effect.result),
      );
      expect(failure.phase).toBe("pin");
      expect(failure.reason).toBe("stale-epoch");
      expect(recording.positions[2]).toEqual({ epoch: 1, seq: 1 });
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6", "2", "4", "6"]);
      // The pin stays until a new generation takes over; nothing advanced.
      expect(pinnedRecord((yield* stored(s.doubled)).record).pending.seqs?.output).toBe(1);
    }),
  ));

test("a stored stream epoch that is not the generation fails load/stale-epoch", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("tampered");
      yield* s.initialize;
      yield* Projection.run(s.doubled);
      const current = yield* stored(s.doubled);
      if (current.record === undefined) throw new Error("Expected a record");
      yield* (yield* CheckpointStore).compareAndSet(Projection.key(s.doubled), current.token, {
        identity: current.record.identity,
        inputs: current.record.inputs,
        adapters: { outputs: { output: { epoch: 7, nextSeq: 1 } } },
      });
      const failure = fault(yield* Projection.run(s.doubled).pipe(Effect.result));
      expect(failure.phase).toBe("load");
      expect(failure.reason).toBe("stale-epoch");
    }),
  ));

test("a two-input pending unit reproduces both ranges", () =>
  run(
    Effect.gen(function* () {
      const a = In.at({ name: "pair-a" });
      const b = In.at({ name: "pair-b" });
      const output = Out.at({ name: "pair" });
      const seen: Array<{
        readonly a: ReadonlyArray<number>;
        readonly b: ReadonlyArray<number>;
        readonly unit: Unit;
      }> = [];
      const pair = Projection.mapAccumBatch({
        commit: Commit.producer,
        id: "pair",
        params: {},
        inputs: { a, b },
        outputs: { output: output },
        state: { schema: Schema.Null, initial: null },
        mapAccumBatch: (_state, batch, unit) =>
          Effect.sync(() => {
            seen.push({ a: batch.a.items, b: batch.b.items, unit });
            return [
              null,
              {
                output: Projection.items(batch).map((entry) => `${entry.input}:${entry.item}`),
              },
            ] as const;
          }),
      }).at({});
      yield* Streams.create(a);
      yield* Streams.create(b);
      yield* Streams.create(output);
      yield* Streams.append(a, [1, 2]);
      yield* Streams.append(b, [10]);
      const failing = failingAppends(1);
      fault(yield* withWriter(Projection.run(pair), failing.patch).pipe(Effect.result));
      const pinned = pinnedRecord((yield* stored(pair)).record);
      expect(Object.keys(pinned.pending.ranges)).toEqual(["a", "b"]);
      expect(pinned.pending.ranges.a?.count).toBe(2);
      expect(pinned.pending.ranges.b?.count).toBe(1);

      const settled = yield* withWriter(Projection.run(pair), failing.patch);
      expect(failing.outcomes).toEqual(["Appended"]);
      expect(settled.items).toBe(3);
      expect(seen[1]?.a).toEqual([1, 2]);
      expect(seen[1]?.b).toEqual([10]);
      expect(seen[1]?.unit.key).toBe(seen[0]?.unit.key ?? "");
      expect(Object.keys(seen[1]?.unit.ranges ?? {})).toEqual(["a", "b"]);
      expect(yield* readAll(output)).toEqual(["a:1", "a:2", "b:10"]);
      const after = yield* stored(pair);
      expect(after.record?.inputs).toEqual({
        a: pinned.pending.ranges.a?.nextOffset ?? "",
        b: pinned.pending.ranges.b?.nextOffset ?? "",
      });
    }),
  ));

test("producer ids join params to the id", () => {
  expect(producerId("x", 1, {})).toBe("x/v1");
  expect(producerId("x", 1, { b: "2", a: "1" })).toBe(`x/v1/${JSON.stringify({ a: "1", b: "2" })}`);
});

test("a missing output is created before the first pin", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("auto-created");
      yield* Streams.create(s.input);
      yield* Streams.append(s.input, [1, 2]);
      const result = yield* Projection.run(s.doubled);
      expect(result.status).toBe("caught-up");
      expect(yield* readAll(s.output)).toEqual(["2", "4"]);
    }),
  ));

test("a gone output still faults before pinning", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("gone-output");
      yield* s.initialize;
      yield* (yield* StreamsWriter).fork(Out.at({ name: "retained-output" }).id, s.output.id);
      yield* Streams.remove(s.output);
      const error = fault(yield* Projection.run(s.doubled).pipe(Effect.result));
      expect(error.phase).toBe("pin");
      expect(error.reason).toBe("invalid-output");
      expect((yield* stored(s.doubled)).token).toBe("0");
    }),
  ));

test("an over-long retry page is truncated and settles by Duplicate", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("grown-retry");
      yield* s.initialize;
      let saves = 0;
      fault(
        yield* withCheckpointStore(Projection.run(s.doubled), (owner) => ({
          compareAndSet: (key, token, record) =>
            ++saves === 2
              ? Effect.fail(
                  new ProjectionFault({
                    phase: "checkpoint",
                    reason: "storage-failure",
                    message: "crash",
                  }),
                )
              : owner.compareAndSet(key, token, record),
        })).pipe(Effect.result),
      );
      yield* Streams.append(s.input, [4, 5], { close: true });
      const appends = failingAppends(0);
      const result = yield* withWriter(Projection.pass(s.doubled), appends.patch);
      expect(appends.outcomes).toEqual(["Duplicate"]);
      expect(result.items).toBe(3);
      expect(result.status).toBe("progress");
      expect(s.seen[1]?.items).toEqual([1, 2, 3]);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6"]);
      expect((yield* Projection.run(s.doubled)).items).toBe(2);
      expect(yield* readAll(s.output)).toEqual(["2", "4", "6", "8", "10"]);
    }),
  ));

test("an over-long page whose log ends before the pin is unreproducible", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("short-pin-boundary");
      yield* s.initialize;
      const failing = failingAppends(1);
      const first = fault(
        yield* withWriter(Projection.run(s.doubled), failing.patch).pipe(Effect.result),
      );
      expect(first.cause).toBe(appendFault);
      const before = yield* stored(s.doubled);
      const owner = yield* CheckpointStore;
      const record = before.record;
      if (!record?.pending) throw new Error("Expected pin");
      yield* owner.compareAndSet(s.doubled, before.token, {
        identity: record.identity,
        inputs: record.inputs,
        adapters: record.adapters,
        pending: {
          seqs: record.pending.seqs,
          ranges: {
            input: {
              from: ZERO_OFFSET,
              count: 1,
              nextOffset: "9999999999999999_0000000000000000",
            },
          },
        },
      });
      const error = fault(yield* Projection.run(s.doubled).pipe(Effect.result));
      expect(error.phase).toBe("pin");
      expect(error.reason).toBe("range-unreproducible");
      expect(error.message).toContain("shorter than the pin");
      expect(yield* readAll(s.output)).toEqual([]);
    }),
  ));

test("a closed output faults before pinning", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("closed-output");
      yield* s.initialize;
      yield* Streams.append(s.output, [], { close: true });
      const error = fault(yield* Projection.run(s.doubled).pipe(Effect.result));
      expect(error.phase).toBe("pin");
      expect(error.reason).toBe("invalid-output");
      expect((yield* stored(s.doubled)).token).toBe("0");
    }),
  ));

const provideWriter =
  (writer: Writer<StreamsFault>) =>
  <A, E, R>(body: Effect.Effect<A, E, R>) =>
    Effect.flatMap(Out, (service) =>
      body.pipe(
        Effect.provideService(Out, { ...service, wire: { ...service.wire, ...writer } }),
        Effect.provideService(StreamsWriter, writer),
      ),
    );
const provideReader =
  (reader: Reader<StreamsFault>) =>
  <A, E, R>(body: Effect.Effect<A, E, R>) =>
    Effect.flatMap(In, (service) =>
      body.pipe(
        Effect.provideService(In, { ...service, wire: { ...service.wire, ...reader } }),
        Effect.provideService(StreamsReader, reader),
      ),
    );

test("output preparation reads the output stream wire", () =>
  run(
    Effect.gen(function* () {
      const s = scenario("output-head-failure");
      yield* s.initialize;
      const output = yield* Out;
      const headFault = new StorageFault({
        operation: "head",
        message: "output unavailable",
        retryable: true,
      });
      let inspected = 0;
      const result = yield* Projection.pass(s.doubled).pipe(
        Effect.provideService(Out, {
          ...output,
          wire: {
            ...output.wire,
            head: () => {
              inspected += 1;
              return Effect.fail(headFault);
            },
          },
        }),
        Effect.result,
      );
      expect(inspected).toBe(1);
      expect(fault(result).reason).toBe("storage-failure");
      expect((yield* stored(s.doubled)).record).toBeUndefined();
    }),
  ));
