import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Option, Schema } from "effect";
import {
  StreamRef,
  Streams,
  StreamsReader,
  ZERO_OFFSET,
  type Reader,
  type StreamsFault,
} from "@streamsy/core";
import { MemoryCommitBoundary } from "@streamsy/core/internal/memory";
import { Checkpoints, Projection, ProjectionFault } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { layerMemory } from "@streamsy/projection/memory";
import { minimal } from "../src/examples/minimal.ts";
import {
  composition,
  failAtSave,
  initialize,
  input,
  inspect,
  positives,
  readAll,
  type Services,
} from "./support/scenarios.ts";

const run = <A, E>(body: Effect.Effect<A, E, Services | MemoryCommitBoundary>, readLimit = 1000) =>
  Effect.runPromise(body.pipe(Effect.provide(layerMemory({ readLimit }))));
const failure = <A, E>(body: Effect.Effect<A, E, Services | MemoryCommitBoundary>) =>
  Effect.gen(function* () {
    const result = yield* body.pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    return result._tag === "Failure" ? Option.some(result.failure) : Option.none<E>();
  });
const fault = (value: Option.Option<unknown>): ProjectionFault => {
  const error = Option.getOrThrow(value);
  if (!(error instanceof ProjectionFault)) throw new Error("Expected a ProjectionFault");
  return error;
};
/** Substitutes the reader's `read` for one effect; every other operation is untouched. */
const withRead = <A, E, R>(
  body: Effect.Effect<A, E, R | StreamsReader>,
  read: (reader: Reader<StreamsFault>) => Reader<StreamsFault>["read"],
) =>
  Effect.gen(function* () {
    const reader = yield* StreamsReader;
    return yield* body.pipe(
      Effect.provideService(StreamsReader, StreamsReader.of({ ...reader, read: read(reader) })),
    );
  });

test("fresh ordered run, fused rollback and restart without repeated output", () =>
  run(
    Effect.gen(function* () {
      const result = yield* composition;
      expect(result.first.status).toBe("limit-reached");
      expect(result.first.items).toBe(1);
      expect(result.failed).toBe("Failure");
      expect(result.after).toEqual(result.before);
      expect(result.final.status).toBe("caught-up");
      expect(result.final.items).toBe(2);
      expect(result.restart.items).toBe(0);
      expect(result.restart.status).toBe("caught-up");
      expect(result.stored.output).toEqual([1, 3]);
      expect(result.stored.loaded.token).toBe("3");
    }),
    1,
  ));

test("a non-empty pass is progress even when its read is up to date", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const full = yield* Projection.pass(positives);
      expect(full.items).toBe(3);
      expect(full.status).toBe("progress");
      const empty = yield* Projection.pass(positives);
      expect(empty.status).toBe("caught-up");
      expect(empty.units).toBe(0);
      expect(empty.items).toBe(0);
      expect(empty.record).toEqual(full.record);
    }),
  ));

test("run reaches caught-up through a trailing empty pass without losing totals", () =>
  run(
    Effect.gen(function* () {
      let reads = 0;
      yield* initialize;
      const result = yield* withRead(Projection.run(positives), (reader) => (id, options) => {
        reads += 1;
        return reader.read(id, options);
      });
      expect(result.status).toBe("caught-up");
      expect(result.units).toBe(1);
      expect(result.items).toBe(3);
      expect(reads).toBe(2);
      expect((yield* inspect).loaded.token).toBe("1");
    }),
  ));

test("zero-output unit advances the checkpoint without output progress", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const first = yield* Projection.pass(positives);
      const second = yield* Projection.pass(positives);
      expect(second.items).toBe(1);
      expect(second.record.inputs.input).not.toBe(first.record.inputs.input);
      const stored = yield* inspect;
      expect(stored.output).toEqual([1]);
      expect(stored.loaded.token).toBe("2");
      expect(Option.getOrThrow(stored.loaded.record).inputs).toEqual(second.record.inputs);
    }),
    1,
  ));

test("first commit failure leaves no output and no record", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const failed = fault(
        yield* failAtSave.pipe(
          Effect.map((r) => (r._tag === "Failure" ? Option.some(r.failure) : Option.none())),
        ),
      );
      expect(failed.phase).toBe("checkpoint");
      const stored = yield* inspect;
      expect(Option.isNone(stored.loaded.record)).toBe(true);
      expect(stored.loaded.token).toBe("0");
      expect(stored.output).toEqual([]);
    }),
  ));

test("limit counts checkpoint transactions", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const limited = yield* Projection.run(positives, { limit: 2 });
      expect(limited.status).toBe("limit-reached");
      expect(limited.units).toBe(2);
      expect(limited.items).toBe(2);
      const rest = yield* Projection.run(positives);
      expect(rest.units).toBe(1);
      expect(rest.items).toBe(1);
      expect(rest.status).toBe("caught-up");
    }),
    1,
  ));

test("identity mismatch on inputs stops without reset", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      yield* Projection.run(positives);
      const before = yield* inspect;
      const other = StreamRef.json("elsewhere", { schema: Schema.Finite });
      yield* Streams.create(other);
      const changed = Projection.make({
        id: positives.id,
        input: other,
        process: () => Effect.void,
      });
      const failed = fault(yield* failure(Projection.run(changed)));
      expect(failed.phase).toBe("load");
      expect(failed.reason).toBe("identity-mismatch");
      expect(yield* inspect).toEqual(before);
    }),
  ));

test("closed input drains before source-closed", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      yield* Streams.append(input, [], { close: true });
      const partial = yield* Projection.run(positives, { limit: 2 });
      expect(partial.status).toBe("limit-reached");
      expect(partial.items).toBe(2);
      const result = yield* Projection.run(positives);
      expect(result.status).toBe("source-closed");
      expect(result.items).toBe(1);
      expect((yield* inspect).output).toEqual([1, 3]);
      expect((yield* Projection.run(positives)).status).toBe("source-closed");
    }),
    1,
  ));

test("missing history preserves the accepted record", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      yield* Projection.pass(positives);
      const before = yield* inspect;
      yield* Streams.remove(input);
      const failed = fault(yield* failure(Projection.run(positives)));
      expect(failed.phase).toBe("read");
      expect(failed.reason).toBe("history-unavailable");
      expect(failed.input).toBe("input");
      expect(yield* inspect).toEqual(before);
    }),
  ));

test("invalid stored record stops", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      yield* Projection.run(positives);
      const boundary = yield* MemoryCommitBoundary;
      const key = recordKey(Projection.key(positives));
      const stored = Option.getOrThrow(yield* boundary.read(key));
      const bad = stored.replace(/"input":"\d{16}_\d{16}"/, '"input":"not-an-offset"');
      expect(bad).not.toBe(stored);
      for (const value of ["{not json", '{"version":1,"record":{}}', bad]) {
        yield* boundary.write(key, value);
        const failed = fault(yield* failure(Projection.run(positives)));
        expect(failed.phase).toBe("load");
        expect(failed.reason).toBe("invalid-record");
      }
    }),
  ));

test("invalid options fails before reading", () =>
  run(
    Effect.gen(function* () {
      let reads = 0;
      yield* initialize;
      for (const options of [{ limit: 0 }, { limit: -1 }, { limit: Infinity }, { limit: 1.5 }]) {
        const failed = fault(
          yield* failure(
            withRead(Projection.run(positives, options), (reader) => (id, readOptions) => {
              reads += 1;
              return reader.read(id, readOptions);
            }),
          ),
        );
        expect(failed.phase).toBe("load");
        expect(failed.reason).toBe("invalid-options");
      }
      expect(reads).toBe(0);
      expect(Option.isNone((yield* inspect).loaded.record)).toBe(true);
    }),
  ));

test("compiled minimal example doubles three inputs", () =>
  run(
    Effect.gen(function* () {
      const result = yield* minimal;
      expect(result.status).toBe("caught-up");
      expect(result.items).toBe(3);
      expect(result.units).toBe(1);
      const doubled = StreamRef.json("doubled", { schema: Schema.Finite });
      expect(yield* readAll(doubled)).toEqual([2, 4, 6]);
    }),
  ));

test("nonadvancing read is rejected without a commit", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const failed = fault(
        yield* failure(
          withRead(
            Projection.run(positives),
            (reader) => (id, options) =>
              reader
                .read(id, options)
                .pipe(Effect.map((result) => ({ ...result, nextOffset: ZERO_OFFSET }))),
          ),
        ),
      );
      expect(failed.phase).toBe("read");
      expect(failed.reason).toBe("invalid-source");
      expect(failed.input).toBe("input");
      expect(Option.isNone((yield* inspect).loaded.record)).toBe(true);
    }),
  ));

test("record change between load and transaction is a token conflict", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const services = yield* Effect.context<Services>();
      // A competing runner completes inside this runner's read window.
      const failed = fault(
        yield* failure(
          withRead(
            Projection.run(positives),
            (reader) => (id, options) =>
              Projection.run(positives).pipe(
                Effect.provide(services),
                Effect.provideService(StreamsReader, reader),
                Effect.orDie,
                Effect.andThen(reader.read(id, options)),
              ),
          ),
        ),
      );
      expect(failed.phase).toBe("checkpoint");
      expect(failed.reason).toBe("token-conflict");
      const stored = yield* inspect;
      expect(stored.output).toEqual([1, 3]);
      expect(stored.loaded.token).toBe("1");
    }),
  ));

test("two runners on one memory host: the second conflicts and output is not doubled", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const owner = yield* Checkpoints;
      const inside = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const loaded = yield* Deferred.make<void>();
      const gated = Projection.make({
        id: positives.id,
        input,
        process: (batch, unit) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(inside, undefined);
            yield* Deferred.await(release);
            yield* positives.process(batch, unit);
          }),
      });
      const first = yield* Effect.forkChild(Projection.run(gated));
      yield* Deferred.await(inside);
      const observing = Checkpoints.of({
        ...owner,
        load: (key) => owner.load(key).pipe(Effect.tap(() => Deferred.succeed(loaded, undefined))),
      });
      const second = yield* Effect.forkChild(
        Projection.run(positives).pipe(Effect.provideService(Checkpoints, observing)),
      );
      yield* Deferred.await(loaded);
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(first)).items).toBe(3);
      const failed = fault(yield* failure(Fiber.join(second)));
      expect(failed.phase).toBe("checkpoint");
      expect(failed.reason).toBe("token-conflict");
      const stored = yield* inspect;
      expect(stored.output).toEqual([1, 3]);
      expect(stored.loaded.token).toBe("1");
    }),
  ));

test("interruption inside the transaction rolls back the output append and the checkpoint", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const appended = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const blocking = Projection.make({
        id: positives.id,
        input,
        process: (batch, unit) =>
          positives
            .process(batch, unit)
            .pipe(
              Effect.andThen(Deferred.succeed(appended, undefined)),
              Effect.andThen(Deferred.await(release)),
            ),
      });
      const fiber = yield* Effect.forkChild(Projection.run(blocking));
      yield* Deferred.await(appended);
      yield* Fiber.interrupt(fiber);
      const [exit] = yield* Fiber.awaitAll([fiber]);
      expect(exit !== undefined && Exit.hasInterrupts(exit)).toBe(true);
      const stored = yield* inspect;
      expect(stored.output).toEqual([]);
      expect(Option.isNone(stored.loaded.record)).toBe(true);
      expect(stored.loaded.token).toBe("0");
      const result = yield* Projection.run(positives);
      expect(result.items).toBe(3);
      expect((yield* inspect).output).toEqual([1, 3]);
    }),
  ));

test("handler failure rolls back the output append and the checkpoint", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      class Boom extends Schema.TaggedError<Boom>()("Boom", {}) {}
      const failing = Projection.make({
        id: positives.id,
        input,
        process: (batch, unit) => positives.process(batch, unit).pipe(Effect.andThen(new Boom())),
      });
      const failed = yield* failure(Projection.run(failing));
      expect(Option.getOrThrow(failed)).toBeInstanceOf(Boom);
      const stored = yield* inspect;
      expect(stored.output).toEqual([]);
      expect(Option.isNone(stored.loaded.record)).toBe(true);
    }),
  ));

test("version one keeps existing record keys and version two starts a fresh key", () => {
  const definition = { id: "versioned", input, process: () => Effect.void };
  const original = Projection.make(definition);
  const one = Projection.make({ ...definition, version: 1 });
  const two = Projection.make({ ...definition, version: 2 });
  expect(recordKey(Projection.key(original))).toBe(
    '["streamsy.projection.v1","versioned","1","{}"]',
  );
  expect(recordKey(Projection.key(one))).toBe(recordKey(Projection.key(original)));
  expect(recordKey(Projection.key(two))).not.toBe(recordKey(Projection.key(original)));
});

test("run without a limit drains more than one hundred server pages", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(
        input,
        Array.from({ length: 101 }, (_, n) => n),
      );
      const projection = Projection.make({ id: "uncapped", input, process: () => Effect.void });
      const result = yield* Projection.run(projection);
      expect(result.status).toBe("caught-up");
      expect(result.units).toBe(101);
      expect(result.items).toBe(101);
    }),
    1,
  ));

test("stream units retain the declared version on a fresh pass and pinned retry", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const versions: number[] = [];
      const output = StreamRef.json("versioned-output", { schema: Schema.Finite });
      const projection = Projection.stream({
        id: "versioned-stream",
        version: 7,
        input,
        output,
        process: (batch, unit) =>
          Effect.sync(() => {
            versions.push(unit.version);
            return batch.input.items;
          }),
      });
      const owner = yield* Checkpoints;
      let saves = 0;
      const failed = yield* Projection.run(projection).pipe(
        Effect.provideService(Checkpoints, {
          ...owner,
          save: (key, record, token) => {
            saves += 1;
            return saves === 2
              ? Effect.fail(
                  new ProjectionFault({
                    phase: "checkpoint",
                    reason: "storage-failure",
                    message: "after append",
                  }),
                )
              : owner.save(key, record, token);
          },
        }),
        Effect.result,
      );
      expect(failed._tag).toBe("Failure");
      expect(Option.getOrThrow((yield* owner.load(projection)).record).pending).toBeDefined();
      expect(versions).toEqual([7]);
      yield* Projection.run(projection);
      expect(versions).toEqual([7, 7]);
      expect(Option.getOrThrow((yield* owner.load(projection)).record).pending).toBeUndefined();
      expect(yield* readAll(output)).toEqual([1, -1, 3]);
    }),
  ));
