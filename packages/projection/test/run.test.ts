import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { expect, test } from "bun:test";
import { Array, Deferred, Effect, Fiber, Option, Schema } from "effect";
import { ZERO_OFFSET, type StreamsFault } from "@streamsy/core";
import { Streams, StreamsReader, type Reader } from "@streamsy/core/protocol";
import { SqlClient } from "effect/sql";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { layerSqlite } from "./support/sqlite.ts";
import { minimal, Numbers, Doubled } from "../src/examples/minimal.ts";
import {
  failAtSave,
  initialize,
  input,
  inspect,
  positives,
  readAll,
  type Services,
} from "./support/scenarios.ts";

const run = <A, E>(
  body: Effect.Effect<A, E, Services | SqlClient.SqlClient | Numbers | Doubled>,
  readLimit = 1000,
) =>
  Effect.runPromise(
    body.pipe(Effect.provide(FamilyFixture.withFamilies(layerSqlite(readLimit), Numbers, Doubled))),
  );
const failure = <A, E>(
  body: Effect.Effect<A, E, Services | SqlClient.SqlClient | Numbers | Doubled>,
) =>
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
const sum = Projection.fold({
  id: "sum",
  params: {},
  inputs: { input },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (total, { item }) => total + item,
}).at({});
/** Substitutes the reader's `read` for one effect; every other operation is untouched. */
const withRead = <A, E, R>(
  body: Effect.Effect<A, E, R | StreamsReader>,
  read: (reader: Reader<StreamsFault>) => Reader<StreamsFault>["read"],
) =>
  Effect.gen(function* () {
    const reader = yield* StreamsReader;
    return yield* body.pipe(
      FamilyFixture.provideReader(StreamsReader.of({ ...reader, read: read(reader) })),
    );
  });

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
      expect(empty.offsets).toEqual(full.offsets);
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
      expect(second.offsets.input).not.toBe(first.offsets.input);
      const stored = yield* inspect;
      expect(stored.output).toEqual([1]);
      expect(stored.loaded.token).toBe("2");
      expect(Option.getOrThrow(stored.loaded.record).inputs).toEqual(second.offsets);
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
      const other = FamilyFixture.json("elsewhere", { schema: Schema.Finite });
      yield* Streams.create(other);
      const changed = Projection.process({
        commit: Commit.transaction,
        id: positives.id,
        params: {},
        inputs: { input: other },
        process: () => Effect.void,
      }).at({});
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
      const sql = yield* SqlClient.SqlClient;
      const key = recordKey(Projection.key(positives));
      const rows = yield* sql<{
        readonly value: string;
      }>`SELECT owner_value AS value FROM streamsy_owner_records WHERE owner_key = ${key}`;
      const stored = Option.getOrThrow(Option.fromUndefinedOr(rows[0])).value;
      const bad = stored.replace(/"input":"\d{16}_\d{16}"/, '"input":"not-an-offset"');
      expect(bad).not.toBe(stored);
      for (const value of ["{not json", '{"version":1,"record":{}}', bad]) {
        yield* sql`UPDATE streamsy_owner_records SET owner_value = ${value} WHERE owner_key = ${key}`;
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
      const doubled = FamilyFixture.json("doubled", { schema: Schema.Finite });
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
      const raced = <A, E>(pass: Effect.Effect<A, E, Services>) =>
        failure(
          withRead(
            pass,
            (reader) => (id, options) =>
              pass.pipe(
                FamilyFixture.provideReader(reader),
                Effect.provide(services),
                Effect.orDie,
                Effect.andThen(reader.read(id, options)),
              ),
          ),
        ).pipe(Effect.map(fault));
      const failed = yield* raced(Projection.pass(positives));
      expect(failed.phase).toBe("checkpoint");
      expect(failed.reason).toBe("token-conflict");
      const stored = yield* inspect;
      expect(stored.output).toEqual([1, 3]);
      expect(stored.loaded.token).toBe("1");
      // The loser of a fold race leaves the winner's state.
      expect((yield* raced(Projection.pass(sum))).reason).toBe("token-conflict");
      expect((yield* Projection.read(sum)).state).toBe(3);
      expect((yield* (yield* CheckpointStore).load(sum)).token).toBe("1");
    }),
  ));

test("two competing passes on one SQLite host: the second conflicts and output is not doubled", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const owner = yield* CheckpointStore;
      const inside = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const loaded = yield* Deferred.make<void>();
      const firstDone = yield* Deferred.make<void>();
      const gated = Projection.process({
        commit: Commit.transaction,
        id: positives.id,
        params: {},
        inputs: { input },
        // Both runners load the old token before either SQL transaction starts.
        prepare: () =>
          Deferred.succeed(inside, undefined).pipe(Effect.andThen(Deferred.await(release))),
        process: (batch, unit) => positives.process(batch, unit),
      }).at({});
      const first = yield* Effect.forkChild(Projection.pass(gated));
      yield* Deferred.await(inside);
      const observing = CheckpointStore.of({
        ...owner,
        load: (key) => owner.load(key).pipe(Effect.tap(() => Deferred.succeed(loaded, undefined))),
      });
      const second = yield* Effect.forkChild(
        Projection.pass(
          Projection.process({
            commit: Commit.transaction,
            id: positives.id,
            params: {},
            inputs: { input },
            prepare: () => Deferred.await(firstDone),
            process: (batch, unit) => positives.process(batch, unit),
          }).at({}),
        ).pipe(Effect.provideService(CheckpointStore, observing)),
      );
      yield* Deferred.await(loaded);
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(first)).items).toBe(3);
      yield* Deferred.succeed(firstDone, undefined);
      const failed = fault(yield* failure(Fiber.join(second)));
      expect(failed.phase).toBe("checkpoint");
      expect(failed.reason).toBe("token-conflict");
      const stored = yield* inspect;
      expect(stored.output).toEqual([1, 3]);
      expect(stored.loaded.token).toBe("1");
    }),
  ));

test("handler failure rolls back the output append and the checkpoint", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      class Boom extends Schema.TaggedError<Boom>()("Boom", {}) {}
      const failing = Projection.process({
        commit: Commit.transaction,
        id: positives.id,
        params: {},
        inputs: { input },
        process: (batch, unit) => positives.process(batch, unit).pipe(Effect.andThen(new Boom())),
      }).at({});
      const failed = yield* failure(Projection.run(failing));
      expect(Option.getOrThrow(failed)).toBeInstanceOf(Boom);
      const stored = yield* inspect;
      expect(stored.output).toEqual([]);
      expect(Option.isNone(stored.loaded.record)).toBe(true);
    }),
  ));

test("run without a limit drains more than one hundred server pages", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(
        input,
        Array.makeBy(101, (n) => n),
      );
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "uncapped",
        params: {},
        inputs: { input },
        process: () => Effect.void,
      }).at({});
      const result = yield* Projection.run(projection);
      expect(result.status).toBe("caught-up");
      expect(result.units).toBe(101);
      expect(result.items).toBe(101);
    }),
    1,
  ));
