/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the SQLite runtime and transaction instrumentation. */
import { expect, test } from "vitest";
import { Array, Cause, Effect, Exit, Schema } from "effect";
import { Protocol, Streams, StreamsWriter } from "@streamsy/core/protocol";
import { Streamsy, MemoryStorage } from "@streamsy/core";
import { Layer } from "effect";
import { CheckpointStore, Commit, Projection, Transaction } from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";
import * as Family from "./support/families.ts";

const input = Family.json("prepare-input", { schema: Schema.Finite });

const host = Family.withFamilies(layerSqlite());

class Sink extends Streamsy.json<Sink>()("process/Sink", "process/sink/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}

test("process and prepare receive bound outputs; only the handler appends", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [2, 3]);
      const sink = yield* Sink;
      yield* sink.create({ id: "a" });
      const address = Sink.at({ id: "a" });

      const projection = Projection.process({
        id: "process-outputs",
        params: {},
        inputs: { input },
        outputs: { sink: address },
        commit: Commit.transaction,
        prepare: (_batch, _unit, { outputs }) => Effect.succeed(outputs.sink.id),
        process: (batch, _unit, { prepared, outputs }) => {
          expect(prepared).toBe(address.id);

          return Effect.gen(function* () {
            const { wire } = yield* outputs.sink.stream;

            if (!Array.isReadonlyArrayNonEmpty(batch.input.items)) return;
            yield* Streams.append(outputs.sink, batch.input.items).pipe(
              Effect.provideService(StreamsWriter, wire),
            );
          });
        },
      }).at({});

      yield* Projection.run(projection);
      expect((yield* sink.collect({ id: "a" })).items).toEqual([2, 3]);
    }).pipe(
      Effect.provide(Protocol.bind(host, Streamsy.group(Sink)).pipe(Layer.provideMerge(host))),
    ),
  ));

test("process checks a declared output on another owner before invoking the handler", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      let called = false;

      const projection = Projection.process({
        id: "process-foreign-output",
        params: {},
        inputs: { input },
        outputs: { sink: Sink.at({ id: "foreign" }) },
        commit: Commit.transaction,
        process: () =>
          Effect.sync(() => {
            called = true;
          }),
      }).at({});

      expect(yield* Projection.run(projection).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure: { phase: "load", reason: "unsupported-composition" },
      });
      expect(called).toBe(false);
      expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("0");
    }).pipe(
      Effect.provide(
        Layer.merge(host, Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Sink))),
      ),
    ),
  ));

test("prepare runs outside the transaction and passes its result into process", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [2, 3]);
      const transaction = yield* Transaction;
      let inside = false;
      let preparedKey = "";
      let calls = 0;

      const projection = Projection.process({
        id: "prepared",
        params: {},
        inputs: { input },
        commit: Commit.transaction,
        prepare: (batch, unit) =>
          Effect.sync(() => {
            expect(inside).toBe(false);
            preparedKey = unit.key;

            return Array.reduce(batch.input.items, 0, (sum, n) => sum + n);
          }),
        process: (_batch, unit, { prepared }) =>
          Effect.sync(() => {
            expect(inside).toBe(true);
            expect(unit.key).toBe(preparedKey);
            expect(prepared).toBe(5);
            calls += 1;
          }),
      }).at({});

      yield* Projection.run(projection).pipe(
        Effect.provideService(Transaction, {
          ...transaction,
          withTransaction: (body) =>
            transaction.withTransaction(
              Effect.sync(() => {
                inside = true;
              }).pipe(
                Effect.andThen(body),
                Effect.ensuring(
                  Effect.sync(() => {
                    inside = false;
                  }),
                ),
              ),
            ),
        }),
      );
      expect(calls).toBe(1);
    }).pipe(Effect.provide(host)),
  ));

test("prepare requires the transaction capability, including through compensation", () => {
  const definition = {
    id: "prepare-check",
    params: {},
    inputs: { input },
    prepare: () => Effect.void,
    process: () => Effect.void,
  };

  expect(() => Projection.process({ ...definition, commit: Commit.atLeastOnce })).toThrow(
    "prepare requires Commit.transaction",
  );
  const forged = Commit.make({ ...Commit.atLeastOnce, name: "transaction" });
  expect(() => Projection.process({ ...definition, commit: forged })).toThrow(
    "prepare requires Commit.transaction",
  );
  expect(() =>
    Projection.process({
      ...definition,
      commit: Commit.transaction.pipe(Commit.withCompensation(() => Effect.void)),
    }),
  ).not.toThrow();
});

test("a prepare failure preserves the handler error and never enters the transaction", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const transaction = yield* Transaction;
      let entered = false;
      let processed = false;

      const projection = Projection.process({
        id: "prepare-failed",
        params: {},
        inputs: { input },
        commit: Commit.transaction,
        prepare: () => Effect.fail("prepare failed"),
        process: () =>
          Effect.sync(() => {
            processed = true;
          }),
      }).at({});

      const result = yield* Projection.run(projection).pipe(
        Effect.provideService(Transaction, {
          ...transaction,
          withTransaction: (body) => {
            entered = true;

            return transaction.withTransaction(body);
          },
        }),
        Effect.result,
      );

      expect(result).toMatchObject({ _tag: "Failure", failure: "prepare failed" });
      expect(entered).toBe(false);
      expect(processed).toBe(false);
    }).pipe(Effect.provide(host)),
  ));

for (const extended of [false, true]) {
  test(`prepare repeats after a crash; range ${extended ? "extension changes" : "replay preserves"} the key`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* Streams.create(input);
        yield* Streams.append(input, [1]);
        const transaction = yield* Transaction;
        const store = yield* CheckpointStore;
        const keys: string[] = [];
        const batches: number[][] = [];
        let processed = 0;

        const declaration = Projection.process({
          id: "prepare-crash",
          params: {},
          inputs: { input },
          commit: Commit.transaction,
          prepare: (batch, unit) =>
            Effect.sync(() => {
              keys.push(unit.key);
              batches.push([...batch.input.items]);

              return batch.input.items.length;
            }),
          process: (_batch, _unit, { prepared }) =>
            Effect.sync(() => {
              processed += prepared;
            }),
        });

        const crash = new Error("crash after prepare, before transaction");

        const exit = yield* Projection.run(declaration.at({})).pipe(
          Effect.provideService(Transaction, {
            ...transaction,
            withTransaction: () => Effect.die(crash),
          }),
          Effect.exit,
        );

        expect(Exit.isFailure(exit)).toBe(true);

        if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(crash);
        expect(processed).toBe(0);
        expect((yield* store.load(declaration.at({}))).token).toBe("0");

        if (extended) yield* Streams.append(input, [2]);
        // Rebind after the simulated crash; only the host's stored checkpoint survives.
        yield* Projection.run(declaration.at({}));
        expect(keys).toHaveLength(2);
        expect(keys[1] === keys[0]).toBe(!extended);
        expect(batches).toEqual(extended ? [[1], [1, 2]] : [[1], [1]]);
        expect(processed).toBe(extended ? 2 : 1);
      }).pipe(Effect.provide(host)),
    ));
}
