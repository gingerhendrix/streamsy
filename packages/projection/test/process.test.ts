/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the memory runtime and transaction instrumentation. */
import { expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import { Streams } from "@streamsy/core/protocol";
import { Commit, Projection, Transaction } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Family from "./support/families.ts";

const input = Family.json("prepare-input", { schema: Schema.Number });
const host = Family.withFamilies(Memory.layerMemory());

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
            return batch.input.items.reduce((sum, n) => sum + n, 0);
          }),
        process: (_batch, unit, prepared) =>
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
