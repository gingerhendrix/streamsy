import { expect, test } from "bun:test";
import { Effect, Layer, Option, Result } from "effect";
import { SqlClient } from "effect/sql";
import { CommitBoundary, MemoryStorage, Streamsy } from "@streamsy/core";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import type { CheckpointRecord } from "@streamsy/projection/checkpoint";

const key = { id: "storage", version: 1, generation: 1, params: {} };

const record: CheckpointRecord = {
  identity: { inputs: {} },
  inputs: {},
  adapters: {},
  state: "42",
};

test("standalone memory checkpoints compare tokens and keep state without a Transaction", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* CheckpointStore;
      expect(Option.isNone(yield* Effect.serviceOption(Transaction))).toBe(true);
      expect(yield* store.load(key)).toEqual({ record: Option.none(), token: "0" });

      const results = yield* Effect.all(
        [
          store.compareAndSet(key, "0", record).pipe(Effect.result),
          store
            .compareAndSet(key, "0", {
              identity: record.identity,
              inputs: record.inputs,
              adapters: record.adapters,
              state: "99",
            })
            .pipe(Effect.result),
        ],
        { concurrency: "unbounded" },
      );

      expect(
        results.map((result) => (Result.isSuccess(result) ? "Success" : "Failure")).toSorted(),
      ).toEqual(["Failure", "Success"]);
      expect((yield* store.load(key)).token).toBe("1");
      yield* store.remove(key);
      expect((yield* store.load(key)).record).toEqual(Option.none());
    }).pipe(Effect.provide(CheckpointStore.layerMemory)),
  ));

const sqlite = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layer(Streamsy.group(), { client: { filename: ":memory:" } })),
);

test("SQL owner checkpoints join the owner transaction and roll back as one row", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* CheckpointStore;
      const transaction = yield* Transaction;
      const client = yield* SqlClient.SqlClient;
      expect(transaction.sql).toBe(client);

      const failed = yield* transaction
        .withTransaction(
          Effect.gen(function* () {
            yield* store.compareAndSet(key, "0", record);
            expect(Option.getOrThrow((yield* store.load(key)).record).state).toBe("42");

            return yield* Effect.fail("rollback");
          }),
        )
        .pipe(Effect.result);

      expect(Result.isFailure(failed)).toBe(true);
      expect((yield* store.load(key)).token).toBe("0");
      yield* store.compareAndSet(key, "0", record);
      expect(yield* client.unsafe("SELECT owner_key FROM streamsy_owner_records")).toHaveLength(1);

      for (const legacy of ["streamsy_projection_v1_state", "streamsy_projection_v2_records"])
        expect(
          yield* client.unsafe("SELECT name FROM sqlite_master WHERE name = ?", [legacy]),
        ).toHaveLength(0);
    }).pipe(Effect.provide(sqlite)),
  ));

test("memory owner checkpoints roll back with the boundary transaction", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* CheckpointStore;
      const boundary = yield* CommitBoundary;

      const failed = yield* boundary
        .withTransaction(
          Effect.andThen(store.compareAndSet(key, "0", record), Effect.fail("rollback")),
        )
        .pipe(Effect.result);

      expect(Result.isFailure(failed)).toBe(true);
      expect((yield* store.load(key)).token).toBe("0");
      expect(yield* store.compareAndSet(key, "0", record)).toBe("1");
      expect(Option.isNone(yield* Effect.serviceOption(Transaction))).toBe(true);
    }).pipe(
      Effect.provide(
        CheckpointStore.layer.pipe(Layer.provideMerge(MemoryStorage.layer(Streamsy.group()))),
      ),
    ),
  ));
