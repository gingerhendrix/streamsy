import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Exit, Layer, Option, Order, Result } from "effect";
import { SqlClient } from "effect/sql";
import { CommitBoundary, MemoryStorage, Streamsy } from "@streamsy/core";
import { ownerOf } from "@streamsy/core/internal";
import { CheckpointStore, Commit, Projection, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { recordKey, type CheckpointRecord } from "@streamsy/projection/checkpoint";

const key = { id: "storage", generation: 1, params: {} };

const record: CheckpointRecord = {
  identity: { inputs: {} },
  inputs: {},
  adapters: {},
  state: "42",
};

layer(CheckpointStore.layerMemory)((it) => {
  it.effect("legacy keys cannot access the default checkpoint or route slot", () =>
    Effect.gen(function* () {
      const store = yield* CheckpointStore;
      const routes = store.routes!;
      const explicitDefault = { ...key, version: 1 };
      const explicitUndefined = { ...key, version: undefined };
      expect(recordKey(explicitDefault)).toBe(recordKey(key));
      expect(recordKey(explicitUndefined)).toBe(recordKey(key));
      yield* store.compareAndSet(explicitDefault, "0", record);
      yield* routes.add(explicitDefault, "out", "stream");

      for (const version of [3, null, "1"]) {
        const legacy = { ...key, version };

        const error = new RangeError(
          `Projection ${key.id}: version was removed from checkpoint keys.`,
        );

        expect(() => recordKey(legacy)).toThrow(error);

        const operations = [
          () => Projection.forget({ ...legacy, commit: Commit.checkpoint }),
          () => Projection.forget({ ...legacy, commit: { remove: () => Effect.void } }),
          () => store.load(legacy),
          () => store.remove(legacy),
          () => store.retire!(legacy),
          () => store.compareAndSet(legacy, "1", record),
          () => routes.has(legacy, "out", "stream"),
          () => routes.state(legacy, "out", "stream"),
          () => routes.add(legacy, "out", "stream"),
          () => routes.list(legacy),
          () => routes.forget(legacy),
          () => routes.clear(legacy),
        ];

        for (const operation of operations) {
          const exit = yield* Effect.suspend(() => operation().pipe(Effect.asVoid)).pipe(
            Effect.exit,
          );

          expect(Result.getOrThrow(Exit.findDefect(exit))).toEqual(error);
        }
      }

      expect((yield* store.load(key)).record).toEqual(Option.some(record));
      expect(yield* routes.state(key, "out", "stream")).toBe("live");
      yield* Projection.forget({ ...explicitDefault, commit: Commit.checkpoint });
      expect((yield* store.load(key)).record).toEqual(Option.none());
    }),
  );

  it.effect(
    "standalone memory checkpoints compare tokens and keep state without a Transaction",
    () =>
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
          Array.sort(
            Array.map(results, (result) => (Result.isSuccess(result) ? "Success" : "Failure")),
            Order.String,
          ),
        ).toEqual(["Failure", "Success"]);
        expect((yield* store.load(key)).token).toBe("1");
        yield* store.remove(key);
        expect((yield* store.load(key)).record).toEqual(Option.none());
      }),
  );
});

const sqlite = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layer(Streamsy.group(), { client: { filename: ":memory:" } })),
);

layer(sqlite)((it) => {
  it.effect("SQL owner checkpoints join the owner transaction and roll back as one row", () =>
    Effect.gen(function* () {
      const store = yield* CheckpointStore;
      const transaction = yield* Transaction;
      const boundary = yield* CommitBoundary;
      expect(transaction.owner).toBe(ownerOf(boundary));
      const client = yield* SqlClient.SqlClient;

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
    }),
  );
});

layer(CheckpointStore.layer.pipe(Layer.provideMerge(MemoryStorage.layer(Streamsy.group()))))(
  (it) => {
    it.effect("memory owner checkpoints roll back with the boundary transaction", () =>
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
      }),
    );
  },
);
