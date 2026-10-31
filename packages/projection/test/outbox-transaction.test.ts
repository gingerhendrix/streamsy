import { it } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { Outbox, OutboxStore } from "@streamsy/outbox";
import { layerSql, migrateOutbox } from "@streamsy/outbox/sql";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
} from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";

class Orders extends Streamsy.json<Orders>()("outbox-transaction/Orders", "orders", {
  params: {},
  schema: Schema.Struct({ id: Schema.String }),
}) {}

const receipts = Outbox.define({
  name: "shop.receipt",
  payload: Schema.Struct({ id: Schema.String }),
  idempotencyKey: (receipt) => `receipt/${receipt.id}`,
  retry: { maxAttempts: 3, initialDelayMs: 100, factor: 2, maxDelayMs: 1000 },
});

it.effect(
  "Commit.transaction rolls back causal writes and enqueue when checkpoint settlement fails",
  () =>
    Effect.gen(function* () {
      const graph = layerSqlite();
      // The outbox store reads the same SqlClient that Transaction.layer reads.
      const outboxGraph = Layer.merge(graph, layerSql.pipe(Layer.provide(graph)));

      const services = yield* Layer.build(
        Protocol.bind(graph, Streamsy.group(Orders)).pipe(Layer.provideMerge(outboxGraph)),
      );

      return yield* Effect.gen(function* () {
        const orders = yield* Orders;
        yield* orders.create({});
        yield* orders.append({}, [{ id: "42" }]);

        const tx = yield* Transaction;
        const sql = tx.sql;
        yield* migrateOutbox(sql);
        const outbox = yield* OutboxStore;
        const checkpoints = yield* CheckpointStore;
        yield* sql`CREATE TABLE causal_receipts (id TEXT PRIMARY KEY)`;
        let processCalls = 0;
        let settlementCalls = 0;

        const projection = Projection.process({
          id: "enqueue-receipts",
          commit: Commit.transaction,
          params: {},
          inputs: { orders: Orders },
          process: (batch) =>
            Effect.gen(function* () {
              // Commit supplies the transaction owner's exact client to the processor.
              const client = yield* SqlClient.SqlClient;
              expect(client).toBe(sql);
              processCalls += 1;

              for (const order of batch.orders.items)
                yield* client`INSERT INTO causal_receipts VALUES (${order.id})`;
              yield* Outbox.enqueue(receipts, batch.orders.items);
            }),
        }).at({});

        const failure = new ProjectionFault({
          phase: "checkpoint",
          reason: "storage-failure",
          message: "injected checkpoint settlement failure",
        });

        const failed = yield* Projection.run(projection).pipe(
          Effect.provideService(CheckpointStore, {
            ...checkpoints,
            compareAndSet: () =>
              Effect.gen(function* () {
                settlementCalls += 1;
                // Both writes already exist in this real owner transaction when settlement fails.
                expect(yield* sql`SELECT * FROM causal_receipts`.pipe(Effect.orDie)).toEqual([
                  { id: "42" },
                ]);
                expect(
                  yield* outbox.list({ name: receipts.name }).pipe(Effect.orDie),
                ).toMatchObject([{ idempotencyKey: "receipt/42", state: "pending" }]);

                return yield* failure;
              }),
          }),
          Effect.result,
        );

        expect(failed).toMatchObject({ _tag: "Failure", failure });
        expect(processCalls).toBe(1);
        expect(settlementCalls).toBe(1);
        expect(yield* sql`SELECT * FROM causal_receipts`).toEqual([]);
        expect(yield* outbox.list({ name: receipts.name })).toEqual([]);
        expect((yield* checkpoints.load(projection)).token).toBe("0");

        const delivered: string[] = [];

        const drain = Outbox.drain(receipts, ({ idempotencyKey }) =>
          Effect.sync(() => {
            delivered.push(idempotencyKey);
          }),
        ).pipe(Effect.provideService(OutboxStore, outbox));

        expect(yield* drain).toMatchObject({ attempted: 0 });
        expect((yield* Projection.run(projection)).items).toBe(1);
        expect(processCalls).toBe(2);
        expect(yield* sql`SELECT * FROM causal_receipts`).toEqual([{ id: "42" }]);
        expect((yield* checkpoints.load(projection)).token).not.toBe("0");
        expect(yield* drain).toMatchObject({ attempted: 1, delivered: 1 });
        expect((yield* Projection.run(projection)).items).toBe(0);
        expect(yield* drain).toMatchObject({ attempted: 0 });
        expect(delivered).toEqual(["receipt/42"]);
        expect(yield* outbox.list({ name: receipts.name })).toMatchObject([
          { idempotencyKey: "receipt/42", state: "delivered", attempts: 1 },
        ]);
      }).pipe(Effect.provideContext(services));
    }),
);
