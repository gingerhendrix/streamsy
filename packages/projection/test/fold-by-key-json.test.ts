import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, FoldByKey, Projection, StateTable, Transaction } from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";

class Facts extends Streamsy.json<Facts>()("FoldJson/Facts", "fold-json-facts", {
  params: {},
  schema: Schema.Finite,
}) {}

const Row = Schema.Struct({ id: Schema.String, count: Schema.FiniteFromString, date: Schema.Date });

type Row = typeof Row.Type;

class Output extends Streamsy.state<Output>()("FoldJson/Output", "fold-json-output/:workspace", {
  params: { workspace: Schema.String },
  collections: { count: { schema: Row, key: "id" } },
}) {}

const Table = StateTable.define(Output);

const local = layerSqlite();

const streams = Protocol.bind(local, Streamsy.group(Facts, Output)).pipe(Layer.provideMerge(local));

const host = Layer.effectDiscard(Effect.flatMap(SqlClient.SqlClient, StateTable.migrate)).pipe(
  Layer.provideMerge(streams),
);

layer(host)((it) => {
  it.effect(
    "compares stored JSON, encodes Date and number transforms once, and binds a fixed output independently of unit params",
    () =>
      Effect.gen(function* () {
        const values = { workspace: "fixed" };
        const stream = yield* Output;
        const table = Table.at(values);
        const source = yield* Facts;
        const date = yield* Schema.decodeEffect(Schema.DateFromString)("2026-11-09T00:00:00.000Z");
        yield* stream.create(values);
        // Both "05" and "5" decode to 5. Only comparison against stored JSON sees the change.
        yield* stream.wire.append(Output.at(values).id, {
          contentType: "application/json",
          data: new TextEncoder().encode(
            '{"type":"count","key":"a","value":{"id":"a","count":"05","date":"2026-11-09T00:00:00.000Z"},"headers":{"operation":"upsert"}}',
          ),
        });

        const fold = (previous: Row | undefined, n: number): Row => ({
          id: "a",
          count: (previous?.count ?? 0) + n,
          date: previous?.date ?? date,
        });

        const binding = Projection.process({
          id: "json-comparison",
          params: {},
          inputs: { facts: Facts },
          outputs: { output: Output.at(values) },
          commit: Commit.transaction,
          process: FoldByKey.process(Table, {
            output: "output",
            collection: "count",
            key: () => "a",
            fold,
          }),
        }).at({});

        yield* source.create({});
        yield* source.append({}, [0]);
        yield* Projection.run(binding);
        expect((yield* stream.collect(values)).items).toHaveLength(2);
        yield* Effect.flatMap(Transaction, (tx) => tx.withTransaction(table.sync()));
        expect((yield* table.get("count", "a")).value).toEqual({ id: "a", count: 5, date });
        const sql = yield* SqlClient.SqlClient;
        expect(
          (yield* sql<{
            value: string;
          }>`SELECT value FROM streamsy_state_rows WHERE stream_id = ${Output.at(values).id}`)[0]
            ?.value,
        ).toBe('{"id":"a","count":"5","date":"2026-11-09T00:00:00.000Z"}');
        const before = (yield* stream.head(values)).nextOffset;
        yield* source.append({}, [0]);
        yield* Projection.run(binding);
        expect((yield* stream.head(values)).nextOffset).toBe(before);
      }),
  );
});
