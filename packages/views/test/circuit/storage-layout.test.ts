/* oxlint-disable effecttsgo/strict-effect-provide -- This test provisions an isolated SQLite host. */
import { expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { layoutGroup, layoutNamespace, runLayoutUnits } from "./support/layout.ts";

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layer(layoutGroup, { client: { filename: ":memory:" } })),
);

// A changed storage pin is a layout break that requires raising the plan `version`
// and adding a breaking release note.
it.effect("pins the plan hash and every operator table after two circuit units", () =>
  Effect.gen(function* () {
    yield* runLayoutUnits;
    const sql = yield* SqlClient.SqlClient;

    const stored = {
      integrals: yield* sql`SELECT * FROM streamsy_view_integrals
        ORDER BY namespace, relation, row_key`.values,
      arrangements: yield* sql`SELECT * FROM streamsy_view_arrangements
        ORDER BY namespace, arrangement, index_key, sort_key, row_key`.values,
      accumulators: yield* sql`SELECT * FROM streamsy_view_accumulators
        ORDER BY namespace, node, group_key`.values,
      keyOwners: yield* sql`SELECT * FROM streamsy_view_key_owners
        ORDER BY namespace, relation, output_key`.values,
      circuits: yield* sql`SELECT * FROM streamsy_view_circuits ORDER BY namespace`.values,
    };

    const namespace = layoutNamespace;

    expect(stored).toEqual({
      integrals: [
        [namespace, "r0", "s1:z", '{"key":"z","row":{"id":"z","team":"t","score":5}}'],
        [namespace, "r0", "s1:ä", '{"key":"ä","row":{"id":"ä","team":"t","score":5}}'],
        [namespace, "r1", "s1:t", '{"key":"t","row":{"id":"t","name":"Team"}}'],
        [
          namespace,
          "r2",
          "a:s5:matchs4:s1:zs4:s1:t",
          '{"key":["match","s1:z","s1:t"],"row":{"id":"z","team":"t","score":5,"details":{"id":"t","name":"Team"}}}',
        ],
        [
          namespace,
          "r2",
          "a:s5:matchs4:s1:äs4:s1:t",
          '{"key":["match","s1:ä","s1:t"],"row":{"id":"ä","team":"t","score":5,"details":{"id":"t","name":"Team"}}}',
        ],
      ],
      arrangements: [
        [namespace, "a0", '["t"]', "", "s1:z"],
        [namespace, "a0", '["t"]', "", "s1:ä"],
        [namespace, "a1", '["t"]', "", "s1:t"],
        [namespace, "a2", '["t"]', "e3febffffffffffff20007b00000", "a:s5:matchs4:s1:zs4:s1:t"],
        [namespace, "a2", '["t"]', "e3febffffffffffff2000e500000", "a:s5:matchs4:s1:äs4:s1:t"],
      ],
      accumulators: [[namespace, "r6", '["t"]', '{"count":2,"sums":{}}']],
      keyOwners: [
        [namespace, "r5", "s1:z", "a:s5:matchs4:s1:zs4:s1:t"],
        [namespace, "r7", "s1:t", "a:s1:t"],
      ],
      circuits: [[namespace, "22effe27"]],
    });
  }).pipe(Effect.provide(host)),
);
