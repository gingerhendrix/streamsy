/* oxlint-disable effecttsgo/strict-effect-provide -- This test provisions an isolated SQLite host. */
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { View } from "../../src/index.ts";

const Entry = Schema.Struct({ id: Schema.String, team: Schema.String, score: Schema.Finite });

class Source extends Streamsy.state<Source>()("Layout/Source", "layout/source/:board", {
  params: { board: Schema.String },
  collections: {
    entry: { key: "id", schema: Entry },
    team: { key: "id", schema: Schema.Struct({ id: Schema.String, name: Schema.String }) },
  },
}) {}

class Output extends Streamsy.state<Output>()("Layout/Output", "layout/out/:board/g/:generation", {
  params: { board: Schema.String, generation: Schema.FiniteFromString },
  collections: {
    ranked: {
      key: "id",
      schema: Schema.Struct({ id: Schema.String, label: Schema.String }),
    },
    totals: {
      key: "team",
      schema: Schema.Struct({ team: Schema.String, entries: Schema.Finite }),
    },
  },
}) {}

const entries = View.from(Source, "entry");

const ranked = entries
  .join(View.from(Source, "team"), {
    as: "details",
    on: (entry, team) => entry.team.eq(team.id),
  })
  .top({
    partitionBy: (row) => row.team,
    by: (row) => [row.score.desc(), row.id.asc()],
    limit: 1,
  })
  .select((row) => ({ id: row.id, label: row.details.name }));

const totals = entries
  .groupBy((entry) => ({ team: entry.team }))
  .aggregate(() => ({ entries: View.count() }));

const circuit = View.circuit({
  id: "layout",
  params: Source.paramCodecs,
  generation: 2,
  output: Projection.perGeneration(Output, "generation"),
  views: { ranked, totals },
});

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(
    BunStorage.layer(Streamsy.group(Source, Output), { client: { filename: ":memory:" } }),
  ),
);

// A changed storage pin is a layout break that requires raising the plan `version`
// and adding a breaking release note.
it.effect("pins the plan hash and every operator table after two circuit units", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* View.migrate(sql);
    const source = yield* Source;
    const params = { board: "b" };
    const binding = circuit.at(params);
    const address = Source.at(params);
    yield* source.create(params);

    const units = [
      [
        DurableState.upsert("team", { id: "t", name: "Team" }),
        DurableState.upsert("entry", { id: "ä", team: "t", score: 5 }),
        DurableState.upsert("entry", { id: "z", team: "t", score: -2 }),
      ],
      [DurableState.upsert("entry", { id: "z", team: "t", score: 5 })],
    ] as const;

    for (const changes of units) {
      yield* source.append(params, DurableState.changes(address, {}, changes));
      expect((yield* Projection.run(binding, { limit: 1 })).units).toBe(1);
    }

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

    const namespace = '["[\\"layout\\",1,{\\"board\\":\\"b\\"}]",2]';

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
