import { Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { expect } from "@effect/vitest";
import { View } from "../../../src/index.ts";

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

export const layoutGroup = Streamsy.group(Source, Output);

export const runLayoutUnits = Effect.gen(function* () {
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
});

export const layoutNamespace = '["[\\"layout\\",1,{\\"board\\":\\"b\\"}]",2]';
