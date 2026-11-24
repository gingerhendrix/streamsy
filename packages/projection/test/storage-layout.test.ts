/* oxlint-disable effecttsgo/strict-effect-provide -- Each test provisions an isolated SQLite host. */
import { expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { Commit, Projection } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { withFamilies } from "./support/families.ts";
import { layerSqlite } from "./support/sqlite.ts";

class Source extends Streamsy.json<Source>()("Layout/Source", "layout/source", {
  params: {},
  schema: Schema.Finite,
}) {}

class Routed extends Streamsy.json<Routed>()(
  "Layout/Routed",
  "layout/routed/:workspace/:bucket/g/:generation",
  {
    params: {
      workspace: Schema.String,
      bucket: Schema.String,
      generation: Schema.FiniteFromString,
    },
    schema: Schema.Finite,
  },
) {}

class Produced extends Streamsy.json<Produced>()("Layout/Produced", "layout/produced", {
  params: {},
  schema: Schema.Finite,
}) {}

const host = withFamilies(layerSqlite(), Source, Routed, Produced);

const seed = Effect.gen(function* () {
  const source = yield* Source;
  yield* source.create({});
  yield* source.append({}, [42]);
});

const routed = Projection.process({
  id: "layout-routed",
  params: { workspace: Schema.String, region: Schema.String },
  generation: 7,
  inputs: { source: Source },
  outputs: { out: Projection.perGeneration(Routed, "generation", { routeBy: ["bucket"] }) },
  commit: Commit.transaction,
  process: (batch, _unit, { outputs: { out } }) =>
    Effect.gen(function* () {
      const target = yield* out.open({ bucket: "day" });
      const { wire } = yield* Routed;
      yield* Streams.append(target, [batch.source.items.length]).pipe(
        Effect.provideService(StreamsWriter, wire),
      );
    }),
});

const routedParams = { workspace: "w", region: "eu" };

const producer = {
  id: "layout-producer",
  generation: 4,
  inputs: { source: Source },
  outputs: { out: Produced },
  commit: Commit.producer,
  emit: () => Effect.succeed({ out: [1] }),
};

const plain = Projection.emit({ ...producer, params: {} }).at({});

const withParams = Projection.emit({
  ...producer,
  params: { workspace: Schema.String },
});

it("pins checkpoint keys for declarations that omit version", () => {
  expect(recordKey(Projection.key(plain))).toBe(
    '["streamsy.projection.v1","layout-producer","1","4","{}"]',
  );
  expect(recordKey(Projection.key(routed.at(routedParams)))).toBe(
    '["streamsy.projection.v1","layout-routed","1","7","{\\"region\\":\\"eu\\",\\"workspace\\":\\"w\\"}"]',
  );
});

it.effect("pins the live route record, first checkpoint envelope and retired tombstone", () =>
  Effect.gen(function* () {
    yield* seed;
    const records = yield* OwnerRecords;
    const binding = routed.at(routedParams);
    const checkpointSlot = recordKey(Projection.key(binding));

    expect((yield* Projection.run(binding, { limit: 1 })).units).toBe(1);
    expect(yield* records.list('["streamsy.projection.routed.v1",')).toEqual([
      [
        '["streamsy.projection.routed.v1","layout-routed","1","7","{\\"region\\":\\"eu\\",\\"workspace\\":\\"w\\"}","out","layout/routed/w/day/g/7"]',
        "{}",
      ],
    ]);
    expect(yield* records.read(checkpointSlot)).toEqual(
      Option.some(
        '{"version":1,"record":{"identity":{"inputs":{"source":"[\\"layout/source\\",\\"application/json\\"]"}},"inputs":{"source":"0000000000000001_0000000000000000"},"closed":{},"adapters":{}}}',
      ),
    );

    yield* Projection.retire(routed, routedParams, { generation: 7 });
    expect(yield* records.read(checkpointSlot)).toEqual(Option.some('{"version":2,"retired":{}}'));
  }).pipe(Effect.provide(host)),
);

it.effect("pins producer ids written by declarations that omit version", () =>
  Effect.gen(function* () {
    yield* seed;

    for (const binding of [
      plain,
      withParams.at({ workspace: "w" }),
      withParams.at({ workspace: "x".repeat(256) }),
    ]) {
      expect((yield* Projection.run(binding, { limit: 1 })).units).toBe(1);
    }

    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT producer_id FROM streamsy_producers ORDER BY producer_id`).toEqual([
      { producer_id: "layout-producer/v1" },
      {
        producer_id:
          "layout-producer/v1/#b589bc9b21ae731d947691d6407b0a2692ab16d9ab6d0d8881398356b461f736",
      },
      { producer_id: 'layout-producer/v1/{"workspace":"w"}' },
    ]);
  }).pipe(Effect.provide(host)),
);
