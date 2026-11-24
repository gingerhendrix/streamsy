/* oxlint-disable effecttsgo/strict-effect-provide -- Each case opens its own dialect host. */
import { describe, expect, it } from "@effect/vitest";
import { DurableState, StreamAddress, Streamsy } from "@streamsy/core";
import { NonRetryable, Projection } from "@streamsy/projection";
import { Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { normalizeUnit } from "../../src/circuit-engine.ts";
import { View } from "../../src/index.ts";
import { canonicalJson, encodeRowKey } from "../../src/runtime/key.ts";
import { ViewDataFault } from "../../src/sql/data-fault.ts";
import { makeEngine } from "../../src/sql/engine.ts";
import { createBackend, hostFor, serverFixtures } from "../support/backends.ts";

const collections = {
  row: { key: "id", schema: Schema.Struct({ id: Schema.String, target: Schema.String }) },
} as const;

class Input extends Streamsy.state<Input>()("ViewsTest/ServerFaults", "server-faults/input", {
  params: {},
  collections,
}) {}

class WideSource extends Streamsy.state<WideSource>()("é".repeat(120), "server-faults/wide", {
  params: {},
  collections,
}) {}

class Output extends Streamsy.state<Output>()(
  "ViewsTest/ServerFaultsOutput",
  "server-faults/output/g/:generation",
  { params: { generation: Schema.FiniteFromString }, collections },
) {}

const output = Projection.perGeneration(Output, "generation");

const lone = "\ud800";

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} Views data faults`, () => {
    const host = hostFor(fixture, Streamsy.group(Input, WideSource, Output));

    for (const kind of ["engine row key", "snapshot mark", "owner output key"] as const) {
      it.live(`surfaces a lone surrogate from ${kind} as non-retryable to Projection`, () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* View.migrate(sql);
          const source = yield* Input;
          yield* source.create({});
          const own = kind === "owner output key";
          const row = View.from(Input, "row");

          const binding = View.circuit({
            id: "pg-surrogate",
            params: {},
            output,
            views: { row: own ? row.select((r) => ({ id: r.target, target: r.target })) : row },
          }).at({});

          const before = yield* Projection.read(binding);
          const entry = DurableState.upsert("row", { id: own ? "valid" : lone, target: lone });

          const events =
            kind === "snapshot mark"
              ? ([StreamAddress.snapshotStart(), entry] as const)
              : ([entry] as const);

          yield* source.append({}, DurableState.changes(Input.at({}), {}, events));

          const fault = yield* Projection.run(binding).pipe(Effect.flip);
          expect(fault).toBeInstanceOf(ViewDataFault);
          expect(fault).toMatchObject({
            key: kind === "snapshot mark" ? lone : encodeRowKey(lone),
            reason: expect.stringContaining("lone surrogate"),
            [NonRetryable]: true,
          });
          expect(yield* Projection.read(binding)).toEqual(before);
          expect(yield* sql`SELECT plan_hash FROM streamsy_view_circuits`).toEqual([]);
          expect(yield* sql`SELECT row_key FROM streamsy_view_arrangements`).toEqual([]);
          expect(yield* sql`SELECT row_key FROM streamsy_view_integrals`).toEqual([]);
        }).pipe(Effect.provide(host)),
      );
    }

    it.live("validates an inactive source before preparing the circuit namespace", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend(fixture);
        const engine = yield* makeEngine({});
        const id = canonicalJson([WideSource.key, "row"]);
        const sources = new Map([[id, { state: WideSource, collection: "row" }]]);
        const circuit = backend.circuit("invalid-source");
        let prepared = false;

        const prepareNamespace = Effect.gen(function* () {
          prepared = true;
          yield* circuit.save("plan");

          return "invalid-source";
        });

        const failure = yield* backend
          .atomic(normalizeUnit(engine, backend, prepareNamespace, sources, {}))
          .pipe(Effect.flip);

        expect(failure).toMatchObject({
          _tag: "ViewDataFault",
          view: "*",
          node: id,
          key: id,
          message: `*/${id}: Views identifier for ${canonicalJson(id)} exceeds 255 UTF-8 bytes`,
          [NonRetryable]: true,
        });
        expect(prepared).toBe(false);
        expect(yield* circuit.load).toBeUndefined();
      }),
    );

    it.live("rejects an overlong UTF-8 identifier in the first unit and names the source", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* View.migrate(sql);
        const source = yield* WideSource;
        yield* source.create({});
        yield* source.append(
          {},
          DurableState.changes(WideSource.at({}), {}, [
            DurableState.upsert("row", { id: "a", target: "b" }),
          ]),
        );

        const binding = View.circuit({
          id: "pg-wide-source",
          params: {},
          output,
          views: { row: View.from(WideSource, "row") },
        }).at({});

        const before = yield* Projection.read(binding);
        const fault = yield* Projection.run(binding).pipe(Effect.flip);
        expect(fault).toBeInstanceOf(ViewDataFault);
        expect(fault).toMatchObject({
          key: canonicalJson([WideSource.key, "row"]),
          reason: expect.stringContaining("255 UTF-8 bytes"),
          [NonRetryable]: true,
        });
        expect(yield* Projection.read(binding)).toEqual(before);
        expect(yield* sql`SELECT plan_hash FROM streamsy_view_circuits`).toEqual([]);
      }).pipe(Effect.provide(host)),
    );
  });
}
