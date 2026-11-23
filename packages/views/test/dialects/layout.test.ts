/* oxlint-disable effecttsgo/strict-effect-provide -- The two layout fixtures run in isolated database scopes. */
import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Effect, Order } from "effect";
import { SqlClient } from "effect/sql";
import { layoutGroup, layoutNamespace, runLayoutUnits } from "../circuit/support/layout.ts";
import { hostFor, postgres, sqlite } from "../support/backends.ts";

const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

const sorted = <A>(rows: readonly A[]) =>
  Arr.sort(rows, Order.mapInput(Order.String, JSON.stringify));

const sha256 = (key: string) =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", new TextEncoder().encode(key)));

const dump = (namespace: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    return yield* sql.onDialectOrElse({
      pg: () =>
        Effect.gen(function* () {
          const ns = new Uint8Array(yield* sha256(namespace));

          const integrals = yield* sql<{
            relation: Uint8Array;
            row_key: Uint8Array;
            value: string;
          }>`
            SELECT relation, row_key, value FROM streamsy_view_integrals WHERE ns = ${ns}`;

          const arrangements = yield* sql<{
            arrangement: Uint8Array;
            row_key: Uint8Array;
            index_hash: string;
            sort_key: string;
            hash_ok: boolean;
            prefix_ok: boolean;
          }>`SELECT arrangement, row_key, encode(index_hash, 'hex') AS index_hash, sort_key,
              row_hash = sha256(row_key) AS hash_ok,
              ord_prefix = substring(convert_to(sort_key, 'UTF8') || decode('00', 'hex') || row_key FOR 1024) AS prefix_ok
              FROM streamsy_view_arrangements WHERE ns = ${ns}`;

          const accumulators = yield* sql<{ node: Uint8Array; group_key: string; value: string }>`
            SELECT node, group_key, value FROM streamsy_view_accumulators WHERE ns = ${ns}`;

          const owners = yield* sql<{
            relation: Uint8Array;
            output_key: Uint8Array;
            owner_key: Uint8Array;
          }>`
            SELECT relation, output_key, owner_key FROM streamsy_view_key_owners WHERE ns = ${ns}`;

          const circuits =
            yield* sql`SELECT namespace, plan_hash FROM streamsy_view_circuits WHERE ns = ${ns}`
              .values;

          expect(arrangements).not.toHaveLength(0);

          for (const { hash_ok, prefix_ok } of arrangements) {
            expect(hash_ok).toBe(true);
            expect(prefix_ok).toBe(true);
          }

          return {
            integrals: sorted(
              Arr.map(integrals, (row) => [
                text.decode(row.relation),
                text.decode(row.row_key),
                row.value,
              ]),
            ),
            arrangements: sorted(
              Arr.map(arrangements, (row) => [
                text.decode(row.arrangement),
                row.index_hash,
                row.sort_key,
                text.decode(row.row_key),
              ]),
            ),
            accumulators: sorted(
              Arr.map(accumulators, (row) => [text.decode(row.node), row.group_key, row.value]),
            ),
            owners: sorted(
              Arr.map(owners, (row) => [
                text.decode(row.relation),
                text.decode(row.output_key),
                text.decode(row.owner_key),
              ]),
            ),
            circuits,
          };
        }),
      orElse: () =>
        Effect.gen(function* () {
          const arrangements = yield* sql<{
            arrangement: string;
            index_key: string;
            sort_key: string;
            row_key: string;
          }>`SELECT arrangement, index_key, sort_key, row_key FROM streamsy_view_arrangements
            WHERE namespace = ${namespace}`;

          const hashed = yield* Effect.forEach(arrangements, (row) =>
            Effect.map(sha256(row.index_key), (index) => [
              row.arrangement,
              Buffer.from(index).toString("hex"),
              row.sort_key,
              row.row_key,
            ]),
          );

          return {
            integrals: sorted(
              yield* sql`SELECT relation, row_key, value FROM streamsy_view_integrals WHERE namespace = ${namespace}`
                .values,
            ),
            arrangements: sorted(hashed),
            accumulators: sorted(
              yield* sql`SELECT node, group_key, value FROM streamsy_view_accumulators WHERE namespace = ${namespace}`
                .values,
            ),
            owners: sorted(
              yield* sql`SELECT relation, output_key, owner_key FROM streamsy_view_key_owners WHERE namespace = ${namespace}`
                .values,
            ),
            circuits:
              yield* sql`SELECT namespace, plan_hash FROM streamsy_view_circuits WHERE namespace = ${namespace}`
                .values,
          };
        }),
    });
  });

const fixedDump = runLayoutUnits.pipe(Effect.andThen(dump(layoutNamespace)));

describe.skipIf(postgres.server.url === undefined)("Postgres layout", () => {
  it.live(
    "matches SQLite's logical dump of the pinned units and checks every arrangement prefix and hash",
    () =>
      Effect.gen(function* () {
        const sqliteDump = yield* fixedDump.pipe(Effect.provide(hostFor(sqlite, layoutGroup)));
        const pgDump = yield* fixedDump.pipe(Effect.provide(hostFor(postgres, layoutGroup)));
        expect(pgDump).toEqual(sqliteDump);
      }),
  );
});
