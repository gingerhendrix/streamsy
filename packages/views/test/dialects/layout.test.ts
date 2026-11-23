/* oxlint-disable effecttsgo/strict-effect-provide -- The two layout fixtures run in isolated database scopes. */
import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Effect, Order } from "effect";
import { SqlClient } from "effect/sql";
import { layoutGroup, layoutNamespace, runLayoutUnits } from "../circuit/support/layout.ts";
import { hostFor, serverFixtures, sqlite } from "../support/backends.ts";

const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

const sorted = <A>(rows: readonly A[]) =>
  Arr.sort(rows, Order.mapInput(Order.String, JSON.stringify));

const sha256 = (key: string) =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", new TextEncoder().encode(key)));

const dump = (namespace: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const server = () =>
      Effect.gen(function* () {
        const hash = sql.onDialectOrElse({
          mysql: () => sql`LOWER(HEX(index_hash))`,
          orElse: () => sql`encode(index_hash, 'hex')`,
        });

        const rowHash = sql.onDialectOrElse({
          mysql: () => sql`UNHEX(SHA2(row_key, 256))`,
          orElse: () => sql`sha256(row_key)`,
        });

        const prefix = sql.onDialectOrElse({
          mysql: () => sql`LEFT(CONCAT(CAST(sort_key AS BINARY), X'00', row_key), 1024)`,
          orElse: () =>
            sql`substring(convert_to(sort_key, 'UTF8') || decode('00', 'hex') || row_key FOR 1024)`,
        });

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
          hash_ok: boolean | number;
          prefix_ok: boolean | number;
        }>`SELECT arrangement, row_key, ${hash} AS index_hash, sort_key,
              row_hash = ${rowHash} AS hash_ok,
              ord_prefix = ${prefix} AS prefix_ok
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
          expect(Number(hash_ok)).toBe(1);
          expect(Number(prefix_ok)).toBe(1);
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
      });

    return yield* sql.onDialectOrElse({
      pg: server,
      mysql: server,
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

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} layout`, () => {
    it.live(
      "matches SQLite's logical dump of the pinned units and checks every arrangement prefix and hash",
      () =>
        Effect.gen(function* () {
          const sqliteDump = yield* fixedDump.pipe(Effect.provide(hostFor(sqlite, layoutGroup)));
          const serverDump = yield* fixedDump.pipe(Effect.provide(hostFor(fixture, layoutGroup)));
          expect(serverDump).toEqual(sqliteDump);
        }),
    );
  });
}
