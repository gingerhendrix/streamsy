import { describe, expect, it } from "@effect/vitest";
import { Cause, Context, Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { View } from "../../src/index.ts";
import { backendFor } from "../../src/sql/backend.ts";
import { dialectClient } from "../../../storage/test/dialects/support.ts";
import { fixtureDatabase, mysql } from "../support/backends.ts";

describe.skipIf(mysql.server.url === undefined)("MySQL Views driver", () => {
  it.live("preserves bound binary bytes under different connection character sets", () =>
    Effect.gen(function* () {
      const url = yield* fixtureDatabase(mysql.server);

      const sql = Context.get(
        yield* Layer.build(dialectClient(mysql.server, url, { maxConnections: 1 })),
        SqlClient.SqlClient,
      );

      yield* View.migrate(sql);
      const backend = yield* backendFor(sql);
      const store = backend.store("charset");
      const arrangement = "m😀";
      const rowKey = "\ufeffm\u0000é😀";
      const encoder = new TextEncoder();

      for (const charset of ["utf8mb4", "latin1", "ascii"]) {
        yield* sql.unsafe(`SET NAMES ${charset}`);
        yield* store.clearIndex(arrangement);
        yield* store.snapshotMark(arrangement, rowKey);
        expect(yield* store.lookup(arrangement, "")).toEqual([{ row_key: rowKey, sort_key: "" }]);

        const [row] = yield* sql<{
          arrangement: Buffer;
          row_key: Buffer;
        }>`SELECT arrangement, row_key FROM streamsy_view_arrangements`;

        expect(Buffer.isBuffer(row?.arrangement)).toBe(true);
        expect(Buffer.isBuffer(row?.row_key)).toBe(true);
        expect(row?.arrangement).toEqual(Buffer.from(encoder.encode(arrangement)));
        expect(row?.row_key).toEqual(Buffer.from(encoder.encode(rowKey)));
      }
    }),
  );

  it.live("fails decoding invalid stored UTF-8 instead of silently replacing it", () =>
    Effect.gen(function* () {
      const url = yield* fixtureDatabase(mysql.server);

      const sql = Context.get(
        yield* Layer.build(dialectClient(mysql.server, url)),
        SqlClient.SqlClient,
      );

      yield* View.migrate(sql);
      const backend = yield* backendFor(sql);
      const store = backend.store("fatal");
      yield* store.snapshotMark("a0", "valid");
      yield* sql`UPDATE streamsy_view_arrangements SET row_key = X'C328'`;
      const cause = yield* store.lookup("a0", "").pipe(Effect.sandbox, Effect.flip);
      expect(Cause.squash(cause)).toBeInstanceOf(TypeError);
    }),
  );
});
