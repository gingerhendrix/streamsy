import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { Reactivity } from "effect/reactivity";
import { SqlClient, Statement } from "effect/sql";
import { View, ViewFault } from "../../src/index.ts";
import { backendFor } from "../../src/sql/backend.ts";

it.live("rejects unsupported SQL dialects in backend selection and the public migration", () =>
  Effect.gen(function* () {
    for (const dialect of ["mssql", "clickhouse"] as const) {
      const sql = yield* SqlClient.make({
        acquirer: Effect.die(new Error("Unsupported dialect acquired a connection")),
        compiler: { ...Statement.makeCompilerSqlite(), dialect },
        driver: dialect,
        spanAttributes: [],
      });

      for (const fault of [
        yield* backendFor(sql).pipe(Effect.flip),
        yield* View.migrate(sql).pipe(Effect.flip),
      ]) {
        expect(fault).toBeInstanceOf(ViewFault);
        expect(fault.message).toContain(dialect);
        expect(fault.message).toContain("sqlite, pg and mysql");
      }
    }
  }).pipe(Effect.provide(Reactivity.layer)),
);
