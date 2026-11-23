import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { View } from "../../src/index.ts";
import { mysql } from "../../src/sql/mysql.ts";
import { postgres } from "../../src/sql/postgres.ts";
import { serverBackend } from "../../src/sql/server.ts";
import { engineConformance } from "../sql/conformance-suite.ts";
import { client, createBackend, serverFixtures } from "../support/backends.ts";

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} engine conformance`, () => {
    it.live(
      "matches the independent oracle for 25 seeds × 40 units",
      () => Effect.flatMap(createBackend(fixture), (backend) => engineConformance(backend, 25)),
      120_000,
    );

    it.live(
      "matches the oracle with 8-byte prefixes and repairs boundary ties",
      () =>
        Effect.gen(function* () {
          const sql = Context.get(yield* Layer.build(client(fixture)), SqlClient.SqlClient);
          yield* View.migrate(sql);
          let repairs = 0;

          const backend = serverBackend(sql, fixture.server.name === "mysql" ? mysql : postgres, {
            prefixLength: 8,
            onTieRepair: () => {
              repairs++;
            },
          });

          yield* engineConformance(backend, 25);
          expect(repairs).toBeGreaterThan(0);
          yield* Effect.log(`${fixture.name} 8-byte prefix tie repairs: ${repairs}`);
        }),
      120_000,
    );
  });
}
