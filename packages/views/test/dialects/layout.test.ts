/* oxlint-disable effecttsgo/strict-effect-provide -- The two layout fixtures run in isolated database scopes. */
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { layoutGroup, layoutNamespace, runLayoutUnits } from "../circuit/support/layout.ts";
import { hostFor, postgres, serverFixtures, sqlite } from "../support/backends.ts";
import { sqlDump } from "../support/layout-dump.ts";
import { registerRollback } from "../support/rollback.ts";

const fixedDump = runLayoutUnits.pipe(Effect.andThen(sqlDump(layoutNamespace, true)));

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} layout`, () => {
    it.live(
      "matches SQLite's logical dump of the pinned units and checks every arrangement prefix and hash",
      () =>
        Effect.gen(function* () {
          const sqliteDump = yield* fixedDump.pipe(Effect.provide(hostFor(sqlite, layoutGroup)));
          const serverDump = yield* fixedDump.pipe(Effect.provide(hostFor(fixture, layoutGroup)));
          expect(serverDump.arrangements).not.toHaveLength(0);
          expect(serverDump).toEqual(sqliteDump);
        }),
    );

    if (fixture === postgres) registerRollback(fixture);
  });
}
