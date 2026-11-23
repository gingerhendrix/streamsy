/* oxlint-disable effecttsgo/strict-effect-provide -- The two layout fixtures run in isolated database scopes. */
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { layoutGroup, layoutNamespace, runLayoutUnits } from "../circuit/support/layout.ts";
import { hostFor, postgres, sqlite } from "../support/backends.ts";
import { dump } from "../support/layout-dump.ts";

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
