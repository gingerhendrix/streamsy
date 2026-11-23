import { SqlStorage } from "@streamsy/storage";
import { Effect, Layer } from "effect";
import { test } from "vitest";
import { checkForeignTransaction } from "../support/foreign-sql-owner.ts";
import { storageDialectSuite } from "./storage-suite.ts";
import { dialectClient, freshDatabase, sqlDialects } from "./support.ts";

storageDialectSuite(sqlDialects.pg);

const pgUrl = sqlDialects.pg.url;

const host = (url: string) =>
  Layer.build(
    SqlStorage.layerProtocol().pipe(Layer.provideMerge(dialectClient(sqlDialects.pg, url))),
  );

test.skipIf(pgUrl === undefined)(
  "rejects a nested transaction on a separate Postgres database",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        if (pgUrl === undefined) return;

        const leftUrl = yield* freshDatabase(sqlDialects.pg, pgUrl);
        const rightUrl = yield* freshDatabase(sqlDialects.pg, pgUrl);

        yield* checkForeignTransaction(yield* host(leftUrl), yield* host(rightUrl));
      }).pipe(Effect.scoped),
    ),
);
