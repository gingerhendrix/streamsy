/* oxlint-disable effecttsgo/async-function -- vitest owns the host hooks. */
/** The official server suite exercises the generic SQL storage on MySQL through a local Bun host. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { MysqlClient } from "@effect/sql-mysql2";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect, Exit, Layer, Redacted, Scope } from "effect";
import { SqlStorage } from "@streamsy/storage";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";
import { freshDatabase, sqlDialects } from "../../storage/test/dialects/support.ts";

const url = sqlDialects.mysql.url;

let database: Scope.Closeable | undefined;

let host: TestHost | undefined;

describe.skipIf(url === undefined)("Effect SQL Bun host on MySQL", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    if (url === undefined) return;
    database = await Effect.runPromise(Scope.make());

    const fresh = await Effect.runPromise(
      freshDatabase(sqlDialects.mysql, url).pipe(Scope.provide(database)),
    );

    host = await Effect.runPromise(
      testHost({
        layer: SqlStorage.layerProtocol({ longPollTimeoutMs: 1_500 }).pipe(
          Layer.provideMerge(
            SqlStorage.clientLayer(MysqlClient.make({ url: Redacted.make(fresh) })),
          ),
        ),
        port: 0,
      }),
    );
    config.baseUrl = new URL(host.url).origin;
  });
  afterAll(async () => {
    if (host !== undefined) await Effect.runPromise(host.stop);

    if (database !== undefined) await Effect.runPromise(Scope.close(database, Exit.void));
  });
  runConformanceTests(config);
});
