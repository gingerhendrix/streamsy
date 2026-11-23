/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
import { describe, expect, test } from "vitest";
import { Array, Context, Effect, Layer, Option, type Scope } from "effect";
import { Offset, Storage, StreamId } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { SqlClient } from "effect/sql";
import { migrate } from "../../src/migrations.ts";
import {
  STORAGE_MIGRATIONS,
  STORAGE_VERSION_TABLE,
  STORAGE_VERSION_TABLE_DDL,
} from "../../src/schema.ts";
import { storageLayer } from "../../src/storage.ts";
import { dialectClient, freshDatabase, sqlDialects, type SqlDialect } from "./support.ts";

/** Run `use` against a new database, dropped afterwards. */
const withFreshDatabase = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (url: string) => Effect.Effect<A, E, Scope.Scope>,
) =>
  freshDatabase(dialect, baseUrl).pipe(
    Effect.flatMap((url) => Effect.scoped(use(url))),
    Effect.scoped,
  );

const bytes = (text: string) => new TextEncoder().encode(text);

for (const dialect of [sqlDialects.pg, sqlDialects.mysql]) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} storage schema`, () => {
    test("migrates a fresh database twice to one version row and the storage tables", async () => {
      if (url === undefined) return;

      const result = await Effect.runPromise(
        withFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const sql = Context.get(
              yield* Layer.build(dialectClient(dialect, fresh)),
              SqlClient.SqlClient,
            );

            yield* migrate(sql);
            yield* migrate(sql);

            const versions = yield* sql.unsafe<{ readonly version: unknown }>(
              `SELECT version FROM ${STORAGE_VERSION_TABLE} ORDER BY version`,
            );

            const tables = yield* sql.unsafe<{ readonly name: string }>(dialect.tables);

            return {
              versions: Array.map(versions, ({ version }) => Number(version)),
              tables: Array.map(tables, ({ name }) => name),
            };
          }),
        ),
      );

      expect(result).toEqual({
        versions: dialect.name === "mysql" ? [1, 2, 3, 4] : [1, 2, 3],
        tables: [
          "streamsy_messages",
          "streamsy_owner_records",
          "streamsy_producers",
          "streamsy_projection_pending",
          "streamsy_schema",
          "streamsy_streams",
        ],
      });
    });

    test("opening v2 preserves marks and recovers every partially applied MySQL v3 prefix", async () => {
      if (url === undefined) return;

      // MySQL may have committed any subset of the v3 additions before a crash.
      for (const prefix of dialect.name === "mysql" ? [0, 1, 2, 3, 4, 5] : [0]) {
        const result = await Effect.runPromise(
          withFreshDatabase(dialect, url, (fresh) =>
            Effect.gen(function* () {
              const sql = Context.get(
                yield* Layer.build(dialectClient(dialect, fresh)),
                SqlClient.SqlClient,
              );

              yield* sql.unsafe(STORAGE_VERSION_TABLE_DDL[dialect.name]);

              for (const migration of STORAGE_MIGRATIONS.slice(0, 2))
                for (const statement of migration[dialect.name]) yield* sql.unsafe(statement);
              yield* sql`INSERT INTO ${sql(STORAGE_VERSION_TABLE)} (version, applied_at_ms) VALUES (1, 1), (2, 2)`;
              yield* sql`INSERT INTO streamsy_projection_pending (projection, params, seq, attempts, due_at) VALUES ('kept', 'a', 7, 3, 1234567890123)`;

              for (const statement of STORAGE_MIGRATIONS[2]![dialect.name].slice(0, prefix))
                yield* sql.unsafe(statement);

              const opened = yield* Layer.build(
                storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
              );

              const marks = Context.get(opened, ProjectionMarks);
              const row = (yield* marks.read("kept", ["a"])).get("a");
              yield* migrate(Context.get(opened, SqlClient.SqlClient));

              const versions = yield* sql<{
                readonly version: unknown;
              }>`SELECT version FROM ${sql(STORAGE_VERSION_TABLE)} ORDER BY version`;

              return { row, versions: Array.map(versions, ({ version }) => Number(version)) };
            }),
          ),
        );

        expect(result).toEqual({
          row: {
            params: "a",
            seq: 7,
            attempts: 3,
            dueAt: 1234567890123,
            wakeAt: Option.none(),
            stopped: false,
            failure: Option.none(),
          },
          versions: dialect.name === "mysql" ? [1, 2, 3, 4] : [1, 2, 3],
        });
      }
      // MySQL builds six fresh databases here. On a CI runner that takes about 5 s.
    }, 30_000);

    test("creates, appends and reads through the storage layer", async () => {
      if (url === undefined) return;
      const id = StreamId.make("dialect/stream");
      const first = Offset.make("0000000000000001_0000000000000001");
      const second = Offset.make("0000000000000002_0000000000000001");

      const result = await Effect.runPromise(
        withFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* Layer.build(
              storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
            );

            const storage = Context.get(context, Storage);
            yield* storage.mutate({
              operations: [
                {
                  _tag: "Create",
                  record: {
                    id,
                    config: { contentType: "text/plain", createdAt: 1 },
                    lifecycle: { closed: false, softDeleted: false },
                    currentOffset: first,
                  },
                  initialMessages: [{ offset: first, timestamp: 1, data: bytes("one") }],
                },
              ],
            });
            const created = Option.getOrThrow(yield* storage.record(id));
            yield* storage.mutate({
              operations: [
                {
                  _tag: "Append",
                  streamId: id,
                  expectedOffset: first,
                  messages: [{ offset: second, timestamp: 2, data: bytes("two") }],
                  patch: { currentOffset: second },
                },
              ],
            });
            const all = yield* storage.messages(id, {});
            const after = yield* storage.messages(id, { after: first, limit: 1 });
            const record = Option.getOrThrow(yield* storage.record(id));

            return {
              created: created.currentOffset,
              current: record.currentOffset,
              all: Array.map(all, ({ offset, data }) => [offset, new TextDecoder().decode(data)]),
              after: Array.map(after, ({ offset }) => offset),
            };
          }),
        ),
      );

      expect(result).toEqual({
        created: first,
        current: second,
        all: [
          [first, "one"],
          [second, "two"],
        ],
        after: [second],
      });
    });
  });
}
