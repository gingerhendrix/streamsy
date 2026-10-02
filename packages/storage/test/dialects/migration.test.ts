/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
import { describe, expect, test } from "bun:test";
import { Context, Effect, Layer, Option, type Scope } from "effect";
import { Offset, Storage, StreamId } from "@streamsy/core";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { migrate } from "../../src/migrations.ts";
import { STORAGE_SCHEMA_VERSION, STORAGE_VERSION_TABLE } from "../../src/schema.ts";
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
              `SELECT version FROM ${STORAGE_VERSION_TABLE}`,
            );
            const tables = yield* sql.unsafe<{ readonly name: string }>(dialect.tables);
            return {
              versions: versions.map(({ version }) => Number(version)),
              tables: tables.map(({ name }) => name),
            };
          }),
        ),
      );
      expect(result).toEqual({
        versions: [STORAGE_SCHEMA_VERSION],
        tables: [
          "streamsy_messages",
          "streamsy_owner_records",
          "streamsy_producers",
          "streamsy_schema",
          "streamsy_streams",
        ],
      });
    });

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
              all: all.map(({ offset, data }) => [offset, new TextDecoder().decode(data)]),
              after: after.map(({ offset }) => offset),
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
