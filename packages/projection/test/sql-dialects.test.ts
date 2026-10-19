/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/**
 * Checkpoints on the storage owner (`CheckpointStore.layer`) and fused commits on
 * Postgres and MySQL. Each dialect runs in its own fresh database and skips when
 * its URL is absent.
 */
import { describe, expect, test } from "vitest";
import { Array, Context, Effect, Layer, Option, Result } from "effect";
import { SqlClient } from "effect/sql";
import { Storage, StreamId } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { SqlStorage } from "@streamsy/storage";
import type { CheckpointRecord } from "@streamsy/projection/checkpoint";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
} from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { initialize, input, output, readAll } from "./support/scenarios.ts";
import {
  dialectClient,
  freshDatabase,
  sqlDialects,
  type SqlDialect,
} from "../../storage/test/dialects/support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];

const TIMEOUT_MS = 60_000;

const SAVES = 8;

const key = (id: string) => ({ id, version: 1, generation: 1, params: { tenant: "ä/1" } });

const record = (state: string): CheckpointRecord => ({
  identity: { inputs: {} },
  inputs: {},
  adapters: {},
  state,
});

/** Run `use` on a fresh database of `dialect`. */
const onFreshDatabase = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (url: string) => Effect.Effect<A, E, import("effect").Scope.Scope>,
) =>
  Effect.runPromise(
    freshDatabase(dialect, baseUrl).pipe(
      Effect.flatMap((url) => use(url).pipe(Effect.scoped)),
      Effect.scoped,
    ),
  );

/** Checkpoints on the storage owner, over one pool. */
const checkpoints = (dialect: SqlDialect, url: string) =>
  CheckpointStore.layer.pipe(
    Layer.provideMerge(SqlStorage.layerProtocol()),
    Layer.provideMerge(dialectClient(dialect, url)),
  );

/** Projection checkpoints and transactions joined to storage, as one host process over its own pool. */
const host = (dialect: SqlDialect, url: string) =>
  Layer.build(
    FamilyFixture.withFamilies(
      Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
        Layer.provideMerge(SqlStorage.layerProtocol()),
        Layer.provideMerge(dialectClient(dialect, url)),
      ),
    ),
  );

const count = (sql: SqlClient.SqlClient) =>
  sql<{ readonly n: unknown }>`SELECT COUNT(*) AS n FROM seen`.pipe(
    Effect.map((rows) => Number(rows[0]?.n)),
  );

/** Fails at the checkpoint save, after the handler's SQL and append ran. */
const failingCheckpoints = Effect.map(CheckpointStore, (owner) =>
  CheckpointStore.of({
    ...owner,
    compareAndSet: () =>
      Effect.fail(
        new ProjectionFault({ phase: "checkpoint", reason: "storage-failure", message: "forced" }),
      ),
  }),
);

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} projection SQL`, () => {
    test(
      "the checkpoint store reads, writes, overwrites and removes",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* Layer.build(checkpoints(dialect, fresh));
            // A second build finds the schema migrated and creates nothing.
            yield* Layer.build(checkpoints(dialect, fresh));
            const store = Context.get(context, CheckpointStore);
            const sql = Context.get(context, SqlClient.SqlClient);
            const absent = yield* store.load(key("upper"));
            const first = yield* store.compareAndSet(key("upper"), "0", record("one"));
            const second = yield* store.compareAndSet(key("upper"), first, record("two"));
            // The key column compares bytes, so a key that differs only in case is another row.
            yield* store.compareAndSet(key("UPPER"), "0", record("other"));
            const loaded = yield* store.load(key("upper"));

            const stale = yield* store
              .compareAndSet(key("upper"), first, record("stale"))
              .pipe(Effect.result);

            const rows = yield* sql<{
              readonly owner_key: string;
              readonly owner_value: string;
            }>`SELECT owner_key, owner_value FROM streamsy_owner_records`;

            yield* store.remove(key("upper"));
            // Removing an absent key is ignored.
            yield* store.remove(key("upper"));

            return {
              absent: absent.token,
              tokens: [first, second],
              loaded: [loaded.token, Option.map(loaded.record, ({ state }) => state)],
              stale: Result.isFailure(stale) ? stale.failure.reason : "applied",
              rows: rows.length,
              json: rows.every(({ owner_value }) => owner_value.startsWith("{")),
              removed: (yield* store.load(key("upper"))).token,
              other: Option.map((yield* store.load(key("UPPER"))).record, ({ state }) => state),
            };
          }),
        );

        expect(result).toEqual({
          absent: "0",
          tokens: ["1", "2"],
          loaded: ["2", Option.some("two")],
          stale: "token-conflict",
          rows: 2,
          json: true,
          removed: "0",
          other: Option.some("other"),
        });
      },
      TIMEOUT_MS,
    );

    test(
      "concurrent saves of one key from two pools have one winner",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            // Two separate layer builds, so the saves race across two pools.
            const first = Context.get(
              yield* Layer.build(checkpoints(dialect, fresh)),
              CheckpointStore,
            );

            const second = Context.get(
              yield* Layer.build(checkpoints(dialect, fresh)),
              CheckpointStore,
            );

            const stores = [first, second];

            /** SAVES saves of `id` from `ifToken`, alternating pools, all at once. */
            const race = (id: string, ifToken: string) =>
              Effect.all(
                Array.makeBy(SAVES, (index) =>
                  stores[index % 2]!.compareAndSet(key(id), ifToken, record(`save-${index}`)).pipe(
                    Effect.map((token) => ({ index, outcome: `saved ${token}` })),
                    Effect.catchTag("ProjectionFault", (fault) =>
                      Effect.succeed({ index, outcome: fault.reason }),
                    ),
                  ),
                ),
                { concurrency: "unbounded" },
              ).pipe(
                Effect.flatMap((outcomes) =>
                  Effect.map(first.load(key(id)), (loaded) => {
                    const winners = outcomes.filter(({ outcome }) => outcome.startsWith("saved"));

                    return {
                      outcomes: outcomes.map(({ outcome }) => outcome).toSorted(),
                      // The stored record is the winner's, not a later loser's.
                      stored:
                        winners.length === 1 &&
                        Option.getOrUndefined(loaded.record)?.state === `save-${winners[0]!.index}`,
                      token: loaded.token,
                    };
                  }),
                ),
              );

            yield* first.compareAndSet(key("existing"), "0", record("seed"));

            return { existing: yield* race("existing", "1"), absent: yield* race("absent", "0") };
          }),
        );

        const conflicts = Array.replicate("token-conflict", SAVES - 1);
        expect(result).toEqual({
          existing: { outcomes: ["saved 2", ...conflicts].toSorted(), stored: true, token: "2" },
          absent: { outcomes: ["saved 1", ...conflicts].toSorted(), stored: true, token: "1" },
        });
      },
      TIMEOUT_MS,
    );

    test(
      "checkpoint keys that differ only by a trailing space are different rows",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* Layer.build(checkpoints(dialect, fresh));
            const sql = Context.get(context, SqlClient.SqlClient);

            // Encoded keys end in "]", so the column is checked directly.
            for (const owner_key of ["x", "x "])
              yield* sql`INSERT INTO streamsy_owner_records ${sql.insert({
                owner_key,
                owner_value: "{}",
              })}`;

            const rows = yield* sql<{
              readonly owner_key: string;
            }>`SELECT owner_key FROM streamsy_owner_records WHERE owner_key = ${"x"}`;

            return rows.map(({ owner_key }) => owner_key);
          }),
        );

        expect(result).toEqual(["x"]);
      },
      TIMEOUT_MS,
    );

    test(
      "a fused commit writes the app row, the output append and the checkpoint together",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* host(dialect, fresh);

            return yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`CREATE TABLE seen (item INTEGER NOT NULL)`;
              yield* initialize;

              const projection = Projection.process({
                commit: Commit.transaction,
                id: "fused",
                params: {},
                inputs: { input },
                process: (batch) =>
                  Effect.gen(function* () {
                    for (const item of batch.input.items)
                      yield* sql`INSERT INTO seen ${sql.insert({ item })}`;
                    const kept = batch.input.items.filter((item) => item > 0);

                    if (Array.isReadonlyArrayNonEmpty(kept)) yield* Streams.append(output, kept);
                  }),
              }).at({});

              const owner = yield* CheckpointStore;

              const failed = yield* Projection.run(projection).pipe(
                Effect.provideService(CheckpointStore, yield* failingCheckpoints),
                Effect.result,
              );

              const afterFailure = {
                failed: Result.isFailure(failed),
                seen: yield* count(sql),
                output: yield* readAll(output),
                token: (yield* owner.load(Projection.key(projection))).token,
              };

              const run = yield* Projection.run(projection);

              return {
                afterFailure,
                status: run.status,
                seen: yield* count(sql),
                output: yield* readAll(output),
                token: (yield* owner.load(Projection.key(projection))).token,
              };
            }).pipe(Effect.provide(context));
          }),
        );

        expect(result).toEqual({
          afterFailure: { failed: true, seen: 0, output: [], token: "0" },
          status: "caught-up",
          seen: 3,
          output: [1, 3],
          token: "1",
        });
      },
      TIMEOUT_MS,
    );

    test(
      "a fused commit appends to its output after another pool appended to it",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            // Build one host first, so the second finds the schema migrated.
            const context = yield* host(dialect, fresh);
            const other = yield* host(dialect, fresh);
            const otherWriter = Context.get(other, StreamsWriter);

            return yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`CREATE TABLE seen (item INTEGER NOT NULL)`;
              yield* initialize;
              let interleaved = false;

              const projection = Projection.process({
                commit: Commit.transaction,
                id: "fused-race",
                params: {},
                inputs: { input },
                process: (batch) =>
                  Effect.gen(function* () {
                    // On MySQL this plain read fixes the transaction's snapshot.
                    yield* count(sql);

                    if (!interleaved) {
                      interleaved = true;
                      // Another process appends to the output after that read.
                      yield* FamilyFixture.provideWriter(otherWriter)(
                        Streams.append(output, [100]),
                      );
                    }

                    for (const item of batch.input.items)
                      yield* sql`INSERT INTO seen ${sql.insert({ item })}`;
                    const kept = batch.input.items.filter((item) => item > 0);

                    if (Array.isReadonlyArrayNonEmpty(kept)) yield* Streams.append(output, kept);
                  }),
              }).at({});

              const run = Result.match(yield* Projection.run(projection).pipe(Effect.result), {
                onSuccess: ({ status }) => status,
                onFailure: (error) => `failed:${String(error)}`,
              });

              const storage = yield* Storage;
              const id = StreamId.make(output.id);
              const messages = yield* storage.messages(id, {});
              const offsets = messages.map(({ offset }) => offset);
              const tail = Option.map(yield* storage.record(id), (stored) => stored.currentOffset);

              return {
                run,
                seen: yield* count(sql),
                output: yield* readAll(output),
                ordered: offsets.every(
                  (offset, index) => index === 0 || offsets[index - 1]! < offset,
                ),
                tail: Option.getOrUndefined(tail) === offsets.at(-1),
              };
            }).pipe(Effect.provide(context));
          }),
        );

        expect(result).toEqual({
          run: "caught-up",
          seen: 3,
          output: [100, 1, 3],
          ordered: true,
          tail: true,
        });
      },
      TIMEOUT_MS,
    );
  });
}
