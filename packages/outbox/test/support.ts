/* oxlint-disable effecttsgo/strict-effect-provide -- Test entry points own complete SQL, store and TestClock layers and their scopes. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Effect, Layer, type Scope } from "effect";
import { SqlClient } from "effect/sql";
import { TestClock } from "effect/testing";
import { Outbox, OutboxStore, type OutboxBacking, type OutboxDraft } from "../src/index.ts";
import { layerMemory } from "../src/memory.ts";
import { layerSql, migrateOutbox } from "../src/sql.ts";
import { Schema } from "effect";

export const payload = Schema.Struct({ id: Schema.String, lane: Schema.String });

export const retry = { maxAttempts: 3, initialDelayMs: 100, factor: 2, maxDelayMs: 1_000 };

export const definition = Outbox.define({
  name: "test.notifications",
  payload,
  idempotencyKey: (value) => value.id,
  partitionBy: (value) => value.lane,
  retry,
});

export const note = (id: string, lane = "main") => ({ id, lane });

export const draft = (id: string, partitionId = "main", enqueuedAtMs = 0): OutboxDraft => ({
  name: definition.name,
  idempotencyKey: id,
  partitionId,
  encodedPayload: Schema.encodeSync(Schema.fromJsonString(payload))(note(id, partitionId)),
  enqueuedAtMs,
});

/** Run `use` with the provided store, which it also receives for direct protocol calls. */
const withProvidedStore = <A, E, R>(
  use: (store: OutboxBacking) => Effect.Effect<A, E, R | OutboxStore>,
) =>
  Effect.gen(function* () {
    return yield* use(yield* OutboxStore);
  });

export const withSqlite = <A, E>(
  use: (
    sql: SqlClient.SqlClient,
    store: OutboxBacking,
  ) => Effect.Effect<A, E, OutboxStore | Scope.Scope>,
  filename = ":memory:",
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrateOutbox(sql);

      return yield* withProvidedStore((store) => use(sql, store)).pipe(Effect.provide(layerSql));
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(SqliteClient.layer({ filename, create: true }), TestClock.layer()),
      ),
    ),
  );

export const withStore = <A, E>(
  mode: "memory" | "sqlite",
  use: (store: OutboxBacking) => Effect.Effect<A, E, OutboxStore | Scope.Scope>,
): Promise<A> => {
  if (mode === "sqlite") return withSqlite((_, store) => use(store));

  return Effect.runPromise(
    withProvidedStore(use).pipe(
      Effect.provide(layerMemory),
      Effect.scoped,
      Effect.provide(TestClock.layer()),
    ),
  );
};
