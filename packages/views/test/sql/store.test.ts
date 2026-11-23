import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Context, Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { View } from "../../src/index.ts";
import { encodeRowKey } from "../../src/runtime/key.ts";
import { backendFor } from "../../src/sql/backend.ts";
import { storeContract } from "../support/store-contract.ts";

const sqlite = SqliteClient.layer({ filename: ":memory:" });

storeContract("SQLite Store", () =>
  Effect.gen(function* () {
    const sql = Context.get(yield* Layer.build(sqlite), SqlClient.SqlClient);
    yield* View.migrate(sql);

    return yield* backendFor(sql);
  }),
);

it.live("migrates the SQLite tables idempotently", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* View.migrate(sql);
    const backend = yield* backendFor(sql);
    const store = backend.store("a");
    const entry = { key: "row", row: { id: "row" } };

    yield* store.put("r", [entry]);
    yield* View.migrate(sql);
    expect(yield* store.get("r", encodeRowKey(entry.key))).toEqual(entry);
  }).pipe(Effect.provide(sqlite)),
);
