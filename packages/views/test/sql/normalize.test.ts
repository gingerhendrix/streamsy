import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { NonRetryable } from "@streamsy/projection";
import { Effect, Record } from "effect";
import { SqlClient } from "effect/sql";
import { encodeRowKey } from "../../src/runtime/key.ts";
import { dataFault } from "../../src/sql/data-fault.ts";
import { makeEngine, migrate } from "../../src/sql/engine.ts";
import { normalize } from "../../src/sql/normalize.ts";
import { storeFor, type Store } from "../../src/sql/store.ts";
import { plan, source } from "./fixtures.ts";

const fault = dataFault({ view: "rows", node: "r0" });

const normalizeUpsert = (store: Store, value: { readonly id: string }) =>
  normalize(
    store,
    "rows",
    "r0",
    "row",
    fault,
    [{ type: "row", key: "one", headers: { operation: "upsert" }, value }],
    false,
  );

const sqlite = SqliteClient.layer({ filename: ":memory:" });

it.live("omits undefined object members before storing or comparing State upserts", () =>
  Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine({ rows: plan("rows", [source("rows")]) });
    const store = storeFor(yield* SqlClient.SqlClient, "test");

    const value = {
      id: "one",
      absent: undefined,
      nested: { absent: undefined, kept: null },
      items: [{ absent: undefined, kept: true }],
    };

    const expected = { id: "one", nested: { kept: null }, items: [{ kept: true }] };

    const upsert = yield* normalizeUpsert(store, value);

    expect(upsert).toStrictEqual([
      { kind: "upsert", sourceId: "rows", key: "one", value: expected },
    ]);
    expect(yield* engine.process("test", upsert)).toStrictEqual([
      { kind: "upsert", view: "rows", key: "one", value: expected },
    ]);
    expect(yield* store.get("r0", encodeRowKey("one"))).toStrictEqual({
      key: "one",
      row: expected,
    });

    const equivalent = yield* normalizeUpsert(store, expected);
    expect(yield* engine.process("test", equivalent)).toStrictEqual([]);
  }).pipe(Effect.provide(sqlite)),
);

it.live("keeps invalid State values as non-retryable data faults", () =>
  Effect.gen(function* () {
    const store = storeFor(yield* SqlClient.SqlClient, "test");

    class Instance {
      readonly absent = undefined;
    }

    const cycle = { self: {} };
    cycle.self = cycle;

    for (const [label, invalid] of Record.toEntries({
      "undefined array item": [undefined],
      function: () => undefined,
      NaN,
      Infinity,
      bigint: 1n,
      instance: new Instance(),
      cycle,
    })) {
      const value = { id: "one", absent: undefined, invalid };
      const result = yield* normalizeUpsert(store, value).pipe(Effect.result);

      expect(result, label).toMatchObject({
        _tag: "Failure",
        failure: {
          _tag: "ViewDataFault",
          key: "one",
          reason: 'Expected JSON value\n  at ["invalid"]',
          [NonRetryable]: true,
        },
      });
    }
  }).pipe(Effect.provide(sqlite)),
);
