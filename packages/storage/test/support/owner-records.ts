import { expect } from "vitest";
import { Array, Effect, Schema } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { OwnerRecords } from "@streamsy/core/internal";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const ids = ["a", "ab", "A", "a ", 'a"b', "a\\", "a%_", "\u00e9", "\uffff", "𐀀", "ends\u{10ffff}"];

const lengths = [1, 30, 40, 446, 447, 448, 511, 512, 513];

const entries = [
  ...Array.flatMap(ids, (id) =>
    Array.flatMap(["checkpoint", "route"], (kind) =>
      Array.map(
        [id, "𐀀".repeat(550) + "a", "𐀀".repeat(550) + "b"],
        (value) => [json([kind, id, 1, 1, { value }]), value] as const,
      ),
    ),
  ),
  ["\u00e9", "composed"],
  ["e\u0301", "decomposed"],
  ["raw-\uffff-tail", "last BMP code point"],
  ["raw-\u{10000}-tail", "first astral code point"],
  ["raw-space", "no trailing space"],
  ["raw-space ", "one trailing space"],
  ["range\ud7fflast", "before surrogates"],
  ["range\ue000", "after surrogates"],
  ["range\u{10ffff}\u{10ffff}tail", "carry across trailing largest scalars"],
  ["\u{10ffff}\u{10ffff}", "no upper bound"],
  ["x".repeat(447) + "#" + "a".repeat(64), "literal digest shape"],
  ["", "empty key"],
] as const;

const prefixes = new Set([
  "raw-\uffff",
  "raw-space",
  "raw-space ",
  "",
  "absent",
  "\u00c9",
  "range\ud7ff",
  "range\u{10ffff}\u{10ffff}",
  "\u{10ffff}",
  "\u{10ffff}\u{10ffff}",
  ...Array.flatMap(ids, (id) =>
    Array.map(["checkpoint", "route"], (kind) => json([kind, id]).slice(0, -1) + ","),
  ),
  ...Array.flatMap(entries, ([key]) => {
    const points = Array.fromIterable(key);

    return Array.map(lengths, (length) => Array.join(Array.take(points, length), ""));
  }),
]);

export const checkOwnerRecordPrefixes = Effect.gen(function* () {
  const owner = yield* OwnerRecords;
  // Protect the fixture against editor normalization before testing the owner.
  expect(Array.fromIterable("\u00e9")).toHaveLength(1);
  expect(Array.fromIterable("e\u0301")).toHaveLength(2);

  for (const [key, value] of entries) yield* owner.write(key, value);

  for (const prefix of prefixes) {
    const expected = new Map(Array.filter(entries, ([key]) => key.startsWith(prefix)));
    const found = yield* owner.list(prefix);
    expect(found.length, json(prefix)).toBe(expected.size);
    expect(new Map(found), json(prefix)).toEqual(expected);
  }
});

export const rejectsMalformedPrefixes = Effect.gen(function* () {
  const owner = yield* OwnerRecords;

  for (const prefix of ["a\ud800", "a\udfff"]) {
    expect(yield* owner.list(prefix).pipe(Effect.flip)).toMatchObject({
      operation: "records.list",
      retryable: false,
      message: expect.stringContaining("well-formed UTF-16"),
    });
  }
});

/** Explain the statement actually used by OwnerRecords.list. */
export const explainList = Effect.fnUntraced(function* (prefix: string) {
  const records = yield* OwnerRecords;
  const sql = yield* SqlClient.SqlClient;

  const explain = sql.onDialectOrElse({
    sqlite: () => "EXPLAIN QUERY PLAN",
    orElse: () => "EXPLAIN",
  });

  const statements: Array<readonly [string, ReadonlyArray<unknown>]> = [];
  yield* records.list(prefix).pipe(
    Effect.provideService(Statement.CurrentTransformer, (statement) => {
      statements.push(statement.compile());

      return Effect.succeed(statement);
    }),
  );
  expect(statements).toHaveLength(1);
  const [query, params] = statements[0] ?? ["", []];
  expect(query).not.toMatch(/ORDER BY/iu);

  return yield* sql.unsafe(`${explain} ${query}`, params);
});
