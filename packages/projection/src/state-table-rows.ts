import { Array, Effect, Schema } from "effect";
import type { SqlClient } from "effect/sql";

/** Decode one stored value, which is canonical JSON, with its collection schema. */
export const decodeRow = <A, I, RD, RE>(schema: Schema.Codec<A, I, RD, RE>, json: string) =>
  Schema.decodeEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(json);

/** Encode one row to the canonical JSON that the table stores for it. */
export const encodeRow = <A, I, RD, RE>(schema: Schema.Codec<A, I, RD, RE>, value: A) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(value);

/** The stored JSON of each key that has a row. A missing key has no entry. */
export const storedValues = Effect.fn("StateTable.storedValues")(function* (
  sql: SqlClient.SqlClient,
  id: string,
  collection: string,
  keys: ReadonlyArray<string>,
) {
  const values = new Map<string, string>();

  // A 50-key chunk binds 52 parameters, below Durable Objects' limit of 100 per query.
  for (const chunk of Array.chunksOf(keys, 50)) {
    const found = yield* sql<{
      readonly row_key: string;
      readonly value: string;
    }>`SELECT row_key, value FROM streamsy_state_rows WHERE stream_id = ${id} AND collection = ${collection} AND ${sql.in("row_key", chunk)}`;

    for (const row of found) values.set(row.row_key, row.value);
  }

  return values;
});
