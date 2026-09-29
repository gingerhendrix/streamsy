import { expect, test } from "bun:test";
import { Schema } from "effect";
import { Rows, StreamAddress } from "@streamsy/core";

const card = {
  schema: Schema.Struct({ id: Schema.String, total: Schema.FiniteFromString }),
  key: "id",
} as const;
const count = {
  schema: Schema.Struct({ id: Schema.Finite, count: Schema.Finite }),
  key: "id",
} as const;
test("single collection operations retain wire names, string keys and the row codec", () => {
  const collections = { card };
  const address = StreamAddress.state("cards", { collections });
  const event = Rows.encode(collections, Rows.upsert({ id: "a", total: 2 }));
  expect(JSON.parse(String(Schema.encodeSync(address.codec)(event)))).toEqual({
    type: "card",
    key: "a",
    value: { id: "a", total: "2" },
    headers: { operation: "upsert" },
  });
  expect(Rows.encode(collections, Rows.remove(3))).toEqual({
    type: "card",
    key: "3",
    headers: { operation: "delete" },
  });
});
test("named operations select the collection and its key", () => {
  const collections = { card, count };
  expect(Rows.encode(collections, Rows.upsert("count", { id: 4, count: 2 }))).toEqual({
    type: "count",
    key: "4",
    value: { id: 4, count: 2 },
    headers: { operation: "upsert" },
  });
  expect(Rows.encode(collections, Rows.remove("card", "a"))).toEqual({
    type: "card",
    key: "a",
    headers: { operation: "delete" },
  });
  // @ts-expect-error multi-collection changes require a name, also checked at runtime
  expect(() => Rows.encode(collections, Rows.remove("a"))).toThrow(RangeError);
  // @ts-expect-error unknown collection names are rejected at both boundaries
  expect(() => Rows.encode(collections, Rows.remove("missing", "a"))).toThrow(RangeError);
});
