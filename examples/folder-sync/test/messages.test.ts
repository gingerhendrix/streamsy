import { expect, test } from "vitest";
import { Schema } from "effect";
import {
  afterLastReset,
  applyChanges,
  FileMessage,
  isReset,
  isUpsert,
  type StateMessage,
} from "../server/messages.ts";

const upsert = (key: string, value: number): StateMessage<number> => ({
  type: "n",
  key,
  value,
  headers: { operation: "upsert" },
});

const remove = (key: string): StateMessage<number> => ({
  type: "n",
  key,
  headers: { operation: "delete" },
});

const reset: StateMessage<number> = { headers: { control: "reset" } };

test("applyChanges sets, deletes and clears on reset", () => {
  const rows = applyChanges(new Map([["old", 0]]), [upsert("a", 1), upsert("b", 2), remove("a")]);
  expect([...rows]).toEqual([
    ["old", 0],
    ["b", 2],
  ]);

  expect([...applyChanges(rows, [reset, upsert("c", 3)])]).toEqual([["c", 3]]);
});

test("afterLastReset keeps only the changes after the last reset", () => {
  const messages = [upsert("a", 1), reset, upsert("b", 2), reset, upsert("c", 3)];
  expect(afterLastReset(messages)).toEqual([upsert("c", 3)]);
  expect(afterLastReset([upsert("a", 1)])).toEqual([upsert("a", 1)]);
});

test("the guards tell a reset, an upsert and a delete apart", () => {
  expect([reset, upsert("a", 1), remove("a")].map(isReset)).toEqual([true, false, false]);
  expect([reset, upsert("a", 1), remove("a")].map(isUpsert)).toEqual([false, true, false]);
});

test("FileMessage decodes a file change and a reset, with the offset header", () => {
  const decode = Schema.decodeUnknownSync(Schema.Array(FileMessage));

  const [change, control] = decode([
    {
      type: "file",
      key: "a.md",
      value: { path: "a.md", version: 1, hash: "0".repeat(64), size: 1, author: "t", text: "x" },
      headers: { operation: "upsert", txid: "t", offset: "0000000000000001_0000000000000000" },
    },
    { headers: { control: "reset" } },
  ]);

  expect(change !== undefined && isUpsert(change) && change.headers.offset).toBe(
    "0000000000000001_0000000000000000",
  );
  expect(control !== undefined && isReset(control)).toBe(true);
});
