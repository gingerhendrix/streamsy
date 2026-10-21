import { expect, test } from "vitest";
import { StreamAddress } from "@streamsy/core";
import { Array, Schema } from "effect";
import { applyFileMessage } from "../server/folder-view.ts";
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

const reset: StateMessage<number> = StreamAddress.reset();

const snapshotStart: StateMessage<number> = StreamAddress.snapshotStart();

const snapshotEnd: StateMessage<number> = StreamAddress.snapshotEnd();

test("applyChanges sets, deletes and clears on reset", () => {
  const rows = applyChanges(new Map([["old", 0]]), [upsert("a", 1), upsert("b", 2), remove("a")]);
  expect([...rows]).toEqual([
    ["old", 0],
    ["b", 2],
  ]);

  expect([...applyChanges(rows, [reset, upsert("c", 3)])]).toEqual([["c", 3]]);
});

test("applyChanges skips snapshot markers and keeps the rows", () => {
  const rows = applyChanges(new Map([["old", 0]]), [snapshotStart, upsert("a", 1), snapshotEnd]);
  expect([...rows]).toEqual([
    ["old", 0],
    ["a", 1],
  ]);
});

test("the folder view skips snapshot markers and clears its files at a reset", () => {
  const live = { txid: "t", version: 3, hash: "0".repeat(64), size: 1, author: "t" };
  const view = { last: 3, files: new Map([["a.md", live]]) };
  expect(applyFileMessage(view, StreamAddress.snapshotStart())).toBe(view);
  expect(applyFileMessage(view, StreamAddress.snapshotEnd())).toBe(view);
  expect(applyFileMessage(view, StreamAddress.reset())).toEqual({ last: 3, files: new Map() });
});

test("afterLastReset keeps only the changes after the last reset", () => {
  const messages = [upsert("a", 1), reset, upsert("b", 2), reset, upsert("c", 3)];
  expect(afterLastReset(messages)).toEqual([upsert("c", 3)]);
  expect(afterLastReset([upsert("a", 1)])).toEqual([upsert("a", 1)]);
});

test("the guards tell a reset, an upsert and a delete apart", () => {
  const messages = [reset, snapshotStart, upsert("a", 1), remove("a")];
  expect(Array.map(messages, isReset)).toEqual([true, false, false, false]);
  expect(Array.map(messages, isUpsert)).toEqual([false, false, true, false]);
});

test("FileMessage decodes a file change and each State control, with the offset header", () => {
  const decode = Schema.decodeUnknownSync(Schema.Array(FileMessage));

  const [change, control, start, end] = decode([
    {
      type: "file",
      key: "a.md",
      value: { path: "a.md", version: 1, hash: "0".repeat(64), size: 1, author: "t", text: "x" },
      headers: { operation: "upsert", txid: "t", offset: "0000000000000001_0000000000000000" },
    },
    { headers: { control: "reset" } },
    { headers: { control: "snapshot-start" } },
    { headers: { control: "snapshot-end" } },
  ]);

  expect(change !== undefined && isUpsert(change) && change.headers.offset).toBe(
    "0000000000000001_0000000000000000",
  );
  expect(control !== undefined && isReset(control)).toBe(true);
  expect([start, end]).toEqual([StreamAddress.snapshotStart(), StreamAddress.snapshotEnd()]);
});
