/**
 * Durable-file validation at the adapter seam: a corrupt `record.json` or
 * `producers.json` surfaces as a failure, distinct from the absence that
 * legitimately reads as "no stream" / "no producer". Silently reinterpreting
 * corruption as absence would let a create overwrite a damaged stream or let a
 * duplicate producer append through.
 */
import { mkdtempSync, appendFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "bun:test";
import type { StreamRecord } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";

const ZERO = `${"0".repeat(16)}_${"0".repeat(16)}`;
const enc = (s: string): Uint8Array => new TextEncoder().encode(s);
const dec = (u: Uint8Array): string => new TextDecoder().decode(u);

function freshRoot(): string {
  return mkdtempSync(path.join(tmpdir(), "streamsy-fs-persisted-"));
}

function newRecord(id: string): StreamRecord {
  return {
    id,
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle: {},
    currentOffset: ZERO,
    counter: 0,
  };
}

describe("persisted record validation", () => {
  it("reads a missing stream as absent, not as an error", async () => {
    const adapter = createFsStorageAdapter({ root: freshRoot() });
    expect(await adapter.getRecord("never-created")).toBeNull();
    expect(await adapter.listMessages("never-created")).toEqual([]);
    expect(await adapter.getProducerState("never-created", "p1")).toBeUndefined();
  });

  it("surfaces a record.json that is not valid JSON", async () => {
    const root = freshRoot();
    const adapter = createFsStorageAdapter({ root });
    await adapter.create({ record: newRecord("s") });

    writeFileSync(path.join(root, "s", "record.json"), "{ truncated");
    expect(() => adapter.getRecord("s")).toThrow(SyntaxError);
  });

  it("surfaces a record.json whose JSON is not a stream record", async () => {
    const root = freshRoot();
    const adapter = createFsStorageAdapter({ root });
    await adapter.create({ record: newRecord("s") });
    const recordPath = path.join(root, "s", "record.json");

    // Well-formed JSON that would have passed straight through an assertion.
    for (const contents of [
      "null",
      "[]",
      '"a string"',
      JSON.stringify({ ...newRecord("s"), currentOffset: 7 }),
      JSON.stringify({ ...newRecord("s"), counter: "0" }),
      JSON.stringify({ ...newRecord("s"), config: null }),
      JSON.stringify({ ...newRecord("s"), config: { createdAt: 0 } }),
      JSON.stringify({ ...newRecord("s"), lifecycle: undefined }),
    ]) {
      writeFileSync(recordPath, contents);
      expect(() => adapter.getRecord("s")).toThrow(/corrupt record\.json/);
    }
  });

  it("surfaces a producers.json whose JSON is not a producer map", async () => {
    const root = freshRoot();
    const adapter = createFsStorageAdapter({ root });
    await adapter.create({ record: newRecord("s") });
    const producersPath = path.join(root, "s", "producers.json");

    for (const contents of [
      "[]",
      '{"p1":null}',
      '{"p1":{"epoch":1}}',
      '{"p1":{"epoch":"1","lastSeq":0}}',
    ]) {
      writeFileSync(producersPath, contents);
      expect(() => adapter.getProducerState("s", "p1")).toThrow(/corrupt producers\.json/);
    }

    // A valid map still reads back normally.
    writeFileSync(producersPath, '{"p1":{"epoch":1,"lastSeq":3}}');
    expect(await adapter.getProducerState("s", "p1")).toEqual({ epoch: 1, lastSeq: 3 });
  });

  it("skips messages.jsonl lines that are valid JSON but not envelopes", async () => {
    const root = freshRoot();
    const adapter = createFsStorageAdapter({ root });
    await adapter.create({
      record: newRecord("s"),
      initialMessages: [{ data: enc("kept"), offset: ZERO, timestamp: 0 }],
    });

    // `null` and a bare number are objects/primitives an assertion would have
    // indexed as an envelope; a truncated line is not JSON at all.
    appendFileSync(path.join(root, "s", "messages.jsonl"), 'null\n123\n[]\n{"offset":\n');

    const messages = await adapter.listMessages("s");
    expect(messages.map((message) => dec(message.data))).toEqual(["kept"]);
  });
});
