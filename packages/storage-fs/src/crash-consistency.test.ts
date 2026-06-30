/**
 * Crash consistency: `record.currentOffset` is the authoritative visible tail.
 * A trailing message line beyond it (e.g. flushed just before a crash, before the
 * record advanced) is invisible to `listMessages`, and a later legitimate advance
 * that covers that offset supersedes the orphan line (last-write-wins per offset).
 */
import { appendFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "bun:test";
import type { StreamRecord } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";

const ZERO = `${"0".repeat(16)}_${"0".repeat(16)}`;
const offset = (counter: number): string =>
  `${String(counter).padStart(16, "0")}_${"0".repeat(16)}`;
const enc = (s: string): Uint8Array => new TextEncoder().encode(s);
const dec = (u: Uint8Array): string => new TextDecoder().decode(u);

function freshRoot(): string {
  return mkdtempSync(path.join(tmpdir(), "streamsy-fs-crash-"));
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

async function appendOne(
  adapter: ReturnType<typeof createFsStorageAdapter>,
  id: string,
  text: string,
) {
  const record = (await adapter.getRecord(id))!;
  const next = record.counter + 1;
  return adapter.append(id, {
    preconditions: { expectedOffset: record.currentOffset, expectedClosed: false },
    messages: [{ data: enc(text), offset: offset(next), timestamp: 0 }],
    recordPatch: { currentOffset: offset(next), counter: next },
  });
}

describe("crash consistency", () => {
  it("ignores message lines beyond the authoritative currentOffset", async () => {
    const root = freshRoot();
    const adapter = createFsStorageAdapter({ root });
    await adapter.create({ record: newRecord("s") });
    await appendOne(adapter, "s", "first");

    expect((await adapter.listMessages("s")).length).toBe(1);

    // Simulate a crash after the message flush but before the record advanced:
    // an orphan line at offset 2 while record.currentOffset is still offset 1.
    const messagesPath = path.join(root, "s", "messages.jsonl");
    appendFileSync(
      messagesPath,
      JSON.stringify({
        offset: offset(2),
        timestamp: 0,
        b64: Buffer.from(enc("orphan")).toString("base64"),
      }) + "\n",
    );

    const afterOrphan = await adapter.listMessages("s");
    expect(afterOrphan.length).toBe(1);
    expect(dec(afterOrphan[0]!.data)).toBe("first");

    // A legitimate advance to offset 2 makes the (new) message visible and
    // supersedes the orphan line for that offset.
    const advanced = await appendOne(adapter, "s", "second");
    expect(advanced.status).toBe("appended");
    const visible = await adapter.listMessages("s");
    expect(visible.map((m) => dec(m.data))).toEqual(["first", "second"]);
  });
});
