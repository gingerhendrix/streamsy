/**
 * Cross-process multi-writer CAS contention.
 *
 * Spawns several independent `bun` processes that all race to append the same
 * first message. The on-disk lock + freshly-read precondition check must let
 * exactly one win, with no torn or duplicated visible message.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "bun:test";
import type { StreamRecord } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";

const ZERO = `${"0".repeat(16)}_${"0".repeat(16)}`;

function newRecord(id: string): StreamRecord {
  return {
    id,
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle: {},
    currentOffset: ZERO,
    counter: 0,
  };
}

describe("multi-writer CAS contention", () => {
  it("lets exactly one of N concurrent process writers win the first offset", async () => {
    const root = mkdtempSync(path.join(tmpdir(), "streamsy-fs-multi-"));
    const adapter = createFsStorageAdapter({ root });
    await adapter.create({ record: newRecord("s") });

    const fixture = path.join(import.meta.dir, "concurrent-writer.fixture.ts");
    const writers = 6;
    const procs = Array.from({ length: writers }, (_unused, i) =>
      Bun.spawn(["bun", fixture, root, "s", `value-${i}`], { stdout: "pipe", stderr: "pipe" }),
    );

    const statuses = await Promise.all(
      procs.map(async (proc) => {
        await proc.exited;
        return (await new Response(proc.stdout).text()).trim();
      }),
    );

    const appended = statuses.filter((s) => s === "appended").length;
    const rejected = statuses.filter((s) => s === "precondition-failed").length;
    expect(appended).toBe(1);
    expect(rejected).toBe(writers - 1);

    // Exactly one message is visible, and the tail advanced by exactly one.
    const messages = await adapter.listMessages("s");
    expect(messages.length).toBe(1);
    const record = (await adapter.getRecord("s"))!;
    expect(record.counter).toBe(1);
  }, 20_000);
});
