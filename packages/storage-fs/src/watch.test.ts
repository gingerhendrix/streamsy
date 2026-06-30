/**
 * `fs.watch`-backed cross-process wake.
 *
 * A parked `awaitChange` in this process must be woken by a write from a SEPARATE
 * process (the HTTP-frontend + serverless-writers use-case). With the bounded poll
 * fallback set far higher than the timeout, resolving quickly proves the wake came
 * from the `fs.watch` event rather than a poll re-read. Correctness never depends
 * on the event (the loop re-reads first); this asserts the latency optimization.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "bun:test";
import type { StreamRecord } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";

const ZERO = `${"0".repeat(16)}_${"0".repeat(16)}`;
const OFFSET_1 = `${"1".padStart(16, "0")}_${"0".repeat(16)}`;

function newRecord(id: string): StreamRecord {
  return {
    id,
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle: {},
    currentOffset: ZERO,
    counter: 0,
  };
}

describe("fs.watch cross-process awaitChange", () => {
  it("wakes a parked waiter when another process advances the stream", async () => {
    const root = mkdtempSync(path.join(tmpdir(), "streamsy-fs-watch-"));
    // Poll fallback is 5s, far above the change latency we expect from fs.watch.
    const adapter = createFsStorageAdapter({ root, watch: true, watchPollMs: 5000 });
    await adapter.create({ record: newRecord("s") });

    const started = Date.now();
    const pending = adapter.awaitChange("s", {
      fromOffset: ZERO,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 6000,
    });

    // Give the waiter a moment to park, then advance from a separate process.
    await new Promise((resolve) => setTimeout(resolve, 100));
    const fixture = path.join(import.meta.dir, "concurrent-writer.fixture.ts");
    const writer = Bun.spawn(["bun", fixture, root, "s", "from-another-process"], {
      stdout: "pipe",
    });
    await writer.exited;

    const result = await pending;
    const elapsed = Date.now() - started;

    expect(result.status).toBe("changed");
    expect(result.snapshot.currentOffset).toBe(OFFSET_1);
    // Resolved well before the 5s poll fallback ⇒ the fs.watch event delivered.
    expect(elapsed).toBeLessThan(3000);
  }, 15_000);
});
